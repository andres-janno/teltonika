import asyncio
import os
import struct
import json
import datetime
import paho.mqtt.client as mqtt

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
TCP_PORT = 5027

# Which EYE sensor (BLE MAC) is currently attached to which tractor (FMC650
# IMEI). This pairing isn't sent over the AVL data stream - the EYE sensor
# lives on the implement/attachment and gets moved between tractors, so it's
# tracked here instead of hardcoded. Edit IMEI_EYE_MAC_FILE to reflect a swap;
# it is reloaded automatically (see reload_imei_eye_mac_periodically) without
# needing to restart the service.
IMEI_EYE_MAC_FILE = "imei_eye_mac.json"
IMEI_EYE_MAC_RELOAD_SECONDS = 30
imei_eye_mac = {}

def load_imei_eye_mac():
    try:
        with open(IMEI_EYE_MAC_FILE) as f:
            data = json.load(f)
        imei_eye_mac.clear()
        imei_eye_mac.update(data)
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        print(f"[-] Failed to load {IMEI_EYE_MAC_FILE}: {e}")

async def reload_imei_eye_mac_periodically():
    last_mtime = None
    while True:
        try:
            mtime = os.stat(IMEI_EYE_MAC_FILE).st_mtime
        except FileNotFoundError:
            mtime = None
        if mtime != last_mtime:
            load_imei_eye_mac()
            last_mtime = mtime
        await asyncio.sleep(IMEI_EYE_MAC_RELOAD_SECONDS)

# Teltonika IO ID lookup table (English parameter names).
#
# Note: on this device, AVL IDs 701, 705, 709, 713, 729 and 730 carry EYE
# sensor readings (confirmed against the field-tested server.py mapping),
# not the standard CAN / FMS / Tachograph fields those IDs are documented
# for on other Teltonika models. Mapped accordingly below.
IO_NAMES = {
    # System & Power
    21: "gsm_signal_level",        # 0-5 bars
    22: "device_battery_voltage_v",# mV -> V
    24: "speed_avl_kmh",           # AVL speed element - may duplicate or differ from GPS-derived speed depending on the device's configured speed source
    66: "external_voltage_v",      # mV -> V
    67: "battery_voltage_v",       # mV -> V
    68: "battery_current_ma",
    71: "gnss_status",
    181: "gnss_pdop",
    182: "gnss_hdop",
    200: "sleep_mode",
    239: "ignition",               # bool
    240: "movement",               # bool
    241: "gsm_operator_code",

    # Teltonika EYE Sensor (relayed via AVL IDs 701-730 on this device)
    701: "eye_temperature_c",      # x100 C
    705: "eye_battery_percent",    # %
    709: "eye_humidity_percent",   # %
    713: "eye_magnet_closed",      # bool, bit 3 active-low
    729: "eye_magnet_movement_count",
    730: "eye_movement_count",

    # Teltonika EYE Sensor / BLE Sensor 1
    25: "eye_1_temperature",        # x10 C
    26: "eye_1_battery_v",          # mV -> V
    27: "eye_1_humidity",           # %
    28: "eye_1_magnet",             # 1 = magnet present / closed
    86: "eye_1_pitch",
    87: "eye_1_roll",
    10822: "eye_1_magnet_state",
    10823: "eye_1_movement",
    10824: "eye_1_pitch",
    10825: "eye_1_roll",
    10826: "eye_1_temperature",     # x100 C
    10827: "eye_1_humidity",        # x100 %
    10828: "eye_1_battery_v",       # mV -> V
    10829: "eye_1_battery_percent", # %

    # BLE Sensor 2
    29: "eye_2_temperature",
    30: "eye_2_battery_v",
    31: "eye_2_humidity",
    32: "eye_2_magnet",

    # Raw Beacon data
    385: "ble_beacon_raw_hex"
}

# IO IDs that carry EYE sensor data rather than FMC650 device data. Used to
# split each record into two separate MQTT messages (see handle_teltonika).
EYE_IO_IDS = {
    25, 26, 27, 28, 86, 87,
    29, 30, 31, 32,
    701, 705, 709, 713, 729, 730,
    10822, 10823, 10824, 10825, 10826, 10827, 10828, 10829,
    385,
}

try:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    mqtt_client = mqtt.Client()

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

def parse_eye_beacon_mac(raw_hex):
    """Extracts the EYE sensor's Bluetooth MAC address from a Universal BLE
    Beacon element (AVL ID 385: data-part byte, flags byte, beacon ID,
    optional signal strength/battery/temperature - per Teltonika's Beacon
    Packet structure). Assumes the device's Advanced Beacon Capturing is
    configured to detect this beacon by MAC address, in which case the
    beacon ID field is the raw 6-byte MAC instead of an iBeacon/Eddystone
    UUID. Returns None if the payload doesn't match that shape."""
    try:
        data = bytes.fromhex(raw_hex)
    except ValueError:
        return None
    if len(data) < 2:
        return None

    flags = data[1]
    optional_bytes = (
        (1 if flags & 0x01 else 0)  # signal strength
        + (2 if flags & 0x02 else 0)  # battery voltage
        + (2 if flags & 0x04 else 0)  # temperature
    )
    beacon_id = data[2:len(data) - optional_bytes]
    if len(beacon_id) != 6:
        return None
    return beacon_id.hex(':').upper()

def translate_io_values(io_data):
    """Translates raw IO IDs into named values, split into FMC650 device
    values and EYE sensor values (see EYE_IO_IDS)."""
    fmc_values = {}
    eye_values = {}
    for io_id, raw_val in io_data.items():
        name = IO_NAMES.get(io_id, f"io_{io_id}")

        # Convert raw values into their logical units
        if io_id in (66, 67, 22, 26, 30, 10828):
            # Millivolts to volts (e.g. 11586 mV -> 11.586 V)
            val = round(raw_val / 1000.0, 3)
        elif io_id in (239, 240):
            # Boolean states (ignition, movement)
            val = bool(raw_val)
        elif io_id in (25, 29):
            # EYE sensor temperature (often scaled by 10 or 100)
            val = raw_val / 10.0 if abs(raw_val) > 200 else raw_val
        elif io_id in (701, 10826):
            # EYE temperature (0.01 C step)
            val = round(raw_val / 100.0, 2)
        elif io_id == 10827:
            # Codec 8 Extended EYE humidity (0.01 % step)
            val = round(raw_val / 100.0, 2)
        elif io_id == 713:
            # EYE magnet state, bit 3 (1 = magnet near/closed)
            val = (raw_val & 0x08) != 0
        else:
            val = raw_val

        if io_id in EYE_IO_IDS:
            eye_values[name] = val
        else:
            fmc_values[name] = val

    return fmc_values, eye_values

def parse_codec8(data):
    codec_id = data[0]
    num_records = data[1]
    offset = 2
    records = []

    for _ in range(num_records):
        ts_ms = struct.unpack_from(">Q", data, offset)[0]
        offset += 8
        dt_iso = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=datetime.timezone.utc).isoformat()

        priority = data[offset]
        offset += 1

        lon, lat, alt, angle, sats, speed = struct.unpack_from(">iihhBh", data, offset)
        offset += 15

        io_data = {}

        if codec_id == 0x08:  # Codec 8
            event_id, total_io = struct.unpack_from(">BB", data, offset)
            offset += 2

            for byte_size in (1, 2, 4, 8):
                count = data[offset]; offset += 1
                fmt = {1: ">B", 2: ">h", 4: ">i", 8: ">q"}[byte_size]
                for _ in range(count):
                    io_id = data[offset]; offset += 1
                    val = struct.unpack_from(fmt, data, offset)[0]
                    offset += byte_size
                    io_data[io_id] = val

        elif codec_id == 0x8E:  # Codec 8 Extended
            event_id, total_io = struct.unpack_from(">HH", data, offset)
            offset += 4

            for byte_size in (1, 2, 4, 8):
                count = struct.unpack_from(">H", data, offset)[0]; offset += 2
                fmt = {1: ">B", 2: ">h", 4: ">i", 8: ">q"}[byte_size]
                for _ in range(count):
                    io_id = struct.unpack_from(">H", data, offset)[0]; offset += 2
                    val = struct.unpack_from(fmt, data, offset)[0]
                    offset += byte_size
                    io_data[io_id] = val

            # Variable-length fields (X bytes)
            if offset + 2 <= len(data):
                nx = struct.unpack_from(">H", data, offset)[0]; offset += 2
                for _ in range(nx):
                    io_id, vlen = struct.unpack_from(">HH", data, offset)
                    offset += 4
                    io_data[io_id] = data[offset:offset + vlen].hex()
                    offset += vlen

        fmc_values, eye_values = translate_io_values(io_data)
        fmc_raw_io = {k: v for k, v in io_data.items() if k not in EYE_IO_IDS}
        eye_raw_io = {k: v for k, v in io_data.items() if k in EYE_IO_IDS}

        records.append({
            "timestamp": ts_ms,
            "datetime": dt_iso,
            "priority": priority,
            "latitude": lat / 10000000.0,
            "longitude": lon / 10000000.0,
            "altitude": alt,
            "angle": angle,
            "satellites": sats,
            "speed": speed,
            "values": fmc_values,
            "raw_io": fmc_raw_io,
            "eye_values": eye_values,
            "eye_raw_io": eye_raw_io
        })

    return num_records, records

async def handle_teltonika(reader, writer):
    peer = writer.get_extra_info('peername')
    imei = "unknown"

    try:
        imei_len_b = await reader.readexactly(2)
        imei_len = struct.unpack(">H", imei_len_b)[0]
        imei_bytes = await reader.readexactly(imei_len)
        imei = imei_bytes.decode('ascii', errors='ignore')

        writer.write(b'\x01')
        await writer.drain()

        while True:
            header = await reader.readexactly(8)
            _, data_length = struct.unpack(">II", header)

            payload = await reader.readexactly(data_length + 4)
            data_part = payload[:data_length]

            num_records, records = parse_codec8(data_part)

            writer.write(struct.pack(">I", num_records))
            await writer.drain()

            fmc_topic = f"teltonika/{imei}/data"
            eye_topic = f"teltonika/{imei}/eye"
            for rec in records:
                fmc_values = rec.pop("values")
                fmc_raw_io = rec.pop("raw_io")
                eye_values = rec.pop("eye_values")
                eye_raw_io = rec.pop("eye_raw_io")

                # Flat payload (no nested objects) so each key maps directly
                # to a TimescaleDB column/field.
                fmc_rec = {
                    "timestamp": rec["timestamp"],
                    "datetime": rec["datetime"],
                    "imei": imei,
                    "priority": rec["priority"],
                    "latitude": rec["latitude"],
                    "longitude": rec["longitude"],
                    "altitude": rec["altitude"],
                    "angle": rec["angle"],
                    "satellites": rec["satellites"],
                    "speed": rec["speed"],
                    **fmc_values
                }
                mqtt_client.publish(fmc_topic, json.dumps(fmc_rec))
                mqtt_client.publish(f"{fmc_topic}/raw", json.dumps(fmc_raw_io))
                print(f"[>] {fmc_topic} | Ign: {fmc_values.get('ignition')} | Volt: {fmc_values.get('external_voltage_v')}V")

                if eye_values or eye_raw_io:
                    if 385 in eye_raw_io:
                        mac = parse_eye_beacon_mac(eye_raw_io[385])
                    else:
                        mac = imei_eye_mac.get(imei)
                    eye_rec = {
                        "timestamp": rec["timestamp"],
                        "datetime": rec["datetime"],
                        "imei": imei,
                        "mac": mac,
                        "latitude": rec["latitude"],
                        "longitude": rec["longitude"],
                        "altitude": rec["altitude"],
                        "angle": rec["angle"],
                        "speed": rec["speed"],
                        **eye_values
                    }
                    mqtt_client.publish(eye_topic, json.dumps(eye_rec))
                    mqtt_client.publish(f"{eye_topic}/raw", json.dumps(eye_raw_io))
                    print(f"[>] {eye_topic} | IMEI: {imei} | EYE Temp: {eye_values.get('eye_temperature_c', eye_values.get('eye_1_temperature', 'N/A'))}")

    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception as e:
        print(f"[-] Error ({imei}): {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    load_imei_eye_mac()
    asyncio.create_task(reload_imei_eye_mac_periodically())

    server = await asyncio.start_server(handle_teltonika, "0.0.0.0", TCP_PORT)
    print(f"[*] Teltonika TCP -> MQTT gateway listening on port {TCP_PORT}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
