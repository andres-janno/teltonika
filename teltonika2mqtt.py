import asyncio
import collections
import os
import struct
import json
import datetime
import paho.mqtt.client as mqtt

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
TCP_PORT = 5027

# Teltonika devices resend an AVL record if they don't get our ACK in time
# (e.g. our service restarted mid-transmission) - this dedupes those so a
# retransmitted record doesn't get published/stored twice. Keyed by IMEI, an
# LRU of recently published record timestamps; bounded so it can't grow
# unbounded over a long-running process.
RECENT_TIMESTAMPS_PER_IMEI = 100
recent_timestamps = {}

def is_duplicate_record(imei, ts_ms):
    seen = recent_timestamps.setdefault(imei, collections.OrderedDict())
    if ts_ms in seen:
        seen.move_to_end(ts_ms)
        return True
    seen[ts_ms] = None
    if len(seen) > RECENT_TIMESTAMPS_PER_IMEI:
        seen.popitem(last=False)
    return False

# FMC650 Bluetooth 4.0 supports up to 4 paired sensor slots.
MAX_EYE_SLOTS = 4
EYE_SLOTS = range(1, MAX_EYE_SLOTS + 1)

# Which EYE sensor (BLE MAC) is currently attached to which tractor (FMC650
# IMEI), per Bluetooth 4.0 sensor slot (1-4). This pairing isn't sent over
# the AVL data stream - the EYE sensor lives on the implement/attachment and
# gets moved between tractors, so it's tracked here instead of hardcoded. A
# value can be either a plain MAC string (legacy - applies to slot 1 only)
# or an object keyed by slot:
#   {"<imei>": {"1": "AA:BB:CC:DD:EE:FF", "2": "11:22:33:44:55:66"}}
# Edit IMEI_EYE_MAC_FILE to reflect a swap; it is reloaded automatically (see
# reload_imei_eye_mac_periodically) without needing to restart the service.
IMEI_EYE_MAC_FILE = "imei_eye_mac.json"
IMEI_EYE_MAC_RELOAD_SECONDS = 30
imei_eye_mac = {}

# Beacon capturing (AVL 548/385, which carries the live MAC) and the paired
# BLE sensor readings (AVL 701-730) are two independent FMC650 subsystems -
# they rarely land in the same AVL record, and with two sensors paired, a
# single 548 element can report both of their MACs at once with no direct
# link to which slot's readings they belong to. This remembers the last live
# MAC matched to each (imei, slot), so records with only sensor readings
# still get tagged with the right slot's MAC instead of falling straight
# through to the static imei_eye_mac.json, and so a freshly detected MAC can
# be matched back to the slot it belongs to (see resolve_slot_macs). Keyed
# by (imei, slot).
last_seen_eye_mac = {}

# Same idea for the individual EYE sensor readings (temperature, humidity,
# magnet, ...) of each slot: since they arrive piecemeal across separate AVL
# records, this remembers the latest known value of each field per (imei,
# slot), so every eye1/eye2 message published is a complete snapshot rather
# than whatever subset of fields happened to be in that one record.
last_seen_eye_values = {}

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

# Which AVL IDs carry each Bluetooth 4.0 sensor SLOT's EYE readings, and
# under what field name. Confirmed by field-testing across 3 paired sensors:
# temperature/battery/humidity/magnet each follow "base ID + (slot - 1)"
# (e.g. temperature is 701/702/703/704 for slots 1-4) - not documented
# anywhere we could find. Slot 4's fields (704/708/712/716) are extrapolated
# from that confirmed pattern but not yet directly observed. Fields here get
# a slot-specific MAC attached (see resolve_slot_macs) and are published to
# separate teltonika/<imei>/eye<slot> topics - never mixed into one message,
# since they are distinct physical sensors.
#
# Movement/magnet counters (729/730 for slot 1; the 731-740 range for the
# others, which doesn't follow the simple per-slot offset the other fields
# do) are deliberately not mapped/published - not needed, still visible in
# the raw IO dump if that changes.
EYE_SLOT_FIELDS = {
    701: (1, "eye_temperature_c"),        # x100 C
    705: (1, "eye_battery_percent"),      # %
    709: (1, "eye_humidity_percent"),     # %
    713: (1, "eye_magnet_closed"),        # bool, bit 3

    702: (2, "eye_temperature_c"),        # x100 C
    706: (2, "eye_battery_percent"),      # %
    710: (2, "eye_humidity_percent"),     # %
    714: (2, "eye_magnet_closed"),        # bool, bit 3

    703: (3, "eye_temperature_c"),        # x100 C
    707: (3, "eye_battery_percent"),      # %
    711: (3, "eye_humidity_percent"),     # %
    715: (3, "eye_magnet_closed"),        # bool, bit 3

    704: (4, "eye_temperature_c"),        # x100 C, extrapolated - unconfirmed
    708: (4, "eye_battery_percent"),      # %, extrapolated - unconfirmed
    712: (4, "eye_humidity_percent"),     # %, extrapolated - unconfirmed
    716: (4, "eye_magnet_closed"),        # bool, bit 3, extrapolated - unconfirmed
}

# IO IDs deliberately not published at all: internal backup battery
# voltage/current (67/68, disabled - not needed) and BLE 3 Battery (22,
# official AVL ID meaning, unrelated to our EYE sensors - removed).
DISABLED_IO_IDS = {22, 67, 68}

# Teltonika IO ID lookup table (English parameter names), for everything
# that ISN'T one of the per-slot EYE fields above.
IO_NAMES = {
    # System & Power
    21: "gsm_signal_level",        # 0-5 bars
    24: "speed_avl_kmh",           # AVL speed element - may duplicate or differ from GPS-derived speed depending on the device's configured speed source
    66: "external_voltage_v",      # mV -> V
    # 67: "battery_voltage_v",     # mV -> V - internal backup battery, disabled: not needed right now
    # 68: "battery_current_ma",    # internal backup battery, disabled: not needed right now
    71: "gnss_status",
    181: "gnss_pdop",
    182: "gnss_hdop",
    200: "sleep_mode",
    239: "ignition",               # bool
    240: "movement",               # bool
    241: "gsm_operator_code",

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
    385: "ble_beacon_raw_hex",       # Simple beacon mode
    548: "ble_advanced_beacon_raw_hex"  # Advanced beacon mode (Beacon Capturing Configuration)
}

# IO IDs that carry EYE sensor data rather than FMC650 device data - includes
# both slot fields (EYE_SLOT_FIELDS) and unidentified/legacy ones. Used to
# split each record's raw IO into fmc_raw_io vs eye_raw_io (see handle_teltonika).
EYE_IO_IDS = set(EYE_SLOT_FIELDS) | {
    25, 26, 27, 28, 86, 87,
    29, 30, 31, 32,
    729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740,  # movement/magnet counters, not published as named fields
    10822, 10823, 10824, 10825, 10826, 10827, 10828, 10829,
    385, 548,
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

def parse_advanced_beacon_macs(raw_hex):
    """Extracts every MAC address found in an AVL ID 548 (Advanced Beacon
    Capturing) element: a 1-byte data-part header (current/total parts, as
    in AVL 385), followed by one or more beacon entries, each a 1-byte entry
    length and then TLV parameters (0x00 RSSI: 1-byte value, 0x01 Beacon ID:
    variable-length value, 0x02 Additional Data: variable-length value).
    With two EYE sensors paired, a single element can report both of their
    MACs at once (one entry per detected beacon) - returns a list of every
    Beacon ID that is exactly 6 bytes long (i.e. captured with 'Beacon ID
    source: MAC address'), in the order they appear. Doesn't say which
    physical sensor slot each MAC belongs to - see resolve_slot_macs."""
    try:
        data = bytes.fromhex(raw_hex)
    except ValueError:
        return []
    if len(data) < 1:
        return []

    macs = []
    offset = 1  # skip the leading data-part header byte
    while offset < len(data):
        entry_len = data[offset]
        offset += 1
        entry_end = offset + entry_len
        if entry_end > len(data):
            break

        pos = offset
        while pos + 2 <= entry_end:
            param_id = data[pos]
            param_len = data[pos + 1]
            pos += 2
            value = data[pos:pos + param_len]
            pos += param_len
            if param_id == 0x01 and len(value) == 6:
                macs.append(value.hex(':').upper())

        offset = entry_end
    return macs

def resolve_slot_macs(imei, eye_raw_io):
    """Attributes each MAC detected live via 548/385 to the sensor slot (see
    EYE_SLOTS) it belongs to, since neither element says so directly.
    Matches a detected MAC against the slot's last-known MAC (seeded from
    the static imei_eye_mac.json); if exactly one slot has no known MAC yet
    and exactly one detected MAC is left unmatched, bootstraps that slot
    with it (only safe to guess when there's no ambiguity). Returns
    {slot: mac_or_None} for every slot in EYE_SLOTS."""
    if 548 in eye_raw_io:
        detected = parse_advanced_beacon_macs(eye_raw_io[548])
    elif 385 in eye_raw_io:
        mac = parse_eye_beacon_mac(eye_raw_io[385])
        detected = [mac] if mac else []
    else:
        detected = []

    static = imei_eye_mac.get(imei, {})
    if isinstance(static, str):
        static = {"1": static}  # legacy config shape: applies to slot 1 only

    unmatched = list(detected)
    result = {}
    for slot in EYE_SLOTS:
        known = last_seen_eye_mac.get((imei, slot)) or static.get(str(slot))
        match = known if known in unmatched else None
        if match:
            unmatched.remove(match)
            last_seen_eye_mac[(imei, slot)] = match
        result[slot] = match or known

    unknown_slots = [s for s in EYE_SLOTS if result[s] is None]
    if len(unknown_slots) == 1 and len(unmatched) == 1:
        slot = unknown_slots[0]
        last_seen_eye_mac[(imei, slot)] = unmatched[0]
        result[slot] = unmatched[0]

    return result

def translate_io_values(io_data):
    """Translates raw IO IDs into named values. Per-slot EYE fields
    (EYE_SLOT_FIELDS) go into eye_slot_values[<slot>], one dict per physical
    sensor - never mixed together. Anything else in EYE_IO_IDS (unidentified
    IDs, older/legacy EYE IDs, raw beacon hex) is skipped here - it stays
    visible only in the raw IO dump, since we can't confidently name or
    attribute it. Everything else is an FMC650 device value."""
    fmc_values = {}
    eye_slot_values = {slot: {} for slot in EYE_SLOTS}
    for io_id, raw_val in io_data.items():
        # Convert raw values into their logical units
        if io_id in DISABLED_IO_IDS:
            continue
        elif io_id in (66, 26, 30, 10828):
            # Millivolts to volts (e.g. 11586 mV -> 11.586 V)
            val = round(raw_val / 1000.0, 3)
        elif io_id in (239, 240):
            # Boolean states (ignition, movement)
            val = bool(raw_val)
        elif io_id in (25, 29):
            # EYE sensor temperature (often scaled by 10 or 100)
            val = raw_val / 10.0 if abs(raw_val) > 200 else raw_val
        elif io_id in (701, 702, 703, 704, 10826):
            # EYE temperature (0.01 C step)
            val = round(raw_val / 100.0, 2)
        elif io_id == 10827:
            # Codec 8 Extended EYE humidity (0.01 % step)
            val = round(raw_val / 100.0, 2)
        elif io_id in (713, 714, 715, 716):
            # EYE magnet state, bit 3 (1 = magnet near/closed)
            val = (raw_val & 0x08) != 0
        else:
            val = raw_val

        if io_id in EYE_SLOT_FIELDS:
            slot, field_name = EYE_SLOT_FIELDS[io_id]
            eye_slot_values[slot][field_name] = val
        elif io_id in EYE_IO_IDS:
            continue
        else:
            name = IO_NAMES.get(io_id, f"io_{io_id}")
            fmc_values[name] = val

    return fmc_values, eye_slot_values

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

        fmc_values, eye_slot_values = translate_io_values(io_data)
        fmc_raw_io = {k: v for k, v in io_data.items() if k not in EYE_IO_IDS}
        eye_raw_io = {k: v for k, v in io_data.items() if k in EYE_IO_IDS}

        records.append({
            "timestamp": ts_ms,
            "datetime": dt_iso,
            "codec": hex(codec_id),
            "priority": priority,
            "latitude": lat / 10000000.0,
            "longitude": lon / 10000000.0,
            "altitude": alt,
            "angle": angle,
            "satellites": sats,
            "speed": speed,
            "values": fmc_values,
            "raw_io": fmc_raw_io,
            "eye_slot_values": eye_slot_values,
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
            for rec in records:
                if is_duplicate_record(imei, rec["timestamp"]):
                    continue

                fmc_values = rec.pop("values")
                fmc_raw_io = rec.pop("raw_io")
                eye_slot_values = rec.pop("eye_slot_values")
                eye_raw_io = rec.pop("eye_raw_io")

                # Flat payload (no nested objects) so each key maps directly
                # to a TimescaleDB column/field.
                fmc_rec = {
                    "timestamp": rec["timestamp"],
                    "datetime": rec["datetime"],
                    # "codec": rec["codec"],  # disabled - keeping the payload as small as possible
                    # "priority": rec["priority"],  # disabled - keeping the payload as small as possible
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

                if eye_raw_io:
                    slot_macs = resolve_slot_macs(imei, eye_raw_io)

                    for slot in EYE_SLOTS:
                        merged = last_seen_eye_values.setdefault((imei, slot), {})
                        merged.update(eye_slot_values[slot])

                        if not merged and slot_macs[slot] is None:
                            continue  # nothing at all known for this slot yet

                        eye_rec = {
                            "timestamp": rec["timestamp"],
                            "datetime": rec["datetime"],
                            "mac": slot_macs[slot],
                            **merged
                        }
                        eye_topic = f"teltonika/{imei}/eye{slot}"
                        mqtt_client.publish(eye_topic, json.dumps(eye_rec))
                        print(f"[>] {eye_topic} | MAC: {slot_macs[slot]} | Temp: {merged.get('eye_temperature_c', 'N/A')}")

                    mqtt_client.publish(f"teltonika/{imei}/eye/raw", json.dumps(eye_raw_io))

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
