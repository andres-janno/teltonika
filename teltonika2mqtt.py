import asyncio
import collections
import os
import struct
import json
import datetime
import math
import paho.mqtt.client as mqtt

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
TCP_PORT = 5027

# Teltonika devices resend an AVL record if they don't get our ACK in time
# (e.g. our service restarted mid-transmission) - this dedupes those so a
# retransmitted record doesn't get published/stored twice. Keyed by IMEI, an
# LRU of recently published record keys (timestamp + IO content - distinct
# records can share a timestamp, e.g. FMC150 EYE Beacon records next to a
# periodic one); bounded so it can't grow unbounded over a long-running process.
RECENT_TIMESTAMPS_PER_IMEI = 100
recent_timestamps = {}

def is_duplicate_record(imei, rec):
    key = (rec["timestamp"], json.dumps([rec["raw_io"], rec["eye_raw_io"]], sort_keys=True))
    seen = recent_timestamps.setdefault(imei, collections.OrderedDict())
    if key in seen:
        seen.move_to_end(key)
        return True
    seen[key] = None
    if len(seen) > RECENT_TIMESTAMPS_PER_IMEI:
        seen.popitem(last=False)
    return False

# GPS glitch filter: bogus fixes (e.g. a single spike or position jumping far away)
# otherwise end up in the DB and draw spikes on the route / inflate distance.
# A fix is an initial glitch candidate if it is further from the last accepted fix
# than the vehicle could have travelled at its reported speed (+ margin) in the elapsed time.
# If a new position is confirmed by a consecutive reading (within 30s), it is accepted
# immediately rather than locking the filter out. If no fix has been accepted for
# GLITCH_RESET_S, the new position is also accepted unconditionally.
GLITCH_BASE_M = 30
GLITCH_SPEED_MARGIN_KMH = 20
GLITCH_RESET_S = 60
last_good_fix = {}      # imei -> (ts_s, lat, lon, speed_kmh)
glitch_candidate = {}   # imei -> (ts_s, lat, lon, speed_kmh)

def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def is_gps_glitch(imei, ts_ms, lat, lon, speed_kmh):
    """True if this fix is physically implausible vs the last accepted one.
    Updates the per-IMEI state; no-fix (0,0) readings are left alone."""
    if lat == 0 and lon == 0:
        return False
    t_s = ts_ms / 1000
    speed_kmh = speed_kmh or 0
    prev = last_good_fix.get(imei)
    if not prev:
        last_good_fix[imei] = (t_s, lat, lon, speed_kmh)
        glitch_candidate.pop(imei, None)
        return False

    dt = t_s - prev[0]
    # Clock jumped backwards or no accepted fix for GLITCH_RESET_S -> accept unconditionally
    if dt <= 0 or dt > GLITCH_RESET_S:
        last_good_fix[imei] = (t_s, lat, lon, speed_kmh)
        glitch_candidate.pop(imei, None)
        return False

    v_ms = (max(speed_kmh, prev[3]) + GLITCH_SPEED_MARGIN_KMH) / 3.6
    dist = _haversine_m(prev[1], prev[2], lat, lon)
    if dist <= GLITCH_BASE_M + v_ms * dt:
        last_good_fix[imei] = (t_s, lat, lon, speed_kmh)
        glitch_candidate.pop(imei, None)
        return False

    # Implausible against last_good_fix: check if this confirms an active candidate
    cand = glitch_candidate.get(imei)
    if cand:
        cand_ts, cand_lat, cand_lon, cand_spd = cand
        dt_cand = t_s - cand_ts
        if 0 <= dt_cand <= 30:
            cand_v_ms = (max(speed_kmh, cand_spd) + GLITCH_SPEED_MARGIN_KMH) / 3.6
            cand_dist = _haversine_m(cand_lat, cand_lon, lat, lon)
            if cand_dist <= GLITCH_BASE_M + cand_v_ms * max(dt_cand, 1):
                # Confirmed new position by consecutive reading -> accept immediately
                last_good_fix[imei] = (t_s, lat, lon, speed_kmh)
                glitch_candidate.pop(imei, None)
                return False

    # Not yet confirmed: record candidate and reject this single fix
    glitch_candidate[imei] = (t_s, lat, lon, speed_kmh)
    return True


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
# reload_configs_periodically) without needing to restart the service.
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

# Device model per IMEI, e.g. {"862129089514594": "FMC150"} - selects the
# model's AVL ID overrides (MODEL_IO_NAMES). Not derivable from the IMEI or
# the data stream. IMEIs not listed are treated as FMC650. Reloaded
# automatically like IMEI_EYE_MAC_FILE.
IMEI_MODEL_FILE = "imei_model.json"
imei_model = {}

# Config files reloaded on change: path -> dict updated in place.
RELOADABLE_CONFIGS = {IMEI_EYE_MAC_FILE: imei_eye_mac, IMEI_MODEL_FILE: imei_model}

def load_config(path):
    target = RELOADABLE_CONFIGS[path]
    try:
        with open(path) as f:
            data = json.load(f)
        target.clear()
        target.update(data)
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        print(f"[-] Failed to load {path}: {e}")

async def reload_configs_periodically():
    last_mtime = {}
    while True:
        for path in RELOADABLE_CONFIGS:
            try:
                mtime = os.stat(path).st_mtime
            except FileNotFoundError:
                mtime = None
            if mtime != last_mtime.get(path, 0):
                load_config(path)
                last_mtime[path] = mtime
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

    # FMC125 "EYE Sensor by MAC #N" (connectionless) uses a different ID
    # range, same "base + (slot - 1)" layout. Slot 1 inferred from live data
    # (10800=1689 -> 16.89 C, 10804=56 -> 56 %RH, 10808=1 -> magnet); slots
    # 2-4 extrapolated from the pattern and unconfirmed. Battery not mapped.
    10800: (1, "eye_temperature_c"),      # x100 C
    10801: (2, "eye_temperature_c"),
    10802: (3, "eye_temperature_c"),
    10803: (4, "eye_temperature_c"),
    10804: (1, "eye_humidity_percent"),   # %
    10805: (2, "eye_humidity_percent"),
    10806: (3, "eye_humidity_percent"),
    10807: (4, "eye_humidity_percent"),
    10808: (1, "eye_magnet_closed"),      # plain 0/1
    10809: (2, "eye_magnet_closed"),
    10810: (3, "eye_magnet_closed"),
    10811: (4, "eye_magnet_closed"),
    10824: (1, "eye_battery_v"),         # mV -> V (observed 3010)
    10825: (2, "eye_battery_v"),         # mV -> V (observed 3040); slots 3/4 likely 10826/10827 but those collide with legacy IDs below - unconfirmed
}

# IO IDs deliberately not published at all: internal backup battery
# voltage/current (67/68, disabled - not needed) and BLE 3 Battery (22,
# official AVL ID meaning, unrelated to our EYE sensors - removed).
DISABLED_IO_IDS = {22, 67, 68}

# Observed on multiple EYE_SLOT_FIELDS (temperature, humidity) when a
# reading fails - e.g. the sensor was briefly out of BLE range. 0x7FFF, the
# max signed/unsigned 16-bit value - a common "invalid reading" convention.
# Filtered out in translate_io_values() rather than published as a bogus
# 327.67 C / 32767% / corrupted magnet-flag reading.
EYE_ERROR_SENTINEL = 32767

# eye_battery_percent reading exactly 0 is the FMC650's signal for "no BLE
# sensor currently connected in this slot" - confirmed against real data: a
# connected sensor's battery reading always falls in a tight ~105-110 band
# (this field is a raw scale, not an actual 0-100 percent, despite the
# name), and never dips anywhere near 0, while a slot with nothing
# paired/in-range reports a hard 0. Temperature/humidity/magnet often
# aren't even present in the same IO batch when this happens, so battery
# alone is the reliable tell. Unlike EYE_ERROR_SENTINEL (a failed read
# attempt), this is "nothing to read" - the whole slot's batch is discarded
# in translate_io_values() so it doesn't overwrite the last real reading or
# get published/stored as bogus zeros.
#
# The "EYE Sensor by MAC" IDs (108xx, FMC150 and FMC650 alike) have no
# battery percent; they signal a sensor that isn't being received with
# battery 0 mV (first record after it drops out, all fields 0) and then
# temperature 25000 (250.00 C), humidity 250 %, battery 250 mV - observed
# on both tractors at once when one sensor went missing. Handled the same way.
EYE_NOT_FOUND_TEMP_C = 250.0
EYE_NOT_FOUND_HUMIDITY = 250

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

    # Engine data
    85: "engine_load_percent",     # %
    88: "engine_rpm",              # rpm
    135: "fuel_rate_lph",          # l/h
    104: "engine_total_hours_h",   # h (FMS: Engine Total Hours Of Operation)
    127: "engine_coolant_temp_c",  # C (FMS: Engine Coolant Temperature)
    80: "can_speed_kmh",          # km/h - empirically tracks speed_avl_kmh (corr 0.97, never lower, typically +0..2); not Data Mode

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

# Per-model AVL ID overrides on top of IO_NAMES (which is FMC650's map).
# FMC150 uses Teltonika's FMB140/FMC150 CAN ID table - confirmed against
# our data: 85 ranges 0-1026 (RPM; on FMC650 it's load 15-100 %), no 88,
# 89 sits at 35-40 (fuel level %). Values are published raw, no scaling.
MODEL_IO_NAMES = {
    "FMC150": {
        81: "can_speed_kmh",           # km/h, CAN vehicle speed
        85: "engine_rpm",              # rpm
        89: "fuel_level_percent",      # %
        107: "fuel_consumed_counted",  # counter, unit unconfirmed (likely 0.1 l)
    },
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

def translate_io_values(io_data, model_io_names=None):
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
        elif isinstance(raw_val, str):
            # Variable-length (X-byte, Codec 8E) element, kept as hex. Its ID
            # may collide with a numeric one below (e.g. FMC150's EYE Beacon
            # data) - never run numeric conversions on it, or the whole
            # packet fails, goes un-ACKed and the device resends it forever.
            # EYE ones stay visible in the raw dump only.
            if io_id in EYE_IO_IDS:
                continue
            val = raw_val
        elif io_id in EYE_SLOT_FIELDS and raw_val == EYE_ERROR_SENTINEL:
            # 32767 (0x7FFF, int16 max) shows up on any EYE slot field when
            # that reading failed (e.g. sensor briefly out of BLE range) -
            # skip it rather than publish a bogus temperature/humidity/magnet
            # value; the previous good reading stays cached downstream.
            continue
        elif io_id in (66, 26, 30, 10828, 10824, 10825):
            # Millivolts to volts (e.g. 11586 mV -> 11.586 V)
            val = round(raw_val / 1000.0, 3)
        elif io_id in (239, 240):
            # Boolean states (ignition, movement)
            val = bool(raw_val)
        elif io_id in (25, 29):
            # EYE sensor temperature (often scaled by 10 or 100)
            val = raw_val / 10.0 if abs(raw_val) > 200 else raw_val
        elif io_id in (701, 702, 703, 704, 10800, 10801, 10802, 10803, 10826):
            # EYE temperature (0.01 C step)
            val = round(raw_val / 100.0, 2)
        elif io_id in (10808, 10809, 10810, 10811):
            # FMC125 EYE magnet state, plain boolean
            val = bool(raw_val)
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
            name = (model_io_names or {}).get(io_id) or IO_NAMES.get(io_id, f"io_{io_id}")
            fmc_values[name] = val

    for slot, vals in eye_slot_values.items():
        if (vals.get("eye_battery_percent") == 0
                or vals.get("eye_battery_v") == 0
                or vals.get("eye_temperature_c") == EYE_NOT_FOUND_TEMP_C
                or vals.get("eye_humidity_percent") == EYE_NOT_FOUND_HUMIDITY):
            eye_slot_values[slot] = {}

    return fmc_values, eye_slot_values

def parse_codec8(data, model_io_names=None):
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

        fmc_values, eye_slot_values = translate_io_values(io_data, model_io_names)
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

            num_records, records = parse_codec8(data_part, MODEL_IO_NAMES.get(imei_model.get(imei)))

            writer.write(struct.pack(">I", num_records))
            await writer.drain()

            fmc_topic = f"teltonika/{imei}/data"
            for rec in records:
                if is_duplicate_record(imei, rec):
                    continue

                fmc_values = rec.pop("values")
                fmc_raw_io = rec.pop("raw_io")
                eye_slot_values = rec.pop("eye_slot_values")
                eye_raw_io = rec.pop("eye_raw_io")

                # Flat payload (no nested objects) so each key maps directly
                # to a TimescaleDB column/field.
                eff_speed = max(rec.get("speed") or 0, fmc_values.get("can_speed_kmh") or 0, fmc_values.get("speed_avl_kmh") or 0)
                glitch = is_gps_glitch(imei, rec["timestamp"], rec["latitude"], rec["longitude"], eff_speed)
                if glitch:
                    print(f"[!] GPS glitch dropped ({imei}): {rec['latitude']}, {rec['longitude']} @ {rec['datetime']}")
                fmc_rec = {
                    "timestamp": rec["timestamp"],
                    "datetime": rec["datetime"],
                    # "codec": rec["codec"],  # disabled - keeping the payload as small as possible
                    # "priority": rec["priority"],  # disabled - keeping the payload as small as possible
                    **({} if glitch else {
                        "latitude": rec["latitude"],
                        "longitude": rec["longitude"],
                        "altitude": rec["altitude"],
                        "angle": rec["angle"],
                    }),
                    "satellites": rec["satellites"],
                    "speed": rec["speed"],
                    **fmc_values
                }
                mqtt_client.publish(fmc_topic, json.dumps(fmc_rec))
                mqtt_client.publish(f"{fmc_topic}/raw", json.dumps(fmc_raw_io))
                print(f"[>] {fmc_topic} | Ign: {fmc_values.get('ignition')} | Volt: {fmc_values.get('external_voltage_v')}V")

                if eye_raw_io:
                    slot_macs = resolve_slot_macs(imei, eye_raw_io)

                    # EYE sensors have no GPS of their own - tag their
                    # records with the host FMC650's position. Falls back to
                    # the last accepted fix when this record's fix was
                    # dropped as a glitch or is a no-fix (0,0).
                    if not glitch and not (rec["latitude"] == 0 and rec["longitude"] == 0):
                        host_pos = {
                            "latitude": rec["latitude"],
                            "longitude": rec["longitude"],
                            "altitude": rec["altitude"],
                        }
                    elif imei in last_good_fix:
                        _, lat, lon, _ = last_good_fix[imei]
                        host_pos = {"latitude": lat, "longitude": lon}
                    else:
                        host_pos = {}

                    for slot in EYE_SLOTS:
                        if not eye_slot_values[slot]:
                            # Nothing fresh for this slot in THIS record -
                            # eye_raw_io being non-empty only means some
                            # slot (or the beacon subsystem) reported
                            # something, not this one. Skip instead of
                            # republishing the cached snapshot again - that
                            # was flooding the DB with a wall of identical
                            # rows that never actually reflected a new
                            # reading.
                            continue

                        merged = last_seen_eye_values.setdefault((imei, slot), {})
                        merged.update(eye_slot_values[slot])

                        eye_rec = {
                            "timestamp": rec["timestamp"],
                            "datetime": rec["datetime"],
                            # omitted when the slot has no known MAC - a null would be stored as the text "null"
                            **({"mac": slot_macs[slot]} if slot_macs[slot] else {}),
                            **host_pos,
                            **merged  # sensor's own coordinates, if it ever sends any, win
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
    for path in RELOADABLE_CONFIGS:
        load_config(path)
    asyncio.create_task(reload_configs_periodically())

    server = await asyncio.start_server(handle_teltonika, "0.0.0.0", TCP_PORT)
    print(f"[*] Teltonika TCP -> MQTT gateway listening on port {TCP_PORT}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
