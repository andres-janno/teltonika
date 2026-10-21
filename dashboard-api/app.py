import bisect
import math
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

DB_DSN = os.environ.get(
    "TELTONIKA_DB_DSN",
    "dbname=telemetry_tsdb user=teltonika_api password=CHANGE_ME host=127.0.0.1",
)
TZ = ZoneInfo("Europe/Tallinn")
GAP_S = 900  # seconds - any inter-record gap longer than this is excluded from
             # moving-time/distance sums instead of being counted as travel

app = Flask(__name__)


def get_conn():
    return psycopg2.connect(DB_DSN)


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# GPS glitch detection (keep in sync with teltonika2mqtt.py): a fix is
# an initial glitch candidate if it is further from the last accepted fix
# than the vehicle could have travelled at its reported speed (+ margin) in the elapsed time.
# If a new position is confirmed by a consecutive reading (within 30s), it is accepted
# immediately rather than locking the filter out. If no fix has been accepted for
# GLITCH_RESET_S, the new position is also accepted unconditionally.
GLITCH_BASE_M = 30
GLITCH_SPEED_MARGIN_KMH = 20
GLITCH_RESET_S = 60


def downsample(rows, target=380):
    n = len(rows)
    if n <= target:
        return rows
    step = math.ceil(n / target)
    out = rows[0::step]
    if rows and rows[-1] is not out[-1]:
        out.append(rows[-1])
    return out


FUEL_COUNTER_L = 0.1    # litres per step of fuel_consumed_counted (AVL 107)
FUEL_LEVEL_SETTLE_S = 120  # ignore CAN fuel level until ignition has been on this long

FROZEN_MIN_S = 60       # shortest frozen-position stretch worth reporting
FROZEN_MIN_KM = 0.1     # ...and the CAN-integrated distance it must hide
FROZEN_CAN_KMH = 3      # CAN speed above this counts as actually moving


def find_frozen_positions(rows):
    """Find stretches where the reported position stays bit-for-bit identical
    while CAN speed says the vehicle is moving. That is the tracker's static
    navigation freezing coordinates (e.g. ignition wrongly reported off), so
    the real route for that time is missing and the map shows a jump after it."""
    out = []

    def flush(run):
        # A frozen run can also contain genuine standstill (position parked,
        # CAN speed 0) - split on stationary stretches so each reported
        # interval only covers the time the vehicle was actually moving.
        part = []
        for r in run:
            if (r.get("can_speed") or 0) < FROZEN_CAN_KMH:
                continue
            if part and (r["time"] - part[-1]["time"]).total_seconds() > FROZEN_MIN_S:
                report(run, part)
                part = []
            part.append(r)
        report(run, part)

    def report(run, moving):
        if len(moving) < 2:
            return
        start, end = moving[0]["time"], moving[-1]["time"]
        if (end - start).total_seconds() < FROZEN_MIN_S:
            return
        km = 0.0
        for a, b in zip(run, run[1:]):
            if a["time"] < start or b["time"] > end:
                continue
            dt = min((b["time"] - a["time"]).total_seconds(), 30)
            km += float(a.get("can_speed") or 0) * dt / 3600
        if km < FROZEN_MIN_KM:
            return
        out.append({
            "from": int(start.timestamp() * 1000),
            "to": int(end.timestamp() * 1000),
            "lat": run[0]["latitude"], "lon": run[0]["longitude"],
            "max_can_speed": max(float(r["can_speed"]) for r in moving),
            "km_est": round(km, 2),
        })

    run = []
    for r in rows:
        if r["latitude"] is None or r["longitude"] is None:
            continue
        if run and (r["latitude"], r["longitude"]) == (run[-1]["latitude"], run[-1]["longitude"]) \
                and (r["time"] - run[-1]["time"]).total_seconds() <= GAP_S:
            run.append(r)
        else:
            flush(run)
            run = [r]
    flush(run)
    return out


def day_bounds_utc(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ)
    start = d
    end = d + timedelta(days=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


DATA_SQL = """
SELECT
  time,
  MAX(CASE WHEN field='latitude' THEN value_double END) AS latitude,
  MAX(CASE WHEN field='longitude' THEN value_double END) AS longitude,
  MAX(CASE WHEN field='speed' THEN value_bigint END) AS speed,
  MAX(CASE WHEN field='altitude' THEN value_bigint END) AS altitude,
  BOOL_OR(CASE WHEN field='movement' THEN value_bool END) AS movement,
  BOOL_OR(CASE WHEN field='ignition' THEN value_bool END) AS ignition,
  MAX(CASE WHEN field='external_voltage_v' THEN value_double END) AS voltage,
  MAX(CASE WHEN field='satellites' THEN value_bigint END) AS satellites,
  MAX(CASE WHEN field='gnss_hdop' THEN value_bigint END) AS hdop,
  MAX(CASE WHEN field='gnss_pdop' THEN value_bigint END) AS pdop,
  MAX(CASE WHEN field='gsm_signal_level' THEN value_bigint END) AS gsm,
  MAX(CASE WHEN field='engine_rpm' THEN COALESCE(value_double, value_bigint) END) AS rpm,
  MAX(CASE WHEN field='fuel_rate_lph' THEN COALESCE(value_double, value_bigint) END) AS fuel_rate,
  MAX(CASE WHEN field='engine_load_percent' THEN COALESCE(value_double, value_bigint) END) AS engine_load,
  MAX(CASE WHEN field='fuel_level_percent' THEN COALESCE(value_double, value_bigint) END) AS fuel_level,
  MAX(CASE WHEN field='fuel_consumed_counted' THEN COALESCE(value_double, value_bigint) END) AS fuel_counter,
  MAX(CASE WHEN field='can_speed_kmh' THEN COALESCE(value_double, value_bigint) END) AS can_speed
FROM measurements
WHERE measurement='data' AND device=%s AND time >= %s AND time < %s
GROUP BY time ORDER BY time
"""

EYE_SQL = """
SELECT
  time, measurement,
  MAX(CASE WHEN field='eye_temperature_c' THEN value_double END) AS temp,
  MAX(CASE WHEN field='eye_humidity_percent' THEN value_bigint END) AS humidity,
  MAX(CASE WHEN field='eye_battery_percent' THEN value_bigint END) AS battery,
  MAX(CASE WHEN field='eye_battery_v' THEN value_double END) AS battery_v,
  BOOL_OR(CASE WHEN field='eye_magnet_closed' THEN value_bool END) AS magnet_closed,
  MAX(CASE WHEN field='mac' THEN value_text END) AS mac
FROM measurements
WHERE measurement IN ('eye1','eye2','eye3') AND device=%s AND time >= %s AND time < %s
GROUP BY time, measurement ORDER BY time, measurement
"""

DATES_SQL = """
SELECT DISTINCT (time AT TIME ZONE 'Europe/Tallinn')::date AS day
FROM measurements
WHERE measurement='data' AND device=%s
ORDER BY day DESC
LIMIT 60
"""

IMEIS_SQL = """
SELECT DISTINCT device
FROM measurements
WHERE measurement='data' AND device IS NOT NULL
ORDER BY device
"""

MAC_DATES_SQL = """
SELECT DISTINCT (time AT TIME ZONE 'Europe/Tallinn')::date AS day
FROM measurements
WHERE measurement IN ('eye1','eye2','eye3') AND field='mac' AND value_text=%s
ORDER BY day DESC
LIMIT 60
"""

MACS_SQL = """
SELECT DISTINCT value_text AS mac
FROM measurements
WHERE measurement IN ('eye1','eye2','eye3') AND field='mac' AND value_text IS NOT NULL
ORDER BY mac
"""

# Every EYE record for this attachment (identified by MAC), wherever and
# whenever it was seen - joins the "mac" field's (device, measurement, time)
# back onto the other fields sharing that same key, since each is its own
# row in the long/narrow schema. %(start)s / %(end)s are optional (NULL =
# unbounded) so the same query serves the "all time", "last N hours" and
# single-day views.
MAC_USAGE_SQL = """
WITH mac_keys AS (
  SELECT device, measurement, time
  FROM measurements
  WHERE measurement IN ('eye1','eye2','eye3') AND field='mac' AND value_text=%(mac)s
    AND (%(start)s::timestamptz IS NULL OR time >= %(start)s)
    AND (%(end)s::timestamptz IS NULL OR time < %(end)s)
), eye AS (
  SELECT k.device, k.measurement, k.time,
    MAX(CASE WHEN m.field='eye_temperature_c' THEN m.value_double END) AS temp,
    MAX(CASE WHEN m.field='eye_humidity_percent' THEN m.value_bigint END) AS humidity,
    MAX(CASE WHEN m.field='eye_battery_percent' THEN m.value_bigint END) AS battery,
    MAX(CASE WHEN m.field='eye_battery_v' THEN m.value_double END) AS battery_v,
    BOOL_OR(CASE WHEN m.field='eye_magnet_closed' THEN m.value_bool END) AS magnet_closed,
    MAX(CASE WHEN m.field='latitude' THEN m.value_double END) AS eye_lat,
    MAX(CASE WHEN m.field='longitude' THEN m.value_double END) AS eye_lon
  FROM mac_keys k
  JOIN measurements m ON m.device = k.device AND m.measurement = k.measurement AND m.time = k.time
  GROUP BY k.device, k.measurement, k.time
)
-- EYE records only carry the host tractor's coordinates since the gateway
-- started adding them; older ones borrow them from the tractor's own record
-- with the same timestamp (both come from the same AVL record).
SELECT e.device, e.measurement, e.time, e.temp, e.humidity, e.battery, e.battery_v, e.magnet_closed,
  COALESCE(e.eye_lat, p.lat) AS latitude,
  COALESCE(e.eye_lon, p.lon) AS longitude
FROM eye e
LEFT JOIN LATERAL (
  SELECT MAX(CASE WHEN d.field='latitude' THEN d.value_double END) AS lat,
         MAX(CASE WHEN d.field='longitude' THEN d.value_double END) AS lon
  FROM measurements d
  WHERE d.device = e.device AND d.measurement = 'data' AND d.time = e.time
    AND d.field IN ('latitude', 'longitude')
) p ON e.eye_lat IS NULL
ORDER BY e.time
"""


def fetch_rows(cur, sql, params):
    cur.execute(sql, params)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_dashboard(imei, date_str):
    start_utc, end_utc = day_bounds_utc(date_str)
    bundle = build_dashboard_range(imei, start_utc, end_utc)
    if bundle:
        bundle["kpi"]["date"] = date_str
        bundle["kpi"]["hours"] = None
    return bundle


def build_dashboard_hours(imei, hours):
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(hours=hours)
    bundle = build_dashboard_range(imei, start_utc, end_utc)
    if bundle:
        bundle["kpi"]["date"] = None
        bundle["kpi"]["hours"] = hours
    return bundle


def build_dashboard_range(imei, start_utc, end_utc):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            data_rows = fetch_rows(cur, DATA_SQL, (imei, start_utc, end_utc))
            eye_rows = fetch_rows(cur, EYE_SQL, (imei, start_utc, end_utc))
    finally:
        conn.close()

    if not data_rows:
        return None

    for r in data_rows:
        r["spd"] = float(r["speed"]) if r["speed"] is not None else 0.0
        if r.get("can_speed") is not None:
            r["spd"] = max(r["spd"], float(r["can_speed"]))

    # Flag GPS glitches once; used for distance and route.
    last_good = None
    cand = None
    for r in data_rows:
        r["gps_ok"] = False
        if r["latitude"] is None or r["longitude"] is None:
            continue
        t_s = r["time"].timestamp()
        lat, lon, spd = r["latitude"], r["longitude"], r["spd"]

        if last_good is None:
            r["gps_ok"] = True
            last_good = (t_s, lat, lon, spd)
            cand = None
            continue

        dt = t_s - last_good[0]
        if dt <= 0 or dt > GLITCH_RESET_S:
            r["gps_ok"] = True
            last_good = (t_s, lat, lon, spd)
            cand = None
            continue

        v_ms = (max(spd, last_good[3]) + GLITCH_SPEED_MARGIN_KMH) / 3.6
        dist = haversine(last_good[1], last_good[2], lat, lon)
        if dist <= GLITCH_BASE_M + v_ms * dt:
            r["gps_ok"] = True
            last_good = (t_s, lat, lon, spd)
            cand = None
            continue

        # Check candidate confirmation
        if cand is not None:
            cand_ts, cand_lat, cand_lon, cand_spd = cand
            dt_cand = t_s - cand_ts
            if 0 <= dt_cand <= 30:
                cand_v_ms = (max(spd, cand_spd) + GLITCH_SPEED_MARGIN_KMH) / 3.6
                cand_dist = haversine(cand_lat, cand_lon, lat, lon)
                if cand_dist <= GLITCH_BASE_M + cand_v_ms * max(dt_cand, 1):
                    # Confirmed new position by consecutive reading
                    r["gps_ok"] = True
                    last_good = (t_s, lat, lon, spd)
                    cand = None
                    continue

        cand = (t_s, lat, lon, spd)

    gaps = []
    total_dist_m = 0.0
    moving_seconds = 0.0
    max_speed = 0.0
    last_pos = None
    for i in range(1, len(data_rows)):
        a, b = data_rows[i - 1], data_rows[i]
        dt_s = (b["time"] - a["time"]).total_seconds()
        if dt_s > GAP_S:
            gaps.append({
                "from": a["time"].isoformat(),
                "to": b["time"].isoformat(),
                "minutes": round(dt_s / 60, 1),
            })
            last_pos = None
            continue
        if b["gps_ok"]:
            if last_pos is not None:
                total_dist_m += haversine(last_pos["latitude"], last_pos["longitude"], b["latitude"], b["longitude"])
            last_pos = b
        if b["movement"]:
            moving_seconds += dt_s
        max_speed = max(max_speed, b["spd"])

    moving_speeds = [r["spd"] for r in data_rows if r["movement"]]
    avg_speed_moving = round(sum(moving_speeds) / len(moving_speeds), 1) if moving_speeds else 0

    trips = 0
    prev_ign = None
    for r in data_rows:
        if r["ignition"] is None:
            continue
        if r["ignition"] and prev_ign is not True:
            trips += 1
        prev_ign = r["ignition"]

    volts = [r["voltage"] for r in data_rows if r["voltage"] is not None]
    sats = [r["satellites"] for r in data_rows if r["satellites"] is not None]
    hdops = [r["hdop"] for r in data_rows if r["hdop"] is not None]
    gsms = [r["gsm"] for r in data_rows if r["gsm"] is not None]

    kpi = {
        "imei": imei,
        "record_count": len(data_rows),
        "start": data_rows[0]["time"].isoformat(),
        "end": data_rows[-1]["time"].isoformat(),
        "gaps": gaps,
        "total_distance_km": round(total_dist_m / 1000, 2),
        "moving_minutes": round(moving_seconds / 60, 1),
        "max_speed_kmh": max_speed,
        "avg_speed_kmh_when_moving": avg_speed_moving,
        "ignition_cycles": trips,
        "voltage_min": min(volts) if volts else None,
        "voltage_max": max(volts) if volts else None,
        "voltage_avg": round(sum(volts) / len(volts), 2) if volts else None,
        "satellites_avg": round(sum(sats) / len(sats), 1) if sats else None,
        "hdop_avg": round(sum(hdops) / len(hdops), 2) if hdops else None,
        "gsm_avg": round(sum(gsms) / len(gsms), 2) if gsms else None,
    }

    def to_xy(rows, key):
        pts = []
        for i, r in enumerate(rows):
            x = int(r["time"].timestamp() * 1000)
            if i > 0 and (r["time"] - rows[i - 1]["time"]).total_seconds() > GAP_S:
                pts.append({"x": x - 1, "y": None})
            val = r[key]
            pts.append({"x": x, "y": float(val) if val is not None else None})
        return pts

    # FMC150 CAN values (fuel_consumed_counted, AVL 107) come as a counter in
    # 0.1 l steps, not a rate. Turn it into litres used since the window start,
    # summing only forward steps so a counter reset doesn't go negative.
    fuel_used = 0.0
    prev_counter = None
    for r in data_rows:
        c = r["fuel_counter"]
        r["fuel_used"] = None
        if c is None:
            continue
        if prev_counter is not None and c > prev_counter:
            fuel_used += (float(c) - float(prev_counter)) * FUEL_COUNTER_L
        prev_counter = c
        r["fuel_used"] = round(fuel_used, 1)
    kpi["fuel_used_l"] = round(fuel_used, 1) if prev_counter is not None else None

    # CAN fuel level is junk with ignition off (bus asleep, reports 0-3 %) and
    # ramps up from 0 for a minute or two after power-up (gauge damping), so
    # only trust it once ignition has been on for FUEL_LEVEL_SETTLE_S.
    ign_since = None
    for r in data_rows:
        if not r["ignition"]:
            ign_since = None
        elif ign_since is None:
            ign_since = r["time"]
        if r["fuel_level"] is not None and (
                ign_since is None or (r["time"] - ign_since).total_seconds() < FUEL_LEVEL_SETTLE_S):
            r["fuel_level"] = None

    def sparse_xy(key):
        # CAN fields are only present on some records; plotting them on the
        # full row set leaves nulls between every sample and breaks the line.
        rows = [r for r in data_rows if r[key] is not None]
        return to_xy(downsample(rows, 380), key)

    data_ds = downsample(data_rows, 380)
    series = {
        "spd": to_xy(data_ds, "spd"),
        "volt": to_xy(data_ds, "voltage"),
        "sat": to_xy(data_ds, "satellites"),
        "hdop": to_xy(data_ds, "hdop"),
        "rpm": sparse_xy("rpm"),
        "fuel": sparse_xy("fuel_rate"),
        "load": sparse_xy("engine_load"),
        "fuel_level": sparse_xy("fuel_level"),
        "fuel_used": sparse_xy("fuel_used"),
    }

    route_pts = [
        {"lat": r["latitude"], "lon": r["longitude"], "t": int(r["time"].timestamp() * 1000)}
        for r in data_rows if r["gps_ok"]
    ]
    route = downsample(route_pts, 600)
    route_frozen = find_frozen_positions(data_rows)

    eye_by_sensor = {"eye1": [], "eye2": [], "eye3": []}
    for r in eye_rows:
        if r["measurement"] in eye_by_sensor:
            eye_by_sensor[r["measurement"]].append(r)

    eye_series = {}
    eye_stats = {}
    for key, rows in eye_by_sensor.items():
        rows_ds = downsample(rows, 380)
        eye_series[key] = {
            "temp": to_xy(rows_ds, "temp"),
            "hum": to_xy(rows_ds, "humidity"),
        }

        temps = [r["temp"] for r in rows if r["temp"] is not None]
        hums = [r["humidity"] for r in rows if r["humidity"] is not None]
        batts = [r["battery"] for r in rows if r["battery"] is not None]
        batts_v = [r["battery_v"] for r in rows if r["battery_v"] is not None]
        mac = next((r["mac"] for r in rows if r["mac"]), None)

        toggles = 0
        prev = None
        for r in rows:
            if r["magnet_closed"] is None:
                continue
            if prev is not None and r["magnet_closed"] != prev:
                toggles += 1
            prev = r["magnet_closed"]

        eye_stats[key] = {
            "mac": mac,
            "temp_min": min(temps) if temps else None,
            "temp_max": max(temps) if temps else None,
            "humidity_min": min(hums) if hums else None,
            "humidity_max": max(hums) if hums else None,
            "battery": int(batts[-1]) if batts else None,
            "battery_v": batts_v[-1] if batts_v else None,
            "magnet_toggles": toggles,
            "magnet_closed_now": prev,
        }

    return {"kpi": kpi, "series": series, "route": route, "routeFrozen": route_frozen,
            "eye": eye_series, "eyeStats": eye_stats}


def vehicle_data_for_sightings(sightings):
    """Tractor KPIs (records, distance, moving time, max speed) summed over
    the stretches the attachment was seen on each tractor - i.e. what the
    tractor did while this attachment was hitched to it - plus the tractor's
    route for each of those stretches. Returns (kpi_or_None, routes)."""
    total = None
    routes = []
    for s in sightings:
        if s["device"] is None:
            continue
        start = datetime.fromisoformat(s["first_seen"])
        end = datetime.fromisoformat(s["last_seen"]) + timedelta(seconds=1)
        b = build_dashboard_range(s["device"], start, end)
        if not b:
            continue
        if b["route"]:
            routes.append({"device": s["device"], "day": s["day"],
                           "points": b["route"], "frozen": b["routeFrozen"]})
        k = b["kpi"]
        if total is None:
            total = {"record_count": 0, "start": k["start"], "end": k["end"], "total_distance_km": 0.0,
                     "moving_minutes": 0.0, "max_speed_kmh": 0.0, "ignition_cycles": 0, "_speed_x_min": 0.0}
        total["record_count"] += k["record_count"]
        total["start"] = min(total["start"], k["start"])
        total["end"] = max(total["end"], k["end"])
        total["total_distance_km"] += k["total_distance_km"]
        total["moving_minutes"] += k["moving_minutes"]
        total["max_speed_kmh"] = max(total["max_speed_kmh"], k["max_speed_kmh"])
        total["ignition_cycles"] += k["ignition_cycles"]
        total["_speed_x_min"] += k["avg_speed_kmh_when_moving"] * k["moving_minutes"]
    if total is None:
        return None, routes
    sxm = total.pop("_speed_x_min")
    total["avg_speed_kmh_when_moving"] = round(sxm / total["moving_minutes"], 1) if total["moving_minutes"] else 0
    total["total_distance_km"] = round(total["total_distance_km"], 2)
    total["moving_minutes"] = round(total["moving_minutes"], 1)
    return total, routes


# An attachment's EYE sensor can be in BLE range of several tractors at once
# (e.g. all parked in the yard) - a record is ambiguous when another tractor
# also received the same sensor within this many seconds (records come every
# ~5 min), and then isn't attributed to any tractor.
AMBIGUOUS_WINDOW_S = 360


def mark_ambiguous(rows):
    """Sets r["ambiguous"] on rows (sorted by time) seen by more than one device."""
    times = {}
    for r in rows:
        times.setdefault(r["device"], []).append(r["time"].timestamp())
    for r in rows:
        t = r["time"].timestamp()
        r["ambiguous"] = False
        for dev, ts in times.items():
            if dev == r["device"]:
                continue
            i = bisect.bisect_left(ts, t - AMBIGUOUS_WINDOW_S)
            if i < len(ts) and ts[i] <= t + AMBIGUOUS_WINDOW_S:
                r["ambiguous"] = True
                break


def build_mac_usage(mac, hours=None, date_str=None):
    """History for one EYE sensor (attachment), identified by its MAC,
    across every tractor (device) it has ever been seen on - answers 'how
    long was this attachment used, independent of tractor'. With `hours`
    set, restricted to the last N hours from now; with `date_str`, to that
    Tallinn calendar day; otherwise full history."""
    start_utc = end_utc = None
    if hours:
        start_utc = datetime.now(timezone.utc) - timedelta(hours=hours)
    elif date_str:
        start_utc, end_utc = day_bounds_utc(date_str)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            rows = fetch_rows(cur, MAC_USAGE_SQL, {"mac": mac, "start": start_utc, "end": end_utc})
    finally:
        conn.close()

    if not rows:
        return None

    mark_ambiguous(rows)
    attributed = [r for r in rows if not r["ambiguous"]]

    # Sightings: contiguous stretches on the same device, split into
    # per-day segments for a readable summary table. Ambiguous stretches
    # get device None (seen by several tractors, not attributed).
    sightings = []
    cur_key = None
    for r in rows:
        day = r["time"].astimezone(TZ).date().isoformat()
        device = None if r["ambiguous"] else r["device"]
        key = (device, day)
        if key != cur_key:
            sightings.append({
                "device": device, "day": day,
                "first_seen": r["time"].isoformat(), "last_seen": r["time"].isoformat(),
                "records": 0,
            })
            cur_key = key
        s = sightings[-1]
        s["last_seen"] = r["time"].isoformat()
        s["records"] += 1

    temps = [r["temp"] for r in rows if r["temp"] is not None]
    hums = [r["humidity"] for r in rows if r["humidity"] is not None]
    batts = [r["battery"] for r in rows if r["battery"] is not None]
    batts_v = [r["battery_v"] for r in rows if r["battery_v"] is not None]

    toggles = 0
    prev_magnet = None
    for r in rows:
        if r["magnet_closed"] is None:
            continue
        if prev_magnet is not None and r["magnet_closed"] != prev_magnet:
            toggles += 1
        prev_magnet = r["magnet_closed"]

    stats = {
        "mac": mac,
        "hours": hours,
        "first_seen": rows[0]["time"].isoformat(),
        "last_seen": rows[-1]["time"].isoformat(),
        "record_count": len(rows),
        "device_count": len({r["device"] for r in attributed}),
        "ambiguous_count": len(rows) - len(attributed),
        "day_count": len({r["time"].astimezone(TZ).date() for r in rows}),
        "temp_min": min(temps) if temps else None,
        "temp_max": max(temps) if temps else None,
        "temp_avg": round(sum(temps) / len(temps), 2) if temps else None,
        "humidity_min": min(hums) if hums else None,
        "humidity_max": max(hums) if hums else None,
        "battery": int(batts[-1]) if batts else None,
        "battery_v": batts_v[-1] if batts_v else None,
        "magnet_toggles": toggles,
        "magnet_closed_now": prev_magnet,
    }

    def to_xy(rows_, key):
        pts = []
        for i, r in enumerate(rows_):
            x = int(r["time"].timestamp() * 1000)
            broke = False
            if i > 0:
                prev = rows_[i - 1]
                # same physical sensor whichever tractor relayed it - only data gaps break the line
                if (r["time"] - prev["time"]).total_seconds() > GAP_S:
                    broke = True
            if broke:
                pts.append({"x": x - 1, "y": None})
            val = r[key]
            pts.append({"x": x, "y": float(val) if val is not None else None})
        return pts

    rows_ds = downsample(rows, 500)
    series = {
        "temp": to_xy(rows_ds, "temp"),
        "hum": to_xy(rows_ds, "humidity"),
        "magnet": to_xy(rows_ds, "magnet_closed"),  # bool -> 1.0 (kinni) / 0.0 (lahti)
    }

    # Where the attachment itself was when each EYE reading came in.
    eye_points = downsample([
        {"lat": r["latitude"], "lon": r["longitude"], "t": int(r["time"].timestamp() * 1000),
         "device": r["device"], "temp": r["temp"]}
        for r in attributed
        if r["latitude"] is not None and r["longitude"] is not None
        and not (r["latitude"] == 0 and r["longitude"] == 0)
    ], 500)

    vehicle_kpi, routes = vehicle_data_for_sightings(sightings)
    return {"stats": stats, "sightings": sightings, "series": series,
            "vehicleKpi": vehicle_kpi, "routes": routes, "eyePoints": eye_points}


@app.route("/api/imeis")
def api_imeis():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(IMEIS_SQL)
            imeis = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"imeis": imeis})


@app.route("/api/dates")
def api_dates():
    imei = request.args.get("imei")
    if not imei:
        return jsonify({"error": "imei is required"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DATES_SQL, (imei,))
            dates = [row[0].isoformat() for row in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"dates": dates})


def parse_positive_int(raw):
    value = int(raw)
    if value <= 0:
        raise ValueError("must be positive")
    return value


@app.route("/api/dashboard")
def api_dashboard():
    imei = request.args.get("imei")
    if not imei:
        return jsonify({"error": "imei is required"}), 400

    hours_str = request.args.get("hours")
    if hours_str:
        try:
            hours = parse_positive_int(hours_str)
        except ValueError:
            return jsonify({"error": "invalid hours, expected a positive integer"}), 400
        bundle = build_dashboard_hours(imei, hours)
        if bundle is None:
            return jsonify({"error": "no data in this window"}), 404
        return jsonify(bundle)

    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.now(TZ).strftime("%Y-%m-%d")

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "invalid date, expected YYYY-MM-DD"}), 400

    bundle = build_dashboard(imei, date_str)
    if bundle is None:
        return jsonify({"error": "no data for this date"}), 404
    return jsonify(bundle)


@app.route("/api/macs")
def api_macs():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(MACS_SQL)
            macs = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"macs": macs})


@app.route("/api/mac-usage")
def api_mac_usage():
    mac = request.args.get("mac")
    if not mac:
        return jsonify({"error": "mac is required"}), 400

    hours = None
    hours_str = request.args.get("hours")
    if hours_str:
        try:
            hours = parse_positive_int(hours_str)
        except ValueError:
            return jsonify({"error": "invalid hours, expected a positive integer"}), 400

    date_str = request.args.get("date")
    if date_str and not hours:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "invalid date, expected YYYY-MM-DD"}), 400
    else:
        date_str = None

    bundle = build_mac_usage(mac, hours, date_str)
    if bundle is None:
        return jsonify({"error": "no data for this mac"}), 404
    return jsonify(bundle)


@app.route("/api/mac-dates")
def api_mac_dates():
    mac = request.args.get("mac")
    if not mac:
        return jsonify({"error": "mac is required"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(MAC_DATES_SQL, (mac,))
            dates = [row[0].isoformat() for row in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"dates": dates})


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8811)
