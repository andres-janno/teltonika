import math
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

DB_DSN = os.environ.get(
    "TELTONIKA_DB_DSN",
    "dbname=agro_tsdb user=teltonika_api password=CHANGE_ME host=127.0.0.1",
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


def downsample(rows, target=380):
    n = len(rows)
    if n <= target:
        return rows
    step = math.ceil(n / target)
    out = rows[0::step]
    if rows and rows[-1] is not out[-1]:
        out.append(rows[-1])
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
  MAX(CASE WHEN field='gsm_signal_level' THEN value_bigint END) AS gsm
FROM measurements
WHERE measurement='data' AND time >= %s AND time < %s
GROUP BY time ORDER BY time
"""

EYE_SQL = """
SELECT
  time, measurement,
  MAX(CASE WHEN field='eye_temperature_c' THEN value_double END) AS temp,
  MAX(CASE WHEN field='eye_humidity_percent' THEN value_bigint END) AS humidity,
  MAX(CASE WHEN field='eye_battery_percent' THEN value_bigint END) AS battery,
  BOOL_OR(CASE WHEN field='eye_magnet_closed' THEN value_bool END) AS magnet_closed,
  MAX(CASE WHEN field='mac' THEN value_text END) AS mac
FROM measurements
WHERE measurement IN ('eye1','eye2','eye3') AND time >= %s AND time < %s
GROUP BY time, measurement ORDER BY time, measurement
"""

DATES_SQL = """
SELECT DISTINCT (time AT TIME ZONE 'Europe/Tallinn')::date AS day
FROM measurements
WHERE measurement='data'
ORDER BY day DESC
LIMIT 60
"""


def fetch_rows(cur, sql, params):
    cur.execute(sql, params)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_dashboard(date_str):
    start_utc, end_utc = day_bounds_utc(date_str)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            data_rows = fetch_rows(cur, DATA_SQL, (start_utc, end_utc))
            eye_rows = fetch_rows(cur, EYE_SQL, (start_utc, end_utc))
    finally:
        conn.close()

    if not data_rows:
        return None

    for r in data_rows:
        r["spd"] = float(r["speed"]) if r["speed"] is not None else 0.0

    gaps = []
    total_dist_m = 0.0
    moving_seconds = 0.0
    max_speed = 0.0
    for i in range(1, len(data_rows)):
        a, b = data_rows[i - 1], data_rows[i]
        dt_s = (b["time"] - a["time"]).total_seconds()
        if dt_s > GAP_S:
            gaps.append({
                "from": a["time"].isoformat(),
                "to": b["time"].isoformat(),
                "minutes": round(dt_s / 60, 1),
            })
            continue
        if a["latitude"] is not None and b["latitude"] is not None:
            total_dist_m += haversine(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
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
        "date": date_str,
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

    data_ds = downsample(data_rows, 380)
    series = {
        "spd": to_xy(data_ds, "spd"),
        "volt": to_xy(data_ds, "voltage"),
        "sat": to_xy(data_ds, "satellites"),
        "hdop": to_xy(data_ds, "hdop"),
    }

    route_pts = [
        {"lat": r["latitude"], "lon": r["longitude"], "t": int(r["time"].timestamp() * 1000)}
        for r in data_rows if r["latitude"] is not None and r["longitude"] is not None
    ]
    route = downsample(route_pts, 600)

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
            "magnet_toggles": toggles,
            "magnet_closed_now": prev,
        }

    return {"kpi": kpi, "series": series, "route": route, "eye": eye_series, "eyeStats": eye_stats}


@app.route("/api/dates")
def api_dates():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DATES_SQL)
            dates = [row[0].isoformat() for row in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({"dates": dates})


@app.route("/api/dashboard")
def api_dashboard():
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.now(TZ).strftime("%Y-%m-%d")

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "invalid date, expected YYYY-MM-DD"}), 400

    bundle = build_dashboard(date_str)
    if bundle is None:
        return jsonify({"error": "no data for this date"}), 404
    return jsonify(bundle)


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8811)
