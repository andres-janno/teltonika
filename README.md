# teltonika2mqtt

TCP gateway that receives Teltonika AVL data (Codec 8 / Codec 8 Extended) from
tracker devices and publishes decoded records to an MQTT broker.

## Features

- Async TCP server handling multiple simultaneous device connections
- Decodes Codec 8 and Codec 8 Extended AVL packets, including GPS, IO
  elements and variable-length (X) fields
- Translates raw Teltonika IO IDs into named, unit-converted values
  (e.g. voltages in mV → V, ignition/movement as booleans, EYE sensor
  temperature/humidity/magnet state)
- Publishes FMC650 device data (GPS, voltage, ignition, etc.) as a flat JSON
  object (one key per field, no nesting - ready for a TimescaleDB/Node-RED
  style bridge) to `teltonika/<imei>/data`. Up to two EYE sensors can be
  paired at once (Bluetooth 4.0 slot 1/2); each is published as its own flat
  JSON object (temperature, humidity, magnet state, `mac`, see below) to its
  own topic, `teltonika/<imei>/eye1` and `teltonika/<imei>/eye2` - never
  mixed into one message, since they're two distinct physical sensors.
  Neither payload repeats the IMEI or GPS position - the IMEI is already the
  `device` tag derived from the topic itself, and the eye topics share the
  same timestamp as the `/data` record they came from, so position can be
  joined in by `time` + `device` if needed. The raw, untranslated IO map for
  each record is published separately to `teltonika/<imei>/data/raw` and
  `teltonika/<imei>/eye/raw` (shared by both sensor slots) for debugging.

## Requirements

- Python 3.13+
- [paho-mqtt](https://pypi.org/project/paho-mqtt/)

```bash
pip install paho-mqtt
```

## Configuration

Edit the constants at the top of [teltonika2mqtt.py](teltonika2mqtt.py):

| Constant       | Default       | Description                          |
|----------------|---------------|---------------------------------------|
| `MQTT_BROKER`  | `127.0.0.1`   | MQTT broker host                      |
| `MQTT_PORT`    | `1883`        | MQTT broker port                      |
| `TCP_PORT`     | `5027`        | Port to listen on for device traffic  |

## Running

```bash
python teltonika2mqtt.py
```

Point your Teltonika device's server settings at this host on `TCP_PORT`
(protocol: TCP, Codec 8 / Codec 8 Extended).

## Running as a systemd service

A ready-made unit file is provided as [teltonika2mqtt.service](teltonika2mqtt.service). It
assumes the project lives at `/home/debian/teltonika` and runs as user
`debian` — adjust `User`, `WorkingDirectory` and `ExecStart` if your
deployment path or user differs.

```bash
sudo cp teltonika2mqtt.service /etc/systemd/system/teltonika2mqtt.service
sudo systemctl daemon-reload
sudo systemctl enable --now teltonika2mqtt.service
```

Check status and logs:

```bash
sudo systemctl status teltonika2mqtt.service
sudo journalctl -u teltonika2mqtt.service -f
```

After changing `teltonika2mqtt.py` or the unit file:

```bash
sudo systemctl restart teltonika2mqtt.service   # after editing teltonika2mqtt.py
sudo systemctl daemon-reload && sudo systemctl restart teltonika2mqtt.service   # after editing the unit file
```

## Tracking which EYE sensor (attachment) is on which tractor

Each EYE sensor lives on an implement/attachment, not the tractor itself, so
it gets moved between tractors (FMC650 devices) over time - and a given
tractor's slot 1 and slot 2 sensors are two different physical attachments.
Each `teltonika/<imei>/eye1` / `eye2` message includes `mac`, the attachment
currently identified in that slot, so usage duration per attachment can be
computed downstream by grouping messages by `mac`, independent of which
tractor (or slot) it was on.

Sensor readings (temperature, humidity, magnet state, ...) come from the
paired Bluetooth 4.0 connection (AVL IDs 701-730), which tells us nothing
about MAC addresses. The MAC comes from a completely separate subsystem -
Beacon Capturing (AVL 548, or older/simpler AVL 385) - which passively
listens for the sensor's Eddystone/iBeacon broadcast and, when configured
with **Beacon ID source: MAC address**, reports its real MAC. A single 548
element can report both sensors' MACs at once but doesn't say which
belongs to which slot, so `resolve_slot_macs()` matches each detected MAC
against the slot it was last confirmed for (seeded from
[imei_eye_mac.json](imei_eye_mac.json)), and falls back to that static
config if nothing live has been seen yet for a slot:

```json
{
  "860848081816598": {
    "1": "7C:D9:F4:12:AA:A6",
    "2": "7C:D9:F4:12:AA:C9"
  }
}
```

(A plain string instead of a `{"1": ..., "2": ...}` object is also accepted,
for backward compatibility - it's treated as slot 1's MAC.)

Update this file when you move an attachment to a different tractor/slot.
It's reloaded automatically (checked every 30s) — no restart needed. Once a
slot's MAC has been confirmed live via beacon capturing, that takes priority
over the file until the gateway restarts.

## Wiring up to TimescaleDB (Node-RED)

`teltonika/<imei>/data` and `teltonika/<imei>/eye` carry flat JSON objects,
one MQTT message per AVL record, so they can be piped straight into
[node-red-contrib-timescaledb](https://flows.nodered.org/node/node-red-contrib-timescaledb).

That node derives the `measurement` name from the MQTT topic using a mapping
string, one segment per topic level. Our topics only have 3 levels
(`teltonika` / `<imei>` / `data`, `eye1` or `eye2`), so configure the node's
topic mapping as:

```
-/device/measurement
```

`-` skips the `teltonika` prefix, `device` becomes the IMEI (tractor), and
`measurement` becomes `data`, `eye1` or `eye2`. With a JSON object payload
the node takes each remaining top-level key as a field automatically, so no
`field` position is needed in the mapping. Subscribe the MQTT-in node to
`teltonika/+/+` (not `teltonika/#`) so it picks up exactly these 3-level
topics and skips the `.../data/raw` and `.../eye/raw` debug topics.

If you see `Measurement is required (from topic mapping)`, the configured
mapping string doesn't have a `measurement` position lining up with our
topic's segment count — recheck it against the mapping above.

## IO mapping notes

`EYE_SLOT_FIELDS` in [teltonika2mqtt.py](teltonika2mqtt.py) maps each
Bluetooth 4.0 sensor slot's AVL IDs to readable field names - slot 1 uses
701/705/709/713/729/730, slot 2 the same fields +1 (702/710/714/731/732).
Neither is the standard CAN/FMS/Tachograph meaning those IDs are documented
for on other Teltonika models; both were reverse-engineered against this
specific device and firmware. `IO_NAMES` covers everything else. Adjust
both if you connect a different device type.

## Development
Mapping from server to local filesystem for development in code.
```bash
sshfs debian@uvn-243-120.tll07.zonevs.eu:/home/debian/teltonika ~/Progemine/teltonika -o allow_other
cd ~/Progemine/teltonika
code .
```
Tunnel to NodeRED
```bash
ssh -L 1880:localhost:1880 debian@uvn-243-120.tll07.zonevs.eu
```
Open in browser: http://localhost:1880/
