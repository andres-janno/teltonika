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
  style bridge) to `teltonika/<imei>/data`, and EYE sensor data the same way
  to `teltonika/<imei>/eye` (includes `imei` and `mac`, see below). The raw,
  untranslated IO map for each record is published separately to
  `teltonika/<imei>/data/raw` and `teltonika/<imei>/eye/raw` for debugging.

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

The EYE sensor lives on an implement/attachment, not the tractor itself, so
it gets moved between tractors (FMC650 devices) over time. Its BLE MAC
address isn't sent over the AVL data stream, so it can't be read from the
data automatically — it's tracked in [imei_eye_mac.json](imei_eye_mac.json)
instead, mapping each tractor's IMEI to the MAC of the attachment currently
hitched to it:

```json
{
  "860848081816598": "AA:BB:CC:DD:EE:FF"
}
```

When you move an attachment to a different tractor, update this file. It is
reloaded automatically (checked every 30s) — no restart needed. Each
`teltonika/<imei>/eye` MQTT message includes both `imei` (the tractor) and
`mac` (the attachment) at the time it was sent, so usage duration per
attachment can be computed downstream by grouping messages by `mac`,
independent of which tractor it was on.

## Wiring up to TimescaleDB (Node-RED)

`teltonika/<imei>/data` and `teltonika/<imei>/eye` carry flat JSON objects,
one MQTT message per AVL record, so they can be piped straight into
[node-red-contrib-timescaledb](https://flows.nodered.org/node/node-red-contrib-timescaledb).

That node derives the `measurement` name from the MQTT topic using a mapping
string, one segment per topic level. Our topics only have 3 levels
(`teltonika` / `<imei>` / `data` or `eye`), so configure the node's topic
mapping as:

```
-/device/measurement
```

`-` skips the `teltonika` prefix, `device` becomes the IMEI (tractor), and
`measurement` becomes `data` or `eye`. With a JSON object payload the node
takes each remaining top-level key as a field automatically, so no `field`
position is needed in the mapping.

If you see `Measurement is required (from topic mapping)`, the configured
mapping string doesn't have a `measurement` position lining up with our
topic's segment count — recheck it against the mapping above.

## IO mapping notes

`IO_NAMES` in [teltonika2mqtt.py](teltonika2mqtt.py) maps Teltonika AVL IDs to readable
parameter names. On this device model, AVL IDs 701, 705, 709, 713, 729 and
730 carry EYE sensor readings (temperature, battery, humidity, magnet state,
counters) rather than the CAN/FMS/Tachograph fields those IDs are used for
on other models — adjust the mapping if you connect a different device type.

## Development
Mapping from server to local filesystem for development in code.
```bash
sshfs debian@uvn-243-120.tll07.zonevs.eu:/home/debian/teltonika ~/Progemine/teltonika -o allow_other
cd ~/Progemine/teltonika
code .
```
