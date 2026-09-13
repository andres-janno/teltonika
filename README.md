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
- Publishes each record as JSON to `teltonika/<imei>/data`

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
Tunnel to NodeRED
```bash
ssh -L 1880:localhost:1880 debian@uvn-243-120.tll07.zonevs.eu
```
Open in browser: http://localhost:1880/
