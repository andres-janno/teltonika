# Uue EYE anduri lisamine

Juhend, kuidas ühendada uus Teltonika EYE (BTSMP1) Bluetooth andur FMC650
traktoriseadmega ja saada selle andmed `teltonika2mqtt.py` gateway kaudu
MQTT-i / TimescaleDB-sse.

Süsteem toetab **kuni 4 EYE andurit korraga** ühe FMC650 kohta (slotid 1-4).

## 1. Andur füüsiliselt paarita (Teltonika Configurator)

1. Leia anduri MAC-aadress (kirjas anduri korpusel, või loe nRF Connect
   äpiga otse andurilt).
2. Ava Teltonika Configurator, ühendu FMC650 seadmega.
3. Mine **Bluetooth 4.0** lehele.
4. Vali esimene vaba slot (**1st / 2nd / 3rd / 4th Sensor**) → **Working
   mode: Advanced**.
5. Sisesta anduri MAC **Settings → MAC** väljale.
6. Vajuta preset-nupp ja vali **Preset Selection** nimekirjast:
   - **EYE SENSOR** — ainult sensoriandmed (temperatuur/niiskus/aku/magnet),
     ilma MAC-i live-tuvastuseta AVL andmetes.
   - **EYE SENSOR + Eddystone** — sensoriandmed JA anduri MAC on AVL andmetes
     live tuvastatav (vt punkt 2) — **soovitatud**, kui andur toetab
     Eddystone broadcast'i.
   - **EYE SENSOR + iBeacon** — sama, aga iBeacon-vormingus.
7. Kontrolli anduri enda seadistuses (ELA/EYE äpp või NFC), et selle
   broadcast-formaat **klapib valitud presetiga** (nt Eddystone preset ↔
   andur peab broadcast'ima Eddystone formaadis, mitte "Sensors" moodi).
8. Salvesta ja saada konfiguratsioon seadmesse.

## 2. Kontrolli Beacon Capturing seadistust (live MAC tuvastus)

See on juba korra seadistatud (Bluetooth 4.0 → **Beacon List** leht) ja
kehtib kõigi anduritele korraga — uue anduri lisamisel EI PEA seda muutma,
kui kasutad "EYE SENSOR + Eddystone" presetit sama Manufacturer ID-ga.

Kontrolliks:
- **Beacon Settings → Beacon Detection: All**
- **Beacon mode: Advanced**
- **Beacons Capturing Configuration** tabelis on rida:
  `Manufacturer ID: AAFE0000`, `Beacon ID source: MAC address`

Kui puudub, lisa see rida (vt README.md "Wiring up to TimescaleDB" alt
täpsemat konteksti Beacon-mehhanismi kohta).

## 3. Uuenda `imei_eye_mac.json`

Lisa uue anduri MAC järgmise vaba slot-numbri alla (traktori IMEI kohta):

```json
{
  "<imei>": {
    "1": "AA:BB:CC:DD:EE:FF",
    "2": "11:22:33:44:55:66",
    "3": "UUS:AN:DUR:I:MAC:SIIN"
  }
}
```

See on **staatiline varuandmestik** — kasutatakse ainult siis, kui live
MAC-tuvastus (Beacon Capturing) pole veel midagi tuvastanud (nt gateway
äsja taaskäivitatud). Fail laetakse automaatselt iga 30s tagant uuesti,
teenust ei pea taaskäivitama.

**Tähtis:** JSON-is ei tohi sama võti (`"1"`, `"2"` jne) korduda — kui
lisad rea, kontrolli, et number oleks unikaalne, muidu jääb kehtima ainult
viimane väärtus, vaikselt, ilma veata.

## 4. Taaskäivita gateway teenus

```bash
sudo systemctl restart teltonika2mqtt.service
```

Ilma selleta ei jõustu koodimuudatused (kui neid tegid) ega
`imei_eye_mac.json` esimene laadimine käivitusel.

## 5. Kontrolli, kas uus slot juba toimib

Vaata Node-RED debug node'ist (või MQTT kliendist) topicut
`teltonika/<imei>/eye<N>` (N = slot number, nt `eye3`). Kui `mac` väli
tuleb õigesti, aga sensoriväljad (`eye_temperature_c` jne) puuduvad, jätka
punktiga 6.

## 6. Kui sensoriväljad puuduvad — tuvasta uued AVL ID-d

Meie seade kasutab **dokumenteerimata, tuletatud ID-skeemi**: iga väli
järgib mustrit `baas-ID + (slot - 1)`:

| Väli | Slot 1 | Slot 2 | Slot 3 | Slot 4 |
|---|---|---|---|---|
| temperatuur | 701 | 702 | 703 | 704 |
| aku | 705 | 706 | 707 | 708 |
| niiskus | 709 | 710 | 711 | 712 |
| magnet | 713 | 714 | 715 | 716 |

Slotid 1-4 (need 4 välja) on juba `EYE_SLOT_FIELDS` sõnastikus
[teltonika2mqtt.py](../teltonika2mqtt.py) kaetud — **kui lisad 5. anduri**
(FMC650 riistvara seda tavaliselt ei toeta) või kui uue anduri väärtused
mustrisse ei sobi, tuleb need käsitsi tuvastada:

1. Vaata `teltonika/<imei>/data/raw` topicut (uued väljad, mis ei kuulu
   ühelegi teadaolevale slotile, satuvad sinna `io_<ID>` kujul).
2. Võrdle väärtusi tuntud mustriga (temperatuur ~15-35°C vahemikus ×100,
   niiskus 0-100%, aku 0-100%, magnet lipubaitidena ~180-200 vahemikus).
3. Lisa uued read `EYE_SLOT_FIELDS` sõnastikku, samas vormingus:
   ```python
   <uus_id>: (<slot>, "eye_temperature_c"),
   ```
4. Lisa ka `translate_io_values()` teisendusharudesse (temperatuur/100,
   magnet bit 3 kontroll), kui väli seda vajab.
5. Lisa uus ID ka `EYE_IO_IDS` hulka (kui pole juba `EYE_SLOT_FIELDS`
   kaudu automaatselt kaasatud).
6. Taaskäivita teenus ja kontrolli uuesti.

Liikumis-/magnetiloendureid (729-740 vahemik) me teadlikult ei avalda
nimeliste väljadena üheski slotis — need on nähtavad ainult `raw`
topicutes, kui neid kunagi peaks vaja minema.

## 7. Kirjuta MAC üles

Kui andur on kunagi mõne teise traktori taha liigub, ei pea koodi
muutma — piisab [imei_eye_mac.json](../imei_eye_mac.json) uuendamisest uue
traktori IMEI ja sama MAC-iga (vt README.md "Tracking which EYE sensor").
