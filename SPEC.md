# SPEC: smb-kafka-watcher

Kleiner Rust-Dienst, der ein SMB-Share überwacht und für **neu hinzugekommene Dateien** ein Event auf Kafka schreibt. Der Dateiinhalt wird **nicht** gelesen oder übertragen.

## Ziel

- Neue Dateien auf einem SMB2/3-Share erkennen (rekursiv unterhalb eines konfigurierbaren Startpfads).
- Pro neuer Datei genau ein Event auf ein Kafka-Topic publizieren.
- Robust gegen Neustarts, Verbindungsabbrüche und verlorene Change-Notifications.

## Nicht-Ziele

- Kein Lesen oder Übertragen von Dateiinhalten.
- Keine Events für Änderungen bestehender Dateien, Umbenennungen oder Löschungen (siehe „Mögliche Erweiterungen“).
- Kein Mount des Shares, kein Kafka Connect, kein NiFi.

## Technologie-Entscheidungen

| Bereich | Entscheidung | Begründung |
|---|---|---|
| Sprache | Rust (stable), async mit `tokio` | Kleines Binary, geringer Footprint, gut als Container |
| SMB | Crate `smb2` (reines Rust, SMB2/3, NTLM + Kerberos, Watcher für CHANGE_NOTIFY) | Kein Mount, keine C-Abhängigkeit, Push-Benachrichtigungen möglich |
| Kafka | Crate `rdkafka` (librdkafka) | Ausgereift, Producer + Consumer |
| Zustand | Compacted Kafka-Topic | Kein persistentes Volume, keine externe DB nötig |
| Config | Umgebungsvariablen (optional zusätzlich TOML-Datei) | Container-/k3s-freundlich |
| Logging | `tracing` + `tracing-subscriber`, JSON-Ausgabe optional | |

**Hinweis:** `smb2` ist noch 0.x. Die API (insbesondere `Watcher`, `list_directory`, Kerberos-Konfiguration) vor der Implementierung in der aktuellen Doku auf docs.rs prüfen und nicht aus dem Gedächtnis annehmen.

## Architektur

### Erkennung: Watcher + periodischer Abgleich

1. **Primär: SMB2 CHANGE_NOTIFY** über den `Watcher` von `smb2`, rekursiv auf dem Startverzeichnis. Liefert Push-Events vom Server.
2. **Sicherheitsnetz: periodischer Abgleich** (Reconciliation) per rekursivem Listing, z. B. alle 5 Minuten (konfigurierbar). Notwendig, weil CHANGE_NOTIFY
   - bei vielen Änderungen Events verlieren kann (Buffer-Overflow, `STATUS_NOTIFY_ENUM_DIR`),
   - nach Verbindungsabbruch keine verpassten Änderungen nachliefert.
3. Beim Start und nach jedem Reconnect sofort einen vollständigen Abgleich durchführen.
4. Fallback-Modus: Wenn der Watcher vom Server nicht unterstützt wird oder dauerhaft fehlschlägt, nur mit Polling-Abgleich weiterlaufen (Intervall konfigurierbar, dann kürzer, z. B. 30 s).

Watcher-Events werden nur als **Hinweis** behandelt („hier könnte etwas Neues sein“). Die Entscheidung, ob eine Datei neu ist, trifft immer der Abgleich mit dem Zustand (s. u.).

### Stabilitätsprüfung (halb geschriebene Dateien)

Eine Datei gilt erst als „fertig“, wenn **Größe und mtime über zwei Prüfungen im Abstand von N Sekunden** (konfigurierbar, Default 10 s) unverändert sind. Kandidaten werden bis dahin in einer In-Memory-Pending-Liste gehalten.

Optional konfigurierbar: Ignorier-Muster für temporäre Dateien (z. B. `*.tmp`, `~$*`, `.~lock*`, `Thumbs.db`).

### Zustand: compacted Topic

- Topic: `<prefix>.state` mit `cleanup.policy=compact`.
- Key: normalisierter Pfad relativ zum Share-Root (z. B. `\\server\share\` entfernen, Trennzeichen vereinheitlichen auf `/`, Case-Handling: siehe offene Fragen).
- Value: JSON `{ "size": u64, "mtime": "RFC3339", "first_seen": "RFC3339" }`.
- **Beim Start:** Topic von Anfang bis zum aktuellen High-Watermark aller Partitionen einlesen, in eine `HashMap<String, FileState>` laden, dann Consumer schließen.
- **Laufzeit:** Für jede als neu und stabil erkannte Datei zuerst das Event, dann den State-Eintrag schreiben.

### Events

- Topic: `<prefix>.events` (normale Retention, **nicht** compacted).
- Key: normalisierter Pfad (gleiche Normalisierung wie State).
- Value (JSON):

```json
{
  "event": "file_created",
  "share": "\\\\server\\share",
  "path": "unterordner/datei.pdf",
  "size": 123456,
  "mtime": "2026-11-11T10:15:00Z",
  "detected_at": "2026-11-11T10:15:12Z",
  "source": "watcher | reconcile"
}
```

### Zustellsemantik

- At-least-once. Producer mit `enable.idempotence=true`, `acks=all`.
- Reihenfolge pro Datei: Event senden und bestätigen lassen, danach State schreiben. Absturz dazwischen führt höchstens zu einem doppelten Event, nie zu einem verlorenen. Consumer sollen anhand des Keys idempotent sein; das in der README dokumentieren.
- Erster Start mit leerem State: konfigurierbar, ob alle vorhandenen Dateien als Events gemeldet werden (`INITIAL_SCAN_EMITS_EVENTS=false` als Default: dann nur State befüllen, keine Events).

## Konfiguration (Umgebungsvariablen)

| Variable | Beschreibung | Default |
|---|---|---|
| `SMB_SERVER` | Host[:Port] | – (Pflicht) |
| `SMB_SHARE` | Share-Name | – (Pflicht) |
| `SMB_BASE_PATH` | Startverzeichnis im Share | `""` (Root) |
| `SMB_AUTH` | `ntlm` oder `kerberos` | `ntlm` |
| `SMB_USER`, `SMB_PASSWORD`, `SMB_DOMAIN` | Für NTLM | – |
| Kerberos-Parameter | Je nach `smb2`-API (Keytab/Realm/KDC) | – |
| `KAFKA_BOOTSTRAP_SERVERS` | | – (Pflicht) |
| `KAFKA_TOPIC_PREFIX` | Ergibt `<prefix>.events` und `<prefix>.state` | `smb-watcher` |
| Kafka-Security (SASL/SSL) | Durchreichen an rdkafka, z. B. per `KAFKA_` → librdkafka-Property-Mapping | – |
| `RECONCILE_INTERVAL_SECS` | Abgleichsintervall | `300` |
| `STABILITY_DELAY_SECS` | Wartezeit Stabilitätsprüfung | `10` |
| `USE_WATCHER` | CHANGE_NOTIFY nutzen | `true` |
| `IGNORE_PATTERNS` | Kommagetrennte Globs | `*.tmp,~$*,.~lock*,Thumbs.db,desktop.ini` |
| `INITIAL_SCAN_EMITS_EVENTS` | Siehe oben | `false` |
| `LOG_FORMAT` | `pretty` oder `json` | `pretty` |

Topics beim Start prüfen. Optional automatisch anlegen (`state` mit `cleanup.policy=compact`), wenn `KAFKA_CREATE_TOPICS=true`.

## Betrieb

- Graceful Shutdown bei SIGTERM: Producer flushen, Watcher schließen.
- Reconnect mit exponentiellem Backoff bei SMB- und Kafka-Fehlern; nach SMB-Reconnect vollständiger Abgleich.
- Health: einfacher HTTP-Endpoint (`/healthz`, `/readyz`) auf konfigurierbarem Port; ready erst nach Laden des States.
- Metriken (optional, Prometheus): erkannte Dateien, gesendete Events, Dauer des Abgleichs, Anzahl Pending, Watcher-Overflows.
- Dockerfile (multi-stage, schlankes Runtime-Image) und ein Beispiel-Manifest für k3s/Kubernetes (Deployment mit 1 Replica, Secret für Credentials).

## Tests

- Unit-Tests: Pfad-Normalisierung, Ignore-Patterns, Stabilitätslogik, Diff State vs. Listing.
- Integrationstest mit Docker-Compose: Samba-Container + Kafka (KRaft, ein Broker). Szenarien: neue Datei, große Datei im Schreibvorgang, Neustart des Dienstes, Neustart von Samba, viele Dateien auf einmal (Overflow-Pfad).
- `smb2` bringt laut Doku Docker-basierte Test-Server mit; prüfen, ob die nutzbar sind.

## Offene Fragen (vor bzw. während der Umsetzung klären)

1. Zielserver: Windows-Fileserver, Samba oder NetApp? DFS im Spiel?
2. Authentifizierung: NTLM mit Service-Account oder Kerberos (Keytab)?
3. Pfade case-insensitiv behandeln (Windows-Semantik) oder exakt?
4. Größenordnung: wie viele Dateien insgesamt im überwachten Baum, wie viele neue pro Tag? (Relevant für Abgleichsintervall und Speicherbedarf des States.)
5. Kafka-Security (SASL/SSL) im Zielcluster?

## Mögliche Erweiterungen (später)

- Events für geänderte Dateien (`file_modified`, wenn size/mtime abweichen).
- Events für gelöschte Dateien (`file_deleted`) inkl. Tombstone im State-Topic.
- Mehrere Shares in einer Instanz.

## Vorgehen

1. Projektgerüst (`cargo new`), Dependencies, Config-Parsing.
2. SMB-Verbindung + rekursives Listing gegen lokalen Samba-Container.
3. State-Topic laden/schreiben, Diff-Logik, Events senden (zunächst nur Polling).
4. Stabilitätsprüfung.
5. Watcher (CHANGE_NOTIFY) ergänzen, inkl. Overflow-Handling.
6. Reconnect, Shutdown, Health.
7. Integrationstests, Dockerfile, README.
