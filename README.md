# smb-kafka-watcher

Kleiner Rust-Dienst, der ein SMB2/3-Share überwacht und für jede **neu hinzugekommene Datei** genau ein
`file_created`-Event auf Kafka schreibt. Dateiinhalte werden nicht gelesen. Spezifikation: [SPEC.md](SPEC.md).

## Funktionsweise

```
            CHANGE_NOTIFY (Hinweise)          ┌──────────────┐  Event (ack)  ┌──────────────────────┐
 SMB-Share ─────────────────────────────────▶ │              │ ────────────▶ │ <prefix>.events      │
     │      rekursives Listing (Abgleich)     │  Pending +   │  danach State │                      │
     └──────────────────────────────────────▶ │  Stabilität  │ ────────────▶ │ <prefix>.state       │
                                              └──────────────┘               │ (compacted)          │
                                                     ▲   beim Start einlesen └──────────────────────┘
                                                     └───────────────────────────────────┘
```

1. **Start:** Topics prüfen (optional anlegen), State-Topic bis zum High-Watermark in eine `HashMap` laden,
   dann `/readyz` = 200.
2. **Erkennung:** CHANGE_NOTIFY-Watcher (rekursiv) liefert Hinweise. Zusätzlich vollständiger Abgleich per rekursivem
   Listing beim Start, nach jedem Reconnect, nach einem Watcher-Overflow (`STATUS_NOTIFY_ENUM_DIR`) und alle
   `RECONCILE_INTERVAL_SECS`. Ob eine Datei neu ist, entscheidet immer der Vergleich mit dem State.
3. **Stabilität:** Ein Kandidat wird erst gemeldet, wenn Größe und mtime bei zwei Beobachtungen im Abstand von
   mindestens `STABILITY_DELAY_SECS` gleich sind. Bis dahin liegt er in einer In-Memory-Pending-Liste.
4. **Senden:** Event senden und Broker-Ack abwarten, danach State-Eintrag schreiben.
5. **Fallback:** Lässt sich der Watcher nicht einrichten (`Unsupported`) oder schlägt er `WATCHER_MAX_FAILURES`-mal in
   Folge fehl, läuft der Dienst nur noch mit Polling im Abstand von `POLL_INTERVAL_SECS`.

Junctions und Symlinks werden beim Listing nicht verfolgt. Taucht per Watcher ein neues Verzeichnis auf (z. B. ein
hineinkopierter Ordner), wird es sofort rekursiv gelistet.

## Zustellsemantik: at-least-once

- Producer mit `enable.idempotence=true` und `acks=all`.
- Pro Datei wird zuerst das Event bestätigt, erst danach der State geschrieben. Ein Absturz dazwischen führt nach dem
  Neustart zu **einem doppelten Event**, nie zu einem verlorenen.
- **Consumer müssen idempotent sein.** Kafka-Key ist der normalisierte Pfad. Empfehlung: `(key, mtime, size)` als
  Dedup-Schlüssel verwenden, oder die Verarbeitung so gestalten, dass eine Wiederholung unschädlich ist.
- Nur **eine Instanz** pro Share und Topic-Präfix betreiben (Deployment mit `replicas: 1`, `strategy: Recreate`).

## Events

Topic `<prefix>.events`, Key = normalisierter Pfad, Value:

```json
{
  "event": "file_created",
  "share": "\\\\server\\share",
  "path": "unterordner/datei.pdf",
  "size": 123456,
  "mtime": "2026-11-11T10:15:00Z",
  "detected_at": "2026-11-11T10:15:12Z",
  "source": "watcher"
}
```

`source` ist `watcher` oder `reconcile`, je nachdem, wodurch die Datei zuerst bemerkt wurde. `mtime` hat bei Bedarf
Sekundenbruchteile (`…:00.5Z`).

State-Topic `<prefix>.state` (compacted), gleicher Key, Value
`{"size": u64, "mtime": "RFC3339", "first_seen": "RFC3339"}`. Ein Tombstone (Value `null`) entfernt einen Eintrag.
Hinweis: ein Tombstone führt dazu, dass eine noch vorhandene Datei beim nächsten Abgleich erneut gemeldet wird.

**Pfad-Normalisierung:** relativ zum Share-Root (`SMB_BASE_PATH` ist Teil des Pfads), Trenner `/`, ohne führenden und
abschließenden `/`. Mit `PATH_CASE_INSENSITIVE=true` wird der **Key** kleingeschrieben (Windows-Semantik); `path` im
Event behält die Originalschreibweise.

## Konfiguration

Alle Werte per Umgebungsvariable. Optional zusätzlich eine TOML-Datei über `CONFIG_FILE` mit denselben Schlüsseln
(`SMB_SERVER = "fs01"`, Listen als Array). Umgebungsvariablen haben Vorrang.

| Variable | Beschreibung | Default |
|---|---|---|
| `SMB_SERVER` | Host[:Port] | Pflicht |
| `SMB_SHARE` | Share-Name | Pflicht |
| `SMB_BASE_PATH` | Startverzeichnis im Share | `""` (Root) |
| `SMB_AUTH` | `ntlm` oder `kerberos` | `ntlm` |
| `SMB_USER`, `SMB_PASSWORD`, `SMB_DOMAIN` | Anmeldedaten (bei Kerberos: Principal ohne Realm + Passwort) | – |
| `SMB_KRB_REALM` | Kerberos-Realm, z. B. `EXAMPLE.ORG` | Pflicht bei Kerberos |
| `SMB_KRB_KDC` | KDC `host[:port]` | Pflicht bei Kerberos mit Passwort |
| `SMB_KRB_CCACHE` | Pfad zu einem MIT-Credential-Cache (statt Passwort, z. B. von `kinit -k -t keytab`) | – |
| `SMB_KRB_HOSTNAME` | Hostname für den SPN `cifs/<host>`, falls `SMB_SERVER` eine IP ist | Host aus `SMB_SERVER` |
| `SMB_TIMEOUT_SECS` | TCP-Connect-Timeout | `10` |
| `SMB_REQUIRE_ENCRYPTION` | Verbindung nur mit SMB3-Verschlüsselung | `false` |
| `PATH_CASE_INSENSITIVE` | Keys kleinschreiben | `false` |
| `KAFKA_BOOTSTRAP_SERVERS` | | Pflicht |
| `KAFKA_TOPIC_PREFIX` | ergibt `<prefix>.events` und `<prefix>.state` | `smb-watcher` |
| `KAFKA_CREATE_TOPICS` | fehlende Topics anlegen (`state` mit `cleanup.policy=compact`) | `false` |
| `KAFKA_TOPIC_PARTITIONS`, `KAFKA_TOPIC_REPLICATION` | für das Anlegen | `1`, `1` |
| `KAFKA_STATE_LOAD_TIMEOUT_SECS` | maximale Dauer für das Einlesen des States | `120` |
| `KAFKA_PROP_*` | Durchreichen an librdkafka: `KAFKA_PROP_SECURITY_PROTOCOL=SASL_SSL` → `security.protocol` | – |
| `RECONCILE_INTERVAL_SECS` | Abgleichsintervall mit aktivem Watcher | `300` |
| `POLL_INTERVAL_SECS` | Abgleichsintervall ohne Watcher (Fallback / `USE_WATCHER=false`) | `30` |
| `STABILITY_DELAY_SECS` | Wartezeit der Stabilitätsprüfung | `10` |
| `USE_WATCHER` | CHANGE_NOTIFY nutzen | `true` |
| `WATCHER_MAX_FAILURES` | Fehlschläge in Folge bis zum Polling-Fallback | `5` |
| `IGNORE_PATTERNS` | kommagetrennte Globs (Dateiname, case-insensitiv; mit `/` gegen den ganzen Pfad) | `*.tmp,~$*,.~lock*,Thumbs.db,desktop.ini` |
| `INITIAL_SCAN_EMITS_EVENTS` | bei leerem State vorhandene Dateien melden (sonst nur State füllen) | `false` |
| `LOG_FORMAT` | `pretty` oder `json` | `pretty` (Image: `json`) |
| `RUST_LOG` | Log-Filter | `info,smb2=warn,rdkafka=info` |
| `HTTP_PORT` | Port für `/healthz`, `/readyz`, `/metrics` | `8080` |
| `BACKOFF_MAX_SECS` | Obergrenze des exponentiellen Backoffs | `300` |

`KAFKA_PROP_` statt einfach `KAFKA_`: Kubernetes setzt für einen Service namens `kafka` automatisch `KAFKA_PORT`,
`KAFKA_SERVICE_HOST` usw., die sonst bei librdkafka landen würden. `enable.idempotence`, `acks` und
`bootstrap.servers` lassen sich nicht überschreiben.

Beispiel SASL/SSL:

```bash
KAFKA_PROP_SECURITY_PROTOCOL=SASL_SSL
KAFKA_PROP_SASL_MECHANISMS=SCRAM-SHA-512
KAFKA_PROP_SASL_USERNAME=smb-watcher
KAFKA_PROP_SASL_PASSWORD=...
KAFKA_PROP_SSL_CA_LOCATION=/etc/kafka-ca/ca.crt
```

## Betrieb

- **SIGTERM/SIGINT:** Watcher schließen, Tree trennen, Producer flushen (max. 10 s).
- **Fehler:** SMB-Verbindungsfehler führen zu Reconnect mit exponentiellem Backoff (1 s … `BACKOFF_MAX_SECS`) und
  danach zu einem vollständigen Abgleich. Fehlgeschlagene Kafka-Sends bleiben in der Pending-Liste und werden beim
  nächsten Stabilitäts-Tick erneut versucht. Ist Kafka beim Start nicht erreichbar, wird mit Backoff wiederholt.
- **Health:** `/healthz` (Prozess läuft), `/readyz` (200 erst nach Laden des States).
- **Metriken** (`/metrics`, Prometheus, Präfix `smb_watcher_`): `files_detected_total`, `events_sent_total`,
  `kafka_errors_total`, `reconcile_duration_seconds`, `pending_files`, `state_files`, `watcher_overflows_total`,
  `watcher_active`, `smb_reconnects_total`.
- **Speicher:** State liegt komplett im RAM (grob 150–250 Byte pro bekannter Datei plus Pfadlänge).

## Bauen und starten

```bash
cargo build --release
SMB_SERVER=fs01 SMB_SHARE=scans SMB_USER=svc SMB_PASSWORD=... SMB_DOMAIN=EXAMPLE \
KAFKA_BOOTSTRAP_SERVERS=kafka:9092 ./target/release/smb-kafka-watcher

docker build -t smb-kafka-watcher .
```

Das Image ist multi-stage (Builder `rust:1.95-bookworm`, Runtime `distroless/cc-debian12:nonroot`); librdkafka und
OpenSSL sind statisch gelinkt. Ein Beispiel-Manifest für k3s/Kubernetes (Deployment mit 1 Replica, ConfigMap, Secret,
Probes) liegt in [deploy/k8s.yaml](deploy/k8s.yaml).

## Tests

```bash
cargo test                      # Unit-Tests: Pfade, Ignore-Patterns, Stabilität, Diff, Config, JSON-Format
tests/integration/run.sh        # Docker Compose: Samba + Kafka (KRaft), Dienst läuft auf dem Host
KEEP_ENV=1 tests/integration/run.sh   # Container danach stehen lassen
```

Szenarien des Integrationstests: Baseline (keine Events für vorhandene Dateien), neue Datei (via Watcher), große Datei
im Schreibvorgang (genau ein Event mit Endgröße), Ignore-Patterns, Neustart des Dienstes (Datei während Downtime → via
Abgleich, keine Duplikate), Neustart von Samba (Reconnect), 3000 Dateien auf einmal, Polling-Fallback
(`USE_WATCHER=false`). Die Ports binden nur an `127.0.0.1` (Samba 14450, Kafka 19092, HTTP 18080).

Hinweis zum Overflow-Pfad: Samba lieferte im Test auch bei 3000 Dateien alle Notifications ohne
`STATUS_NOTIFY_ENUM_DIR`; der Overflow-Zweig (Abgleich 2 s nach dem Overflow) ist daher nur durch Code-Review, nicht
durch den Test abgedeckt. Gegen einen Windows-Fileserver ist er realistischer.

Die Docker-Testserver aus `smb2` (Feature `testing`) wurden geprüft: Sie sind auf das Testen des Clients
ausgelegt (16 feste Container, Dateien nur im Image), daher nutzt der Integrationstest einen eigenen, schlanken
Samba-Container nach demselben Muster.

## Hinweise zur `smb2`-API (Version 0.43)

- Kerberos gibt es nur über die Low-Level-API (`Session::setup_kerberos`, `setup_kerberos_from_ccache`);
  `SmbClient::connect` kann nur NTLM. Der Dienst verwendet deshalb für beide Verfahren `Connection` + `Session` +
  `Tree` und baut die Verbindung nach Fehlern selbst neu auf (mit `previous_session_id`, damit der Server alte Handles
  sofort schließt).
- Folge daraus: **DFS-Referrals werden nicht verfolgt.** `SMB_SERVER`/`SMB_SHARE` müssen auf das tatsächliche Ziel
  zeigen, nicht auf einen DFS-Namespace.
- Ein Keytab wird nicht direkt gelesen. Für Kerberos ohne Passwort per Sidecar/Init-Container
  `kinit -k -t <keytab>` in einen Credential-Cache schreiben und `SMB_KRB_CCACHE` darauf zeigen lassen. Der Cache
  wird bei jedem (Re-)Connect neu gelesen, ein Sidecar kann ihn also erneuern.
- Watcher-Pfade kommen bereits `/`-getrennt und dekodiert; leere Antworten sind normal (Abo-Erneuerung alle 10 min)
  und werden nicht als Overflow gewertet.

## Offene Fragen aus der Spezifikation

Noch nicht geklärt; der Code lässt die Entscheidung per Konfiguration offen:

1. Zielserver (Windows/Samba/NetApp, DFS?): DFS wird nicht unterstützt (siehe oben).
2. NTLM oder Kerberos: beides implementiert; Kerberos nicht gegen ein echtes AD getestet.
3. Case-Handling: `PATH_CASE_INSENSITIVE`, Default exakt.
4. Mengengerüst: bestimmt `RECONCILE_INTERVAL_SECS` und den RAM-Bedarf (siehe Betrieb).
5. Kafka-Security: über `KAFKA_PROP_*`.
