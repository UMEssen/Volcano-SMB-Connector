//! Configuration from environment variables, optionally seeded from a TOML file.
//!
//! Precedence: environment variable > TOML file (`CONFIG_FILE`) > default.
//! The TOML file uses the same keys as the environment (e.g. `SMB_SERVER = "fs01"`).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmbAuth {
    Ntlm,
    Kerberos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Clone)]
pub struct Config {
    // SMB
    pub smb_server: String,
    pub smb_share: String,
    pub smb_base_path: String,
    pub smb_auth: SmbAuth,
    pub smb_user: String,
    pub smb_password: String,
    pub smb_domain: String,
    pub smb_krb_realm: String,
    pub smb_krb_kdc: String,
    pub smb_krb_ccache: Option<String>,
    pub smb_krb_hostname: Option<String>,
    pub smb_timeout: Duration,
    pub smb_require_encryption: bool,
    /// Pause between directory listings during a scan (load limiting).
    pub smb_list_delay: Duration,
    pub path_case_insensitive: bool,

    // Kafka
    pub kafka_bootstrap_servers: String,
    pub kafka_topic_prefix: String,
    pub kafka_create_topics: bool,
    pub kafka_topic_partitions: i32,
    pub kafka_topic_replication: i32,
    pub kafka_state_load_timeout: Duration,
    /// Extra librdkafka properties from `KAFKA_PROP_*` env vars (see [`kafka_props_from`]).
    pub kafka_props: Vec<(String, String)>,

    // Detection
    pub reconcile_interval: Duration,
    pub poll_interval: Duration,
    pub stability_delay: Duration,
    pub use_watcher: bool,
    pub watcher_max_failures: u32,
    pub ignore_patterns: Vec<String>,
    pub initial_scan_emits_events: bool,

    // Ops
    pub log_format: LogFormat,
    pub http_port: u16,
    pub backoff_max: Duration,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("smb_server", &self.smb_server)
            .field("smb_share", &self.smb_share)
            .field("smb_base_path", &self.smb_base_path)
            .field("smb_auth", &self.smb_auth)
            .field("smb_user", &self.smb_user)
            .field("smb_password", &"***")
            .field("smb_domain", &self.smb_domain)
            .field("smb_krb_realm", &self.smb_krb_realm)
            .field("smb_krb_kdc", &self.smb_krb_kdc)
            .field("smb_krb_ccache", &self.smb_krb_ccache)
            .field("path_case_insensitive", &self.path_case_insensitive)
            .field("kafka_bootstrap_servers", &self.kafka_bootstrap_servers)
            .field("kafka_topic_prefix", &self.kafka_topic_prefix)
            .field("kafka_create_topics", &self.kafka_create_topics)
            .field(
                "kafka_props",
                &self.kafka_props.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("reconcile_interval", &self.reconcile_interval)
            .field("poll_interval", &self.poll_interval)
            .field("stability_delay", &self.stability_delay)
            .field("use_watcher", &self.use_watcher)
            .field("ignore_patterns", &self.ignore_patterns)
            .field("initial_scan_emits_events", &self.initial_scan_emits_events)
            .field("http_port", &self.http_port)
            .finish_non_exhaustive()
    }
}

/// Prefix of variables passed through to librdkafka. A dedicated prefix
/// instead of plain `KAFKA_`, because Kubernetes injects `KAFKA_PORT`,
/// `KAFKA_SERVICE_HOST`, ... for any Service named `kafka`.
const KAFKA_PROP_PREFIX: &str = "KAFKA_PROP_";

/// Properties the service sets itself and that must not be overridden.
const PROTECTED_KAFKA_PROPS: &[&str] = &["enable.idempotence", "acks", "bootstrap.servers"];

impl Config {
    pub fn from_env() -> Result<Self> {
        let mut vars: HashMap<String, String> = HashMap::new();
        if let Ok(path) = std::env::var("CONFIG_FILE") {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading CONFIG_FILE {path}"))?;
            vars.extend(parse_toml_vars(&text).with_context(|| format!("parsing {path}"))?);
        }
        vars.extend(std::env::vars());
        Self::from_vars(&vars)
    }

    pub fn from_vars(vars: &HashMap<String, String>) -> Result<Self> {
        let get = |k: &str| {
            vars.get(k)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        let req = |k: &str| get(k).with_context(|| format!("{k} is required"));
        let or = |k: &str, d: &str| get(k).unwrap_or_else(|| d.to_string());
        let secs = |k: &str, d: u64| -> Result<Duration> {
            Ok(Duration::from_secs(match get(k) {
                Some(v) => v
                    .parse()
                    .with_context(|| format!("{k} must be a number of seconds"))?,
                None => d,
            }))
        };
        let flag = |k: &str, d: bool| -> Result<bool> {
            match get(k) {
                None => Ok(d),
                Some(v) => parse_bool(&v).with_context(|| format!("{k} must be true/false")),
            }
        };
        let int = |k: &str, d: i64| -> Result<i64> {
            match get(k) {
                None => Ok(d),
                Some(v) => v.parse().with_context(|| format!("{k} must be an integer")),
            }
        };

        let smb_auth = match or("SMB_AUTH", "ntlm").to_ascii_lowercase().as_str() {
            "ntlm" => SmbAuth::Ntlm,
            "kerberos" => SmbAuth::Kerberos,
            other => bail!("SMB_AUTH must be ntlm or kerberos, got {other:?}"),
        };
        let log_format = match or("LOG_FORMAT", "pretty").to_ascii_lowercase().as_str() {
            "pretty" => LogFormat::Pretty,
            "json" => LogFormat::Json,
            other => bail!("LOG_FORMAT must be pretty or json, got {other:?}"),
        };

        let cfg = Config {
            smb_server: req("SMB_SERVER")?,
            smb_share: req("SMB_SHARE")?,
            smb_base_path: get("SMB_BASE_PATH").unwrap_or_default(),
            smb_auth,
            smb_user: get("SMB_USER").unwrap_or_default(),
            // Passwords are not trimmed.
            smb_password: vars.get("SMB_PASSWORD").cloned().unwrap_or_default(),
            smb_domain: get("SMB_DOMAIN").unwrap_or_default(),
            smb_krb_realm: get("SMB_KRB_REALM").unwrap_or_default(),
            smb_krb_kdc: get("SMB_KRB_KDC").unwrap_or_default(),
            smb_krb_ccache: get("SMB_KRB_CCACHE"),
            smb_krb_hostname: get("SMB_KRB_HOSTNAME"),
            smb_timeout: secs("SMB_TIMEOUT_SECS", 10)?,
            smb_require_encryption: flag("SMB_REQUIRE_ENCRYPTION", false)?,
            smb_list_delay: Duration::from_millis(int("SMB_LIST_DELAY_MS", 0)?.max(0) as u64),
            path_case_insensitive: flag("PATH_CASE_INSENSITIVE", false)?,

            kafka_bootstrap_servers: req("KAFKA_BOOTSTRAP_SERVERS")?,
            kafka_topic_prefix: or("KAFKA_TOPIC_PREFIX", "smb-watcher"),
            kafka_create_topics: flag("KAFKA_CREATE_TOPICS", false)?,
            kafka_topic_partitions: int("KAFKA_TOPIC_PARTITIONS", 1)? as i32,
            kafka_topic_replication: int("KAFKA_TOPIC_REPLICATION", 1)? as i32,
            kafka_state_load_timeout: secs("KAFKA_STATE_LOAD_TIMEOUT_SECS", 120)?,
            kafka_props: kafka_props_from(vars)?,

            reconcile_interval: secs("RECONCILE_INTERVAL_SECS", 300)?,
            poll_interval: secs("POLL_INTERVAL_SECS", 30)?,
            stability_delay: secs("STABILITY_DELAY_SECS", 10)?,
            use_watcher: flag("USE_WATCHER", true)?,
            watcher_max_failures: int("WATCHER_MAX_FAILURES", 5)? as u32,
            ignore_patterns: or("IGNORE_PATTERNS", "*.tmp,~$*,.~lock*,Thumbs.db,desktop.ini")
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            initial_scan_emits_events: flag("INITIAL_SCAN_EMITS_EVENTS", false)?,

            log_format,
            http_port: int("HTTP_PORT", 8080)? as u16,
            backoff_max: secs("BACKOFF_MAX_SECS", 300)?,
        };

        if cfg.smb_auth == SmbAuth::Kerberos {
            if cfg.smb_krb_realm.is_empty() {
                bail!("SMB_KRB_REALM is required for SMB_AUTH=kerberos");
            }
            if cfg.smb_krb_ccache.is_none() && cfg.smb_password.is_empty() {
                bail!("SMB_AUTH=kerberos needs SMB_PASSWORD or SMB_KRB_CCACHE");
            }
            if cfg.smb_krb_ccache.is_none() && cfg.smb_krb_kdc.is_empty() {
                bail!("SMB_KRB_KDC is required for password-based Kerberos");
            }
        }
        if cfg.reconcile_interval.is_zero() || cfg.poll_interval.is_zero() {
            bail!("RECONCILE_INTERVAL_SECS and POLL_INTERVAL_SECS must be > 0");
        }
        Ok(cfg)
    }

    /// `host:port` for the SMB TCP connection (port 445 if none given).
    pub fn smb_addr(&self) -> String {
        if self.smb_server.contains(':') {
            self.smb_server.clone()
        } else {
            format!("{}:445", self.smb_server)
        }
    }

    /// Host name without port (used for Kerberos SPN `cifs/<host>` and the event's `share`).
    pub fn smb_host(&self) -> &str {
        self.smb_server
            .split(':')
            .next()
            .unwrap_or(&self.smb_server)
    }

    /// UNC form of the share, e.g. `\\server\share`.
    pub fn share_unc(&self) -> String {
        format!(r"\\{}\{}", self.smb_host(), self.smb_share)
    }

    pub fn events_topic(&self) -> String {
        format!("{}.events", self.kafka_topic_prefix)
    }

    pub fn state_topic(&self) -> String {
        format!("{}.state", self.kafka_topic_prefix)
    }
}

/// Maps `KAFKA_PROP_SECURITY_PROTOCOL=SASL_SSL` to `security.protocol=SASL_SSL`.
///
/// The rest after `KAFKA_PROP_` is lowercased and its `_` replaced by `.`.
/// librdkafka property names contain no underscores, so the mapping is
/// unambiguous.
pub fn kafka_props_from(vars: &HashMap<String, String>) -> Result<Vec<(String, String)>> {
    let mut props: Vec<(String, String)> = vars
        .iter()
        .filter(|(k, _)| k.len() > KAFKA_PROP_PREFIX.len() && k.starts_with(KAFKA_PROP_PREFIX))
        .map(|(k, v)| {
            (
                k[KAFKA_PROP_PREFIX.len()..]
                    .to_ascii_lowercase()
                    .replace('_', "."),
                v.clone(),
            )
        })
        .collect();
    if let Some((k, _)) = props
        .iter()
        .find(|(k, _)| PROTECTED_KAFKA_PROPS.contains(&k.as_str()))
    {
        bail!("Kafka property {k} is set by the service and cannot be overridden");
    }
    props.sort();
    Ok(props)
}

fn parse_bool(v: &str) -> Result<bool> {
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("invalid boolean {v:?}"),
    }
}

fn parse_toml_vars(text: &str) -> Result<HashMap<String, String>> {
    let table: toml::Table = text.parse()?;
    table
        .into_iter()
        .map(|(k, v)| {
            let s = match v {
                toml::Value::String(s) => s,
                toml::Value::Integer(i) => i.to_string(),
                toml::Value::Boolean(b) => b.to_string(),
                toml::Value::Array(a) => a
                    .into_iter()
                    .map(|x| match x {
                        toml::Value::String(s) => Ok(s),
                        other => bail!("{k}: array entries must be strings, got {other}"),
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(","),
                other => bail!("{k}: unsupported value {other}"),
            };
            Ok((k.to_ascii_uppercase(), s))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> HashMap<String, String> {
        [
            ("SMB_SERVER", "fs01"),
            ("SMB_SHARE", "scans"),
            ("KAFKA_BOOTSTRAP_SERVERS", "k:9092"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
    }

    #[test]
    fn defaults() {
        let c = Config::from_vars(&base()).unwrap();
        assert_eq!(c.smb_addr(), "fs01:445");
        assert_eq!(c.share_unc(), r"\\fs01\scans");
        assert_eq!(c.events_topic(), "smb-watcher.events");
        assert_eq!(c.state_topic(), "smb-watcher.state");
        assert_eq!(c.reconcile_interval, Duration::from_secs(300));
        assert_eq!(c.stability_delay, Duration::from_secs(10));
        assert!(c.use_watcher);
        assert!(!c.initial_scan_emits_events);
        assert_eq!(c.ignore_patterns.len(), 5);
        assert!(c.kafka_props.is_empty());
    }

    #[test]
    fn missing_required() {
        let mut v = base();
        v.remove("SMB_SHARE");
        assert!(Config::from_vars(&v).is_err());
    }

    #[test]
    fn kafka_passthrough() {
        let mut v = base();
        v.insert("KAFKA_PROP_SECURITY_PROTOCOL".into(), "SASL_SSL".into());
        v.insert("KAFKA_PROP_SASL_MECHANISMS".into(), "SCRAM-SHA-512".into());
        // Kubernetes service links must not leak into librdkafka.
        v.insert("KAFKA_PORT".into(), "tcp://10.0.0.1:9092".into());
        v.insert("KAFKA_SERVICE_HOST".into(), "10.0.0.1".into());
        let c = Config::from_vars(&v).unwrap();
        assert_eq!(
            c.kafka_props,
            vec![
                ("sasl.mechanisms".to_string(), "SCRAM-SHA-512".to_string()),
                ("security.protocol".to_string(), "SASL_SSL".to_string()),
            ]
        );
        v.insert("KAFKA_PROP_ACKS".into(), "1".into());
        assert!(Config::from_vars(&v).is_err());
    }

    #[test]
    fn kerberos_validation() {
        let mut v = base();
        v.insert("SMB_AUTH".into(), "kerberos".into());
        assert!(Config::from_vars(&v).is_err());
        v.insert("SMB_KRB_REALM".into(), "EXAMPLE.ORG".into());
        v.insert("SMB_KRB_CCACHE".into(), "/tmp/krb5cc".into());
        assert!(Config::from_vars(&v).is_ok());
    }

    #[test]
    fn toml_file() {
        let m = parse_toml_vars(
            "smb_server = \"fs02:1445\"\nRECONCILE_INTERVAL_SECS = 60\nIGNORE_PATTERNS = [\"*.tmp\", \"*.part\"]\n",
        )
        .unwrap();
        assert_eq!(m["SMB_SERVER"], "fs02:1445");
        assert_eq!(m["RECONCILE_INTERVAL_SECS"], "60");
        assert_eq!(m["IGNORE_PATTERNS"], "*.tmp,*.part");
    }
}
