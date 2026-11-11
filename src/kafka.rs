//! Kafka: topic checks, state loading and the event/state producer.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::state::{FileEvent, FileState, StateMap};

const METADATA_TIMEOUT: Duration = Duration::from_secs(15);
const SEND_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

fn base_config(cfg: &Config) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.kafka_bootstrap_servers);
    for (k, v) in &cfg.kafka_props {
        c.set(k, v);
    }
    c
}

/// Check that both topics exist; create them if `KAFKA_CREATE_TOPICS=true`.
pub async fn ensure_topics(cfg: &Config) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = base_config(cfg)
        .create()
        .context("creating Kafka admin client")?;
    let events = cfg.events_topic();
    let state = cfg.state_topic();

    let existing = {
        let md = admin
            .inner()
            .fetch_metadata(None, METADATA_TIMEOUT)
            .context("fetching Kafka metadata")?;
        md.topics()
            .iter()
            .filter(|t| t.error().is_none())
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>()
    };
    let missing: Vec<&String> = [&events, &state]
        .into_iter()
        .filter(|t| !existing.contains(t))
        .collect();

    if !missing.is_empty() {
        if !cfg.kafka_create_topics {
            bail!(
                "Kafka topic(s) {missing:?} do not exist; create them or set KAFKA_CREATE_TOPICS=true"
            );
        }
        let repl = || TopicReplication::Fixed(cfg.kafka_topic_replication);
        let mut new_topics = Vec::new();
        if missing.contains(&&events) {
            new_topics.push(NewTopic::new(&events, cfg.kafka_topic_partitions, repl()));
        }
        if missing.contains(&&state) {
            new_topics.push(
                NewTopic::new(&state, cfg.kafka_topic_partitions, repl())
                    .set("cleanup.policy", "compact"),
            );
        }
        let opts = AdminOptions::new().operation_timeout(Some(METADATA_TIMEOUT));
        for r in admin.create_topics(&new_topics, &opts).await? {
            match r {
                Ok(t) => info!(topic = %t, "created Kafka topic"),
                Err((t, RDKafkaErrorCode::TopicAlreadyExists)) => {
                    debug!(topic = %t, "topic already exists")
                }
                Err((t, code)) => bail!("creating topic {t}: {code}"),
            }
        }
    }

    // The state topic must be compacted, otherwise retention eventually
    // forgets known files and they would be reported again. Only checked for a
    // pre-existing topic: a just-created one may not report its config yet.
    if !missing.contains(&&state) {
        let opts = AdminOptions::new().request_timeout(Some(METADATA_TIMEOUT));
        match admin
            .describe_configs(&[ResourceSpecifier::Topic(&state)], &opts)
            .await
        {
            Ok(results) => {
                for r in results.into_iter().flatten() {
                    let policy = r.get("cleanup.policy").and_then(|e| e.value.clone());
                    if !policy.as_deref().unwrap_or("").contains("compact") {
                        warn!(topic = %state, ?policy, "state topic is not compacted (cleanup.policy should be compact)");
                    }
                }
            }
            Err(e) => warn!(error = %e, "could not describe state topic config"),
        }
    }
    Ok(())
}

/// Read the state topic from the beginning up to the current high watermark.
pub async fn load_state(cfg: &Config) -> Result<StateMap> {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || load_state_blocking(&cfg)).await?
}

fn load_state_blocking(cfg: &Config) -> Result<StateMap> {
    let topic = cfg.state_topic();
    let consumer: BaseConsumer = base_config(cfg)
        .set(
            "group.id",
            format!("{}-state-loader", cfg.kafka_topic_prefix),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "false")
        .create()
        .context("creating state consumer")?;

    let md = consumer.fetch_metadata(Some(&topic), METADATA_TIMEOUT)?;
    let partitions: Vec<i32> = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("state topic {topic} not found"))?
        .partitions()
        .iter()
        .map(|p| p.id())
        .collect();

    // Target: high watermark per partition, fixed at start.
    let mut remaining: HashMap<i32, i64> = HashMap::new();
    let mut tpl = TopicPartitionList::new();
    for p in &partitions {
        let (low, high) = consumer.fetch_watermarks(&topic, *p, METADATA_TIMEOUT)?;
        if high > low {
            remaining.insert(*p, high);
            tpl.add_partition_offset(&topic, *p, Offset::Beginning)?;
        }
    }

    let mut state = StateMap::new();
    let mut records = 0u64;
    if remaining.is_empty() {
        info!(topic = %topic, "state topic is empty");
        return Ok(state);
    }
    consumer.assign(&tpl)?;

    let deadline = Instant::now() + cfg.kafka_state_load_timeout;
    while !remaining.is_empty() {
        if Instant::now() > deadline {
            bail!(
                "timed out loading state topic {topic} after {:?} ({records} records read)",
                cfg.kafka_state_load_timeout
            );
        }
        let Some(msg) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let msg = msg.context("reading state topic")?;
        records += 1;
        let Some(key) = msg.key().map(|k| String::from_utf8_lossy(k).into_owned()) else {
            continue;
        };
        match msg.payload() {
            None => {
                // Tombstone
                state.remove(&key);
            }
            Some(p) => match serde_json::from_slice::<FileState>(p) {
                Ok(fs) => {
                    state.insert(key, fs);
                }
                Err(e) => warn!(%key, error = %e, "skipping unparsable state record"),
            },
        }
        let part = msg.partition();
        if remaining
            .get(&part)
            .is_some_and(|high| msg.offset() + 1 >= *high)
        {
            remaining.remove(&part);
        }
    }
    info!(topic = %topic, records, files = state.len(), "state loaded");
    Ok(state)
}

#[derive(Clone)]
pub struct Sink {
    producer: FutureProducer,
    events_topic: String,
    state_topic: String,
}

impl Sink {
    pub fn new(cfg: &Config) -> Result<Self> {
        let producer: FutureProducer = base_config(cfg)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .context("creating Kafka producer")?;
        Ok(Self {
            producer,
            events_topic: cfg.events_topic(),
            state_topic: cfg.state_topic(),
        })
    }

    async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), KafkaError> {
        let record = FutureRecord::to(topic).key(key).payload(payload);
        self.producer
            .send(record, SEND_QUEUE_TIMEOUT)
            .await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }

    /// Send the event and wait for the broker's acknowledgement.
    pub async fn send_event(&self, key: &str, ev: &FileEvent) -> Result<(), KafkaError> {
        let payload = serde_json::to_vec(ev).expect("event serialises");
        self.send(&self.events_topic, key, &payload).await
    }

    pub async fn send_state(&self, key: &str, st: &FileState) -> Result<(), KafkaError> {
        let payload = serde_json::to_vec(st).expect("state serialises");
        self.send(&self.state_topic, key, &payload).await
    }

    pub fn flush(&self, timeout: Duration) {
        if let Err(e) = self.producer.flush(timeout) {
            warn!(error = %e, "Kafka producer flush failed");
        }
    }
}
