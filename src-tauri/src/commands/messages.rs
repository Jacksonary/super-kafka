use crate::config;
use crate::kafka_client::build_client_config;
use crate::types::{
    FetchMessagesRequest, FetchMessagesResponse, KafkaMessage, MessageHeader,
    ProduceMessageRequest,
};
use crate::AppState;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::time::Duration;
use tauri::State;

const POLL_TIMEOUT: Duration = Duration::from_millis(500);

#[tauri::command]
pub async fn fetch_messages(
    state: State<'_, AppState>,
    req: FetchMessagesRequest,
) -> Result<FetchMessagesResponse, String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let password = config::load_sasl_password(&req.cluster_id).ok().flatten();
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let limit = req.limit.max(1).min(10_000);

    tokio::task::spawn_blocking(move || -> Result<FetchMessagesResponse, String> {
        let mut cfg = build_client_config(&cluster, password.as_deref());
        cfg.set(
            "group.id",
            format!("super-kafka-fetch-{}", uuid::Uuid::new_v4()),
        );
        cfg.set("enable.auto.commit", "false");

        let consumer: BaseConsumer = cfg
            .create()
            .map_err(|e| format!("[KAFKA-CONSUMER] create: {e}"))?;

        let meta = consumer
            .fetch_metadata(Some(&req.topic), timeout)
            .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;
        let topic_meta = meta
            .topics()
            .iter()
            .find(|t| t.name() == req.topic)
            .ok_or_else(|| format!("[KAFKA] topic `{}` not found", req.topic))?;

        let target_partitions: Vec<i32> = match req.partition {
            Some(p) => vec![p],
            None => topic_meta.partitions().iter().map(|p| p.id()).collect(),
        };

        // Collect watermarks and starting offsets per partition.
        // We use high watermarks to detect EOF instead of enable.partition.eof,
        // which is incompatible with assign() (non-subscribe) mode on some brokers.
        // Only track partitions that actually have messages (high > start offset).
        let mut partition_high: std::collections::HashMap<i32, i64> =
            std::collections::HashMap::new();
        let mut tpl = TopicPartitionList::new();
        for &p in &target_partitions {
            let (low, high) = consumer
                .fetch_watermarks(&req.topic, p, timeout)
                .unwrap_or((0, 0));
            let offset = decide_offset(&req.fetch_mode, low, high, limit, target_partitions.len() as i64)?;
            tpl.add_partition_offset(&req.topic, p, offset)
                .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            // Skip empty partitions — nothing to consume there
            let start = match offset {
                Offset::Beginning => low,
                Offset::Offset(o) => o,
                _ => low,
            };
            if high > start {
                partition_high.insert(p, high);
            }
        }
        consumer
            .assign(&tpl)
            .map_err(|e| format!("[KAFKA-CONSUMER] assign: {e}"))?;

        // If all partitions are empty, return immediately
        if partition_high.is_empty() {
            return Ok(FetchMessagesResponse {
                messages: vec![],
                total_fetched: 0,
                has_more: false,
            });
        }

        let mut messages: Vec<KafkaMessage> = Vec::with_capacity(limit as usize);
        let started = std::time::Instant::now();
        let max_wait = Duration::from_secs(30);
        let mut consecutive_timeouts = 0usize;

        while messages.len() < limit as usize {
            if started.elapsed() > max_wait {
                break;
            }
            match consumer.poll(POLL_TIMEOUT) {
                Some(Ok(m)) => {
                    consecutive_timeouts = 0;
                    let next_offset = m.offset() + 1;
                    let high = partition_high.get(&m.partition()).copied().unwrap_or(0);
                    let kafka_msg = borrowed_to_kafka_message(&m);
                    messages.push(kafka_msg);
                    // Stop tracking this partition once we've consumed up to its high watermark
                    if next_offset >= high {
                        partition_high.remove(&m.partition());
                        if partition_high.is_empty() {
                            break;
                        }
                    }
                }
                Some(Err(e)) => {
                    return Err(format!("[KAFKA-CONSUMER] poll: {e}"));
                }
                None => {
                    // poll() timed out — could be a slow broker between batches.
                    // Allow a few consecutive timeouts before treating it as EOF.
                    consecutive_timeouts += 1;
                    if consecutive_timeouts >= 3 {
                        break;
                    }
                }
            }
        }

        let total = messages.len() as i32;
        let has_more = total >= limit;

        Ok(FetchMessagesResponse {
            messages,
            total_fetched: total,
            has_more,
        })
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

fn decide_offset(
    fetch_mode: &Value,
    low: i64,
    high: i64,
    limit: i32,
    partition_count: i64,
) -> Result<Offset, String> {
    let mode = fetch_mode
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("latest");
    match mode {
        "earliest" | "from_beginning" => Ok(Offset::Beginning),
        "latest" => {
            // We want the last N messages: rewind by limit/partitions per partition.
            let per_part = (limit as i64 / partition_count.max(1)).max(1);
            let target = (high - per_part).max(low);
            Ok(Offset::Offset(target))
        }
        "from_offset" => {
            let off = fetch_mode
                .get("offset")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "[KAFKA] from_offset requires `offset` field".to_string())?;
            Ok(Offset::Offset(off))
        }
        "from_timestamp" => {
            // Caller should pass a millisecond timestamp; rdkafka offsets_for_times needs special call.
            // Fallback: start from beginning. Frontend can refine later.
            let _ts = fetch_mode.get("timestamp").and_then(|v| v.as_i64());
            Ok(Offset::Beginning)
        }
        _ => Ok(Offset::Beginning),
    }
}

fn borrowed_to_kafka_message(m: &rdkafka::message::BorrowedMessage<'_>) -> KafkaMessage {
    let key_raw: Option<Vec<u8>> = m.key().map(|k| k.to_vec());
    let key_text = key_raw
        .as_ref()
        .and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()));
    let value_raw: Vec<u8> = m.payload().map(|p| p.to_vec()).unwrap_or_default();
    let (value_text, value_encoding) = decode_payload(&value_raw);

    let (timestamp, timestamp_type) = match m.timestamp() {
        Timestamp::CreateTime(ts) => (Some(ts), Some("CreateTime".to_string())),
        Timestamp::LogAppendTime(ts) => (Some(ts), Some("LogAppendTime".to_string())),
        Timestamp::NotAvailable => (None, None),
    };

    let headers: Vec<MessageHeader> = m
        .headers()
        .map(|hs| {
            (0..hs.count())
                .map(|i| {
                    let h = hs.get(i);
                    MessageHeader {
                        key: h.key.to_string(),
                        value: h
                            .value
                            .and_then(|v| std::str::from_utf8(v).ok().map(|s| s.to_string())),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    KafkaMessage {
        partition: m.partition(),
        offset: m.offset(),
        timestamp,
        timestamp_type,
        key_raw,
        key_text,
        value_raw,
        value_text,
        value_encoding,
        headers,
        compression_codec: "unknown".to_string(),
    }
}

fn decode_payload(bytes: &[u8]) -> (Option<String>, String) {
    if bytes.is_empty() {
        return (Some(String::new()), "utf8".to_string());
    }
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            if serde_json::from_str::<Value>(s).is_ok() {
                (Some(s.to_string()), "json".to_string())
            } else {
                (Some(s.to_string()), "utf8".to_string())
            }
        }
        Err(_) => (None, "binary".to_string()),
    }
}

#[tauri::command]
pub async fn produce_message(
    state: State<'_, AppState>,
    req: ProduceMessageRequest,
) -> Result<Value, String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();

    tokio::task::spawn_blocking(move || -> Result<Value, String> {
        let bundle = pool.get_or_create(&req.cluster_id)?;
        let producer: &BaseProducer<_> = &bundle.producer;
        let mut headers = OwnedHeaders::new();
        for h in &req.headers {
            headers = headers.insert(Header {
                key: &h.key,
                value: h.value.as_deref().map(|s| s.as_bytes()),
            });
        }
        let key_owned = req.key.clone();
        let value_bytes = req.value.as_bytes();

        let mut record: BaseRecord<String, [u8]> =
            BaseRecord::to(&req.topic).payload(value_bytes);
        if let Some(p) = req.partition {
            record = record.partition(p);
        }
        if let Some(ref k) = key_owned {
            record = record.key(k);
        }
        record = record.headers(headers);

        producer
            .send(record)
            .map_err(|(e, _)| format!("[KAFKA-PRODUCE] send: {e}"))?;
        producer
            .flush(timeout)
            .map_err(|e| format!("[KAFKA-PRODUCE] flush: {e}"))?;
        Ok(json!({ "ok": true }))
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

