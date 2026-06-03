use crate::config;
use crate::kafka_client::build_client_config;
use crate::types::{
    FetchMessagesRequest, FetchMessagesResponse, KafkaMessage, MessageHeader,
    ProduceMessageRequest,
};
use crate::{AppState, LiveSession};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

        let fetch_type = req.fetch_mode.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let end_ms: Option<i64> = req.fetch_mode.get("end_ms").and_then(|v| v.as_i64());

        let mut tpl = TopicPartitionList::new();

        if fetch_type == "from_timestamp" {
            let timestamp_ms = req.fetch_mode
                .get("timestamp")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "[KAFKA] from_timestamp requires timestamp field".to_string())?;

            let timed_offsets = resolve_offsets_for_timestamp(
                &consumer, &req.topic, &target_partitions, timestamp_ms, timeout,
            )?;

            if timed_offsets.is_empty() {
                return Ok(FetchMessagesResponse { messages: vec![], total_fetched: 0, has_more: false });
            }

            for (p, start_offset) in &timed_offsets {
                tpl.add_partition_offset(&req.topic, *p, Offset::Offset(*start_offset))
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
                let (_, high) = consumer.fetch_watermarks(&req.topic, *p, timeout).unwrap_or((0, 0));
                if high > *start_offset {
                    partition_high.insert(*p, high);
                }
            }
        } else {
            for &p in &target_partitions {
                let (low, high) = consumer
                    .fetch_watermarks(&req.topic, p, timeout)
                    .map_err(|e| format!("[KAFKA-WATERMARK] partition {p}: {e}"))?;
                let offset = decide_offset(&req.fetch_mode, low, high, limit, target_partitions.len() as i64)?;
                tpl.add_partition_offset(&req.topic, p, offset)
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
                let start = match offset {
                    Offset::Beginning => low,
                    Offset::Offset(o) => o,
                    _ => low,
                };
                if high > start {
                    partition_high.insert(p, high);
                }
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
        let max_wait = timeout; // use cluster's configured request_timeout_ms
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
                    if let Some(end) = end_ms {
                        let msg_ts = match m.timestamp() {
                            rdkafka::message::Timestamp::CreateTime(ts) => Some(ts),
                            rdkafka::message::Timestamp::LogAppendTime(ts) => Some(ts),
                            _ => None,
                        };
                        if let Some(ts) = msg_ts {
                            if ts > end {
                                partition_high.remove(&m.partition());
                                if partition_high.is_empty() { break; }
                                continue;
                            }
                        }
                    }
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
                Some(Err(e)) => match &e {
                    // librdkafka surfaces NotImplemented for an unsupported codec or an
                    // unrecognised message format. Non-fatal: skip the batch and continue.
                    KafkaError::MessageConsumption(RDKafkaErrorCode::NotImplemented) => {}
                    _ => return Err(format!("[KAFKA-CONSUMER] poll: {e}")),
                },
                None => {
                    // Only treat repeated timeouts as EOF after the first message has
                    // arrived. Before that the consumer may still be connecting or
                    // fetching from a high offset, so early timeouts must not abort.
                    if !messages.is_empty() {
                        consecutive_timeouts += 1;
                        if consecutive_timeouts >= 3 {
                            break;
                        }
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

fn resolve_offsets_for_timestamp(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timestamp_ms: i64,
    timeout: Duration,
) -> Result<Vec<(i32, i64)>, String> {
    let mut tpl = TopicPartitionList::new();
    for &p in partitions {
        tpl.add_partition_offset(topic, p, Offset::Offset(timestamp_ms))
            .map_err(|e| format!("[KAFKA] tpl timestamp: {e}"))?;
    }
    let result = consumer
        .offsets_for_times(tpl, timeout)
        .map_err(|e| format!("[KAFKA-TIMESTAMP] offsets_for_times: {e}"))?;
    let mut out = Vec::new();
    for elem in result.elements() {
        match elem.offset() {
            Offset::Offset(o) => out.push((elem.partition(), o)),
            _ => {}
        }
    }
    Ok(out)
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
    }
}

fn decode_payload(bytes: &[u8]) -> (Option<String>, String) {
    if bytes.is_empty() {
        return (None, "binary".to_string());
    }
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                (Some(s.to_string()), "json".to_string())
            } else {
                (Some(s.to_string()), "text".to_string())
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
        let password = crate::config::load_sasl_password(&req.cluster_id).ok().flatten();
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

        let codec = req.compression.as_str();
        if codec == "none" {
            // Reuse the pooled producer (no compression overhead).
            let bundle = pool.get_or_create(&req.cluster_id)?;
            let producer: &BaseProducer<_> = &bundle.producer;
            producer
                .send(record)
                .map_err(|(e, _)| format!("[KAFKA-PRODUCE] send: {e}"))?;
            producer
                .flush(timeout)
                .map_err(|e| format!("[KAFKA-PRODUCE] flush: {e}"))?;
        } else {
            // Build a one-shot producer with the requested compression codec.
            let mut cfg = crate::kafka_client::build_client_config(&cluster, password.as_deref());
            cfg.set("compression.type", codec);
            let producer: BaseProducer = cfg
                .create()
                .map_err(|e| format!("[KAFKA-PRODUCE] create compressed producer: {e}"))?;
            producer
                .send(record)
                .map_err(|(e, _)| format!("[KAFKA-PRODUCE] send: {e}"))?;
            producer
                .flush(timeout)
                .map_err(|e| format!("[KAFKA-PRODUCE] flush: {e}"))?;
        }

        Ok(json!({ "ok": true }))
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

#[tauri::command]
pub async fn start_live_consume(
    state: State<'_, AppState>,
    req: FetchMessagesRequest,
    session_id: String,
    channel: tauri::ipc::Channel<KafkaMessage>,
) -> Result<(), String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);

    // 停掉同 session_id 的旧实例（前端重连场景）
    if let Some(old) = state.live_sessions.lock().remove(&session_id) {
        old.running.store(false, Ordering::Release);
    }

    let running = Arc::new(AtomicBool::new(true));
    let running_for_thread = running.clone();
    let session_id_for_log = session_id.clone();
    let topic = req.topic.clone();
    let partition_filter = req.partition;

    let handle = std::thread::spawn(move || {
        let password = config::load_sasl_password(&req.cluster_id).ok().flatten();
        let mut cfg = build_client_config(&cluster, password.as_deref());
        cfg.set("group.id", format!("super-kafka-live-{}", uuid::Uuid::new_v4()));
        cfg.set("enable.auto.commit", "false");

        let consumer: BaseConsumer = match cfg.create() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[KAFKA-LIVE] create consumer: {e}");
                return;
            }
        };

        let target_partitions: Vec<i32> = match partition_filter {
            Some(p) => vec![p],
            None => match consumer.fetch_metadata(Some(&topic), timeout) {
                Ok(meta) => meta
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map(|t| t.partitions().iter().map(|p| p.id()).collect())
                    .unwrap_or_default(),
                Err(e) => {
                    eprintln!("[KAFKA-LIVE] fetch_metadata: {e}");
                    return;
                }
            },
        };

        let mut tpl = TopicPartitionList::new();
        for &p in &target_partitions {
            if let Err(e) = tpl.add_partition_offset(&topic, p, Offset::End) {
                eprintln!("[KAFKA-LIVE] tpl add partition {p}: {e}");
                return;
            }
        }
        if let Err(e) = consumer.assign(&tpl) {
            eprintln!("[KAFKA-LIVE] assign: {e}");
            return;
        }

        loop {
            if !running_for_thread.load(Ordering::Acquire) {
                break;
            }
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(m)) => {
                    if channel.send(borrowed_to_kafka_message(&m)).is_err() {
                        break; // 前端已关闭/导航离开
                    }
                }
                Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::NotImplemented))) => {}
                Some(Err(e)) => {
                    eprintln!("[KAFKA-LIVE] poll error: {e}");
                }
                None => {}
            }
        }

        eprintln!("[KAFKA-LIVE] consumer thread exiting: {session_id_for_log}");
    });

    state.live_sessions.lock().insert(
        session_id,
        LiveSession { running, handle },
    );

    Ok(())
}

#[tauri::command]
pub async fn stop_live_consume(
    state: State<'_, AppState>,
    session_id: String,
) -> Result<(), String> {
    if let Some(session) = state.live_sessions.lock().remove(&session_id) {
        session.running.store(false, Ordering::Release);
    }
    Ok(())
}

