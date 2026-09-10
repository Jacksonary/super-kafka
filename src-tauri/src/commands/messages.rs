use crate::config;
use crate::kafka_client::build_client_config;
use crate::types::{
    ClusterConfig, ExportColumn, ExportProgress, ExportRequest, FetchMessagesRequest,
    FetchMessagesResponse, KafkaMessage, MessageHeader, ProduceMessageRequest, SearchProgress,
    SearchSummary,
};
use crate::{AppState, LiveSession};
use std::fs::File;
use std::io::{BufWriter, Write};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::State;

const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// 全量检索最多返回的命中条数(超出即强制停止;前端同步提示该上限)。
const MAX_SEARCH_HITS: i64 = 5_000;

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
        // Message *content* views stay read_committed so the browser shows what a
        // real consumer would see; only watermark reporting uses read_uncommitted
        // (see build_client_config).
        cfg.set("isolation.level", "read_committed");

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

            for &(p, start_offset, high) in &timed_offsets {
                tpl.add_partition_offset(&req.topic, p, Offset::Offset(start_offset))
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
                // `high` comes back validated from the resolver, which already paid
                // for the watermark round-trip.
                if high > start_offset {
                    partition_high.insert(p, high);
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

/// Resolve a timestamp to a starting offset per partition.
///
/// Returns `(partition, start_offset, high_watermark)` for each partition that
/// resolved to a real position. Partitions that did not resolve are omitted
/// rather than seeked to a bogus offset: librdkafka only overwrites the
/// partitions it could actually query, so an unresolved element still holds the
/// timestamp we passed in — and may carry no error at all. Seeking there lands
/// far past the log end and silently drops that partition from the results.
fn resolve_offsets_for_timestamp(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timestamp_ms: i64,
    timeout: Duration,
) -> Result<Vec<(i32, i64, i64)>, String> {
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
        if elem.error().is_err() {
            continue;
        }
        // A negative sentinel means the broker had nothing at or after the
        // timestamp, however rdkafka maps the raw value.
        let Some(offset) = elem.offset().to_raw().filter(|&o| o >= 0) else {
            continue;
        };
        let partition = elem.partition();
        // Not `unwrap_or((0, 0))`: that would make `start > high` true for any real
        // offset and silently drop the partition from the results — the very
        // failure this validation exists to prevent. The non-timestamp branch of
        // `fetch_messages` propagates this error too.
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, timeout)
            .map_err(|e| format!("[KAFKA-WATERMARK] partition {partition}: {e}"))?;
        // Retention may have deleted that segment since the lookup; for a browse
        // the oldest surviving message is the closest we can honour.
        let start = offset.max(low);
        if start > high {
            // Residual timestamp, or the whole range is past the end.
            continue;
        }
        out.push((partition, start, high));
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
        // Message *content* views stay read_committed so the browser shows what a
        // real consumer would see; only watermark reporting uses read_uncommitted
        // (see build_client_config).
        cfg.set("isolation.level", "read_committed");

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

// ── 全量检索 ──────────────────────────────────────────────

/// ASCII 大小写不敏感的子串匹配(零分配)。
/// 非 ASCII 字节按原样比较:中文等无大小写语义的文本照常匹配,但不再做
/// Unicode 大小写折叠(原实现对 É/é 这类非 ASCII 大小写变体可匹配,现在不会)。
fn str_contains_ci(haystack: &str, needle_lower: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle_lower.as_bytes();
    if n.is_empty() || h.len() < n.len() {
        return false;
    }
    h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// 在原始 bytes 上做命中判定:UTF-8 校验零分配,不做拷贝/JSON 探测。
/// 只有命中后才走 `borrowed_to_kafka_message` 做完整转换。
fn raw_matches(key: Option<&[u8]>, payload: &[u8], query_lower: &str) -> bool {
    if query_lower.is_empty() {
        return false;
    }
    if let Some(k) = key.and_then(|b| std::str::from_utf8(b).ok()) {
        if str_contains_ci(k, query_lower) {
            return true;
        }
    }
    if let Ok(v) = std::str::from_utf8(payload) {
        if str_contains_ci(v, query_lower) {
            return true;
        }
    }
    false
}

/// 单个扫描 worker(一个分区组)的结果。
struct ScanCounts {
    hits: i64,
    scanned: i64,
}

/// 在给定分区组内扫描:一个 consumer 负责该组,组内共享 poll 循环。
/// 命中即经 `hits_channel` 推给前端;达到全局命中上限或"首个命中"条件时
/// 置 `cancel`,让其余 worker 一并退出。
#[allow(clippy::too_many_arguments)]
fn scan_partition_group(
    cluster: &ClusterConfig,
    password: Option<&str>,
    topic: &str,
    fetch_mode: &Value,
    partitions: &[i32],
    query_lower: &str,
    stop_after_first: bool,
    limit: i64,
    no_limit: bool,
    user_running: &AtomicBool,
    cancel: &AtomicBool,
    global_hits: &AtomicI64,
    global_scanned: &AtomicI64,
    global_total: &AtomicI64,
    hits_channel: &tauri::ipc::Channel<KafkaMessage>,
) -> Result<ScanCounts, String> {
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);

    let mut cfg = build_client_config(cluster, password);
    cfg.set(
        "group.id",
        format!("super-kafka-search-{}", uuid::Uuid::new_v4()),
    );
    cfg.set("enable.auto.commit", "false");
    // 与 fetch/live/export 保持一致:内容视图用 read_committed。
    cfg.set("isolation.level", "read_committed");

    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| format!("[KAFKA-CONSUMER] create: {e}"))?;

    let fetch_type = fetch_mode.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let end_ms: Option<i64> = fetch_mode.get("end_ms").and_then(|v| v.as_i64());

    let mut partition_high: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    // time_range 模式下,每分区到 end_ms 对应 offset 即停(精确终止,不依赖空 poll 猜测 EOF)。
    let mut end_offsets: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    // 进度用:本组每分区的起点与终点快照,用于预先算出本组要扫的总条数。
    let mut start_of: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    let mut high_snapshot: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
    let mut tpl = TopicPartitionList::new();

    if fetch_type == "from_timestamp" {
        let timestamp_ms = fetch_mode
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| "[KAFKA] from_timestamp requires timestamp field".to_string())?;
        let timed_offsets =
            resolve_offsets_for_timestamp(&consumer, topic, partitions, timestamp_ms, timeout)?;
        for (p, start_offset, high) in timed_offsets {
            tpl.add_partition_offset(topic, p, Offset::Offset(start_offset))
                .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            start_of.insert(p, start_offset);
            high_snapshot.insert(p, high);
            if high > start_offset {
                partition_high.insert(p, high);
            }
        }
    } else {
        for &p in partitions {
            let (low, high) = consumer
                .fetch_watermarks(topic, p, timeout)
                .map_err(|e| format!("[KAFKA-WATERMARK] partition {p}: {e}"))?;
            // No limit 下的 Latest 失去"往回倒 N 条"的边界,按扫到最早处理。
            let offset = if no_limit && fetch_mode.get("type").and_then(|v| v.as_str()) == Some("latest")
            {
                Offset::Beginning
            } else {
                decide_offset(fetch_mode, low, high, limit as i32, partitions.len() as i64)?
            };
            tpl.add_partition_offset(topic, p, offset)
                .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            let start = match offset {
                Offset::Beginning => low,
                Offset::Offset(o) => o,
                _ => low,
            };
            start_of.insert(p, start);
            high_snapshot.insert(p, high);
            if high > start {
                partition_high.insert(p, high);
            }
        }
    }

    if partition_high.is_empty() {
        return Ok(ScanCounts { hits: 0, scanned: 0 });
    }

    consumer
        .assign(&tpl)
        .map_err(|e| format!("[KAFKA-CONSUMER] assign: {e}"))?;

    // time_range 模式下,用 offsets_for_times(end_ms) 求本组每分区"时间=end 的首条 offset"
    // 作为扫描上界,使结束点由时间边界决定,而非依赖空 poll 猜测 EOF。
    if let Some(end) = end_ms {
        let mut etpl = TopicPartitionList::new();
        for &p in partitions {
            let _ = etpl.add_partition_offset(topic, p, Offset::Offset(end));
        }
        if let Ok(res) = consumer.offsets_for_times(etpl, timeout) {
            for elem in res.elements() {
                if elem.error().is_err() {
                    continue;
                }
                if let Some(o) = elem.offset().to_raw().filter(|&o| o >= 0) {
                    end_offsets.insert(elem.partition(), o);
                }
            }
        }
    }

    // 预先累加本组扫描总条数到全局计数器(进度条分母)。
    // 终点优先取 end_offsets(time_range 的时间边界),否则取高水位。
    {
        let mut group_total: i64 = 0;
        for &p in partitions {
            let start = start_of.get(&p).copied();
            let end = end_offsets
                .get(&p)
                .copied()
                .or_else(|| high_snapshot.get(&p).copied());
            if let (Some(s), Some(e)) = (start, end) {
                if e > s {
                    group_total += e - s;
                }
            }
        }
        global_total.fetch_add(group_total, Ordering::Relaxed);
    }

    let mut hits: i64 = 0;
    let mut scanned: i64 = 0;
    let mut idle_polls = 0usize;
    let started = std::time::Instant::now();
    // 首条消息到达前允许更长的等待窗口防连接卡死。
    // 注意:空 poll 不代表 EOF(消费快于拉取时会出现短暂空窗),结束由
    // end_offsets/高水位这些"确定边界"判定;空 poll 仅作空转兜底。
    let startup_deadline = timeout.max(Duration::from_secs(15));

    loop {
        if !user_running.load(Ordering::Acquire) || cancel.load(Ordering::Acquire) {
            break;
        }
        match consumer.poll(POLL_TIMEOUT) {
            Some(Ok(m)) => {
                idle_polls = 0;
                let next_offset = m.offset() + 1;
                let p = m.partition();
                let high = partition_high.get(&p).copied().unwrap_or(0);

                // 时间范围上界(精确终止):
                // - 到达该分区 end_ms 对应的 end_offset => 扫完窗口,停止该分区;
                // - 仍在 end_offset 之前但本条时间戳越界(乱序) => 跳过本条但不终止分区。
                if let Some(end_bound) = end_offsets.get(&p).copied() {
                    if next_offset > end_bound {
                        partition_high.remove(&p);
                        if partition_high.is_empty() {
                            break;
                        }
                        continue;
                    }
                    if let Some(end) = end_ms {
                        let msg_ts = match m.timestamp() {
                            Timestamp::CreateTime(ts) => Some(ts),
                            Timestamp::LogAppendTime(ts) => Some(ts),
                            _ => None,
                        };
                        if let Some(ts) = msg_ts {
                            if ts > end {
                                // 时间超窗但仍在本分区窗口内:跳过,继续扫乱序的窗内消息
                                continue;
                            }
                        }
                    }
                }

                scanned += 1;
                let global_scanned_now = global_scanned.fetch_add(1, Ordering::Relaxed) + 1;
                // 扫描条数上限(全局):No limit 时跳过,否则扫满 limit 条全体停止。
                if !no_limit && global_scanned_now >= limit {
                    cancel.store(true, Ordering::Release);
                    break;
                }
                // 先匹配后转换:非命中消息不拷贝 value、不做 UTF-8/JSON 解码。
                if raw_matches(m.key(), m.payload().unwrap_or(&[]), query_lower) {
                    let msg = borrowed_to_kafka_message(&m);
                    if hits_channel.send(msg).is_err() {
                        // 前端已关闭/导航离开 -> 停止所有 worker
                        cancel.store(true, Ordering::Release);
                        break;
                    }
                    hits += 1;
                    let total = global_hits.fetch_add(1, Ordering::AcqRel) + 1;
                    // 检索最多支持 MAX_SEARCH_HITS 条命中,超出强制停止(前端同样提示)。
                    if stop_after_first || total >= MAX_SEARCH_HITS {
                        cancel.store(true, Ordering::Release);
                        break;
                    }
                }

                // 消费到该分区 high watermark 后不再追踪。
                if next_offset >= high {
                    partition_high.remove(&p);
                    if partition_high.is_empty() {
                        break;
                    }
                }
            }
            Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::NotImplemented))) => {}
            Some(Err(e)) => return Err(format!("[KAFKA-CONSUMER] poll: {e}")),
            None => {
                if scanned > 0 {
                    idle_polls += 1;
                    // 长时间完全空闲且无任何分区到界(几乎不应发生)才兜底退出
                    if idle_polls >= 120 {
                        break;
                    }
                } else if started.elapsed() > startup_deadline {
                    break;
                }
            }
        }
    }

    Ok(ScanCounts { hits, scanned })
}

/// 全量检索:按 `req` 界定的范围扫描 topic,命中经 `hits_channel` 流式推回前端。
/// 多分区时按分区组分片并行(每 worker 独立 consumer + 线程),因此命中到达顺序
/// 与分区推进速度相关,不保证全局有序。
fn search_messages_scan(
    cluster: &ClusterConfig,
    password: Option<&str>,
    req: &FetchMessagesRequest,
    query_lower: &str,
    stop_after_first: bool,
    no_limit: bool,
    running: &AtomicBool,
    hits_channel: &tauri::ipc::Channel<KafkaMessage>,
    progress_channel: &tauri::ipc::Channel<SearchProgress>,
) -> SearchSummary {
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    // limit 语义:最多扫描的条数(No limit 时忽略)。
    let limit = req.limit.max(1).min(10_000) as i64;

    // 统一构造 summary,并附带整段扫描的实际耗时(前端 toast 展示)。
    let started = std::time::Instant::now();
    let mk_summary =
        |hits: i64, scanned: i64, reason: &str, error: Option<String>| SearchSummary {
            hits,
            scanned,
            reason: reason.to_string(),
            error,
            elapsed_ms: started.elapsed().as_millis() as i64,
        };

    // 用一个临时 consumer 只取分区列表;实际扫描由各 worker 自建 consumer。
    let mut cfg = build_client_config(cluster, password);
    cfg.set(
        "group.id",
        format!("super-kafka-search-meta-{}", uuid::Uuid::new_v4()),
    );
    cfg.set("enable.auto.commit", "false");
    cfg.set("isolation.level", "read_committed");
    let probe: BaseConsumer = match cfg.create() {
        Ok(c) => c,
        Err(e) => {
            return mk_summary(
                0,
                0,
                "error",
                Some(format!("[KAFKA-CONSUMER] create: {e}")),
            )
        }
    };
    let target_partitions: Vec<i32> = match req.partition {
        Some(p) => vec![p],
        None => {
            let meta = match probe.fetch_metadata(Some(&req.topic), timeout) {
                Ok(m) => m,
                Err(e) => {
                    return mk_summary(0, 0, "error", Some(format!("[KAFKA-METADATA] {e}")))
                }
            };
            match meta.topics().iter().find(|t| t.name() == req.topic) {
                Some(t) => t.partitions().iter().map(|p| p.id()).collect(),
                None => {
                    return mk_summary(
                        0,
                        0,
                        "error",
                        Some(format!("[KAFKA] topic `{}` not found", req.topic)),
                    )
                }
            }
        }
    };
    drop(probe);

    if target_partitions.is_empty() {
        return mk_summary(0, 0, "eof", None);
    }

    // 并行取消标志(任一 worker 达成终止条件时广播)与全局聚合计数。
    // 计数用 Arc 共享给进度 reporter 线程,worker 借用同一份。
    let cancel = AtomicBool::new(false);
    let global_hits = Arc::new(AtomicI64::new(0));
    let global_scanned = Arc::new(AtomicI64::new(0));
    let global_total = Arc::new(AtomicI64::new(0));

    // worker 数上限,避免分区过多时线程/连接爆炸;超出则轮转分组。
    const MAX_WORKERS: usize = 16;
    let groups: Vec<Vec<i32>> = if target_partitions.len() <= MAX_WORKERS {
        target_partitions.iter().map(|&p| vec![p]).collect()
    } else {
        let mut g: Vec<Vec<i32>> = vec![Vec::new(); MAX_WORKERS];
        for (i, &p) in target_partitions.iter().enumerate() {
            g[i % MAX_WORKERS].push(p);
        }
        g.retain(|v| !v.is_empty());
        g
    };

    let outcomes: Vec<Result<ScanCounts, String>> = std::thread::scope(|s| {
        // 进度 reporter:唯一上报者,周期读取聚合原子计数推给前端,保证进度单调无乱序。
        // workers 全部 join 后置 finished,reporter 在下一轮(≤200ms)推完最后一帧退出。
        let finished = Arc::new(AtomicBool::new(false));
        let finished_reporter = finished.clone();
        let (scanned_r, total_r, hits_r) =
            (global_scanned.clone(), global_total.clone(), global_hits.clone());
        s.spawn(move || {
            let report_started = std::time::Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(200));
                let _ = progress_channel.send(SearchProgress {
                    scanned: scanned_r.load(Ordering::Relaxed),
                    total: total_r.load(Ordering::Relaxed),
                    hits: hits_r.load(Ordering::Relaxed),
                    elapsed_ms: report_started.elapsed().as_millis() as i64,
                });
                if finished_reporter.load(Ordering::Acquire) {
                    break;
                }
            }
        });

        let handles: Vec<_> = groups
            .iter()
            .map(|partitions| {
                s.spawn(|| {
                    scan_partition_group(
                        cluster,
                        password,
                        &req.topic,
                        &req.fetch_mode,
                        partitions,
                        query_lower,
                        stop_after_first,
                        limit,
                        no_limit,
                        running,
                        &cancel,
                        &global_hits,
                        &global_scanned,
                        &global_total,
                        hits_channel,
                    )
                })
            })
            .collect();
        let outcomes: Vec<Result<ScanCounts, String>> = handles
            .into_iter()
            .map(|h| match h.join() {
                Ok(r) => r,
                Err(_) => Err("[SEARCH] worker panicked".to_string()),
            })
            .collect();
        finished.store(true, Ordering::Release);
        outcomes
    });

    let mut hits: i64 = 0;
    let mut scanned: i64 = 0;
    let mut first_err: Option<String> = None;
    for o in outcomes {
        match o {
            Ok(c) => {
                hits += c.hits;
                scanned += c.scanned;
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    let reason = if first_err.is_some() {
        "error"
    } else if !running.load(Ordering::Acquire) {
        "stopped_by_user"
    } else if stop_after_first && hits > 0 {
        "first_match"
    } else if hits >= MAX_SEARCH_HITS {
        "hit_cap"
    } else if !no_limit && scanned >= limit {
        "limit"
    } else {
        "eof"
    };

    mk_summary(hits, scanned, reason, first_err)
}

#[tauri::command]
pub async fn start_search_consume(
    state: State<'_, AppState>,
    req: FetchMessagesRequest,
    session_id: String,
    query: String,
    stop_after_first: bool,
    no_limit: bool,
    hits_channel: tauri::ipc::Channel<KafkaMessage>,
    done_channel: tauri::ipc::Channel<SearchSummary>,
    progress_channel: tauri::ipc::Channel<SearchProgress>,
) -> Result<(), String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let query_lower = query.trim().to_lowercase();
    if query_lower.is_empty() {
        return Err("[SEARCH] query is empty".to_string());
    }

    // 停掉同 session_id 的旧实例（前端重连场景）
    if let Some(old) = state.search_sessions.lock().remove(&session_id) {
        old.running.store(false, Ordering::Release);
    }

    let running = Arc::new(AtomicBool::new(true));
    let running_for_thread = running.clone();
    let sessions = state.search_sessions.clone();
    let session_id_for_removal = session_id.clone();

    let handle = std::thread::spawn(move || {
        let password = config::load_sasl_password(&req.cluster_id).ok().flatten();
        let summary = search_messages_scan(
            &cluster,
            password.as_deref(),
            &req,
            &query_lower,
            stop_after_first,
            no_limit,
            &running_for_thread,
            &hits_channel,
            &progress_channel,
        );
        let _ = done_channel.send(summary);
        // 线程自行移除条目，避免 map 残留已死会话。
        sessions.lock().remove(&session_id_for_removal);
    });

    state
        .search_sessions
        .lock()
        .insert(session_id, LiveSession { running, handle });

    Ok(())
}

#[tauri::command]
pub async fn stop_search_consume(
    state: State<'_, AppState>,
    session_id: String,
) -> Result<(), String> {
    if let Some(session) = state.search_sessions.lock().remove(&session_id) {
        session.running.store(false, Ordering::Release);
    }
    Ok(())
}

// ── 流式导出 ──────────────────────────────────────────────

/// CSV-escape a single field per RFC 4180.
fn csv_escape(value: &str) -> String {
    if value.contains(['"', ',', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Read a dotted JSON path (e.g. `adress.country`) from a value. Returns "" when
/// any segment is missing or a non-object is traversed. Objects/arrays are
/// serialized back to JSON text; scalars become their plain string form.
fn json_path_value(root: &Value, path: &str) -> String {
    let mut cur = root;
    for part in path.split('.') {
        match cur {
            Value::Object(map) => match map.get(part) {
                Some(v) => cur = v,
                None => return String::new(),
            },
            _ => return String::new(),
        }
    }
    match cur {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Object(_) | Value::Array(_) => cur.to_string(),
        other => other.to_string(),
    }
}

/// Normalize user columns: trim names, default empty path to the column name,
/// drop columns without a name.
fn normalize_columns(columns: &[ExportColumn]) -> Vec<ExportColumn> {
    columns
        .iter()
        .filter_map(|c| {
            let name = c.name.trim().to_string();
            if name.is_empty() {
                return None;
            }
            let path = if c.path.trim().is_empty() {
                name.clone()
            } else {
                c.path.trim().to_string()
            };
            Some(ExportColumn { name, path })
        })
        .collect()
}

#[tauri::command]
pub async fn export_messages(
    state: State<'_, AppState>,
    req: ExportRequest,
    session_id: String,
    channel: tauri::ipc::Channel<ExportProgress>,
) -> Result<(), String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let password = config::load_sasl_password(&req.cluster_id).ok().flatten();
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);

    let cols = normalize_columns(&req.columns);
    if cols.is_empty() {
        return Err("[EXPORT] at least one named column is required".to_string());
    }

    // Register a cancel flag so stop_export can signal this run.
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let mut sessions = state.export_sessions.lock();
        if let Some(old) = sessions.insert(session_id.clone(), cancel.clone()) {
            old.store(true, Ordering::Release);
        }
    }

    let progress_channel = channel.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<ExportProgress, String> {
        let channel = progress_channel;
        // None / <=0 means "until EOF".
        let max_records = req.max_records.filter(|n| *n > 0);
        // decide_offset needs an i32 rewind hint for "latest" mode.
        let offset_limit = max_records
            .unwrap_or(i64::from(i32::MAX))
            .min(i64::from(i32::MAX)) as i32;

        let mut cfg = build_client_config(&cluster, password.as_deref());
        cfg.set(
            "group.id",
            format!("super-kafka-export-{}", uuid::Uuid::new_v4()),
        );
        cfg.set("enable.auto.commit", "false");
        // Message *content* views stay read_committed so the browser shows what a
        // real consumer would see; only watermark reporting uses read_uncommitted
        // (see build_client_config).
        cfg.set("isolation.level", "read_committed");

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

        let mut partition_high: std::collections::HashMap<i32, i64> =
            std::collections::HashMap::new();
        let fetch_type = req.fetch_mode.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let end_ms: Option<i64> = req.fetch_mode.get("end_ms").and_then(|v| v.as_i64());
        let mut tpl = TopicPartitionList::new();

        if fetch_type == "from_timestamp" {
            let timestamp_ms = req
                .fetch_mode
                .get("timestamp")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "[KAFKA] from_timestamp requires timestamp field".to_string())?;
            let timed_offsets = resolve_offsets_for_timestamp(
                &consumer, &req.topic, &target_partitions, timestamp_ms, timeout,
            )?;
            for &(p, start_offset, high) in &timed_offsets {
                tpl.add_partition_offset(&req.topic, p, Offset::Offset(start_offset))
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
                // `high` comes back validated from the resolver, which already paid
                // for the watermark round-trip.
                if high > start_offset {
                    partition_high.insert(p, high);
                }
            }
        } else {
            for &p in &target_partitions {
                let (low, high) = consumer
                    .fetch_watermarks(&req.topic, p, timeout)
                    .map_err(|e| format!("[KAFKA-WATERMARK] partition {p}: {e}"))?;
                let offset = decide_offset(
                    &req.fetch_mode,
                    low,
                    high,
                    offset_limit,
                    target_partitions.len() as i64,
                )?;
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

        // Open the output file and write the header before consuming.
        let file = File::create(&req.out_path)
            .map_err(|e| format!("[EXPORT] create file `{}`: {e}", req.out_path))?;
        let mut writer = BufWriter::new(file);
        let header = cols
            .iter()
            .map(|c| csv_escape(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        writer
            .write_all(header.as_bytes())
            .and_then(|_| writer.write_all(b"\n"))
            .map_err(|e| format!("[EXPORT] write header: {e}"))?;

        // Empty selection: just an header-only file.
        if partition_high.is_empty() {
            writer.flush().map_err(|e| format!("[EXPORT] flush: {e}"))?;
            return Ok(ExportProgress { written: 0, done: true, cancelled: false, error: None });
        }

        let mut written: i64 = 0;
        let mut consecutive_timeouts = 0usize;
        let mut row: Vec<String> = Vec::with_capacity(cols.len());
        // Bound the time we wait for the *first* message so a stalled connection
        // can't hang the export forever. Once data flows, EOF is detected by
        // watermarks + consecutive empty polls instead.
        let started = std::time::Instant::now();
        let startup_deadline = timeout.max(Duration::from_secs(15));

        loop {
            if cancel.load(Ordering::Acquire) {
                writer.flush().map_err(|e| format!("[EXPORT] flush: {e}"))?;
                return Ok(ExportProgress { written, done: true, cancelled: true, error: None });
            }
            if let Some(max) = max_records {
                if written >= max {
                    break;
                }
            }
            match consumer.poll(POLL_TIMEOUT) {
                Some(Ok(m)) => {
                    consecutive_timeouts = 0;
                    let next_offset = m.offset() + 1;
                    let high = partition_high.get(&m.partition()).copied().unwrap_or(0);

                    // Time-range upper bound: stop tracking a partition past end_ms.
                    if let Some(end) = end_ms {
                        let msg_ts = match m.timestamp() {
                            Timestamp::CreateTime(ts) => Some(ts),
                            Timestamp::LogAppendTime(ts) => Some(ts),
                            _ => None,
                        };
                        if let Some(ts) = msg_ts {
                            if ts > end {
                                partition_high.remove(&m.partition());
                                if partition_high.is_empty() {
                                    break;
                                }
                                continue;
                            }
                        }
                    }

                    // Build the CSV row from the message value JSON.
                    let payload = m.payload().unwrap_or_default();
                    let (value_text, _) = decode_payload(payload);
                    let parsed = value_text
                        .as_deref()
                        .and_then(|s| serde_json::from_str::<Value>(s).ok());
                    row.clear();
                    for c in &cols {
                        let cell = match &parsed {
                            Some(root) => json_path_value(root, &c.path),
                            None => String::new(),
                        };
                        row.push(csv_escape(&cell));
                    }
                    let line = row.join(",");
                    writer
                        .write_all(line.as_bytes())
                        .and_then(|_| writer.write_all(b"\n"))
                        .map_err(|e| format!("[EXPORT] write row: {e}"))?;
                    written += 1;

                    if written % 500 == 0 {
                        let _ = channel.send(ExportProgress {
                            written,
                            done: false,
                            cancelled: false,
                            error: None,
                        });
                    }

                    if next_offset >= high {
                        partition_high.remove(&m.partition());
                        if partition_high.is_empty() {
                            break;
                        }
                    }
                }
                Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::NotImplemented))) => {}
                Some(Err(e)) => return Err(format!("[KAFKA-CONSUMER] poll: {e}")),
                None => {
                    // After data has started flowing, repeated empty polls mean EOF.
                    if written > 0 {
                        consecutive_timeouts += 1;
                        if consecutive_timeouts >= 6 {
                            break;
                        }
                    } else if started.elapsed() > startup_deadline {
                        // No first message within the startup window: treat as empty.
                        break;
                    }
                }
            }
        }

        writer.flush().map_err(|e| format!("[EXPORT] flush: {e}"))?;
        Ok(ExportProgress { written, done: true, cancelled: false, error: None })
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?;

    // Clean up the cancel flag regardless of outcome.
    state.export_sessions.lock().remove(&session_id);

    match result {
        Ok(progress) => {
            let _ = channel.send(progress);
            Ok(())
        }
        Err(e) => {
            let _ = channel.send(ExportProgress {
                written: 0,
                done: true,
                cancelled: false,
                error: Some(e.clone()),
            });
            Err(e)
        }
    }
}

#[tauri::command]
pub async fn stop_export(state: State<'_, AppState>, session_id: String) -> Result<(), String> {
    if let Some(cancel) = state.export_sessions.lock().get(&session_id) {
        cancel.store(true, Ordering::Release);
    }
    Ok(())
}

