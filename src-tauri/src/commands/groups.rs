use crate::config;
use crate::kafka_client::build_client_config;
use crate::types::{
    AssignedPartition, ClusterConfig, ConsumerGroupDetail, ConsumerGroupSummary, GroupMember,
    PartitionLag, ResetOffsetRequest, TopicConsumerGroup,
};
use crate::AppState;
use rdkafka::admin::AdminOptions;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::time::Duration;
use tauri::State;

#[tauri::command]
pub async fn list_consumer_groups(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<Vec<ConsumerGroupSummary>, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();

    tokio::task::spawn_blocking(move || -> Result<Vec<ConsumerGroupSummary>, String> {
        let bundle = pool.get_or_create(&id)?;
        let groups = bundle
            .admin
            .inner()
            .fetch_group_list(None, timeout)
            .map_err(|e| format!("[KAFKA-GROUPS] {e}"))?;
        let mut out = Vec::with_capacity(groups.groups().len());
        for g in groups.groups() {
            out.push(ConsumerGroupSummary {
                group_id: g.name().to_string(),
                state: g.state().to_string(),
                member_count: g.members().len() as i32,
            });
        }
        Ok(out)
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

#[tauri::command]
pub async fn get_consumer_group_detail(
    state: State<'_, AppState>,
    cluster_id: String,
    group_id: String,
) -> Result<ConsumerGroupDetail, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let gid = group_id.clone();
    tokio::task::spawn_blocking(move || -> Result<ConsumerGroupDetail, String> {
        let bundle = pool.get_or_create(&id)?;
        let groups = bundle
            .admin
            .inner()
            .fetch_group_list(Some(&gid), timeout)
            .map_err(|e| format!("[KAFKA-GROUPS] {e}"))?;
        let group = groups
            .groups()
            .iter()
            .find(|g| g.name() == gid)
            .ok_or_else(|| format!("[KAFKA] consumer group `{gid}` not found"))?;

        let members: Vec<GroupMember> = group
            .members()
            .iter()
            .map(|m| GroupMember {
                member_id: m.id().to_string(),
                client_id: m.client_id().to_string(),
                client_host: m.client_host().to_string(),
                assigned_partitions: parse_assignment(m.assignment().unwrap_or_default()),
            })
            .collect();

        let group_id = group.name().to_string();
        let state = group.state().to_string();
        Ok(ConsumerGroupDetail {
            group_id,
            state,
            members,
        })
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

/// Parse the binary ConsumerProtocolAssignment payload.
///
/// Wire format (big-endian):
///   INT16  version
///   INT32  topic_count
///   for each topic:
///     INT16  topic_name_len
///     bytes  topic_name (UTF-8)
///     INT32  partition_count
///     for each partition:
///       INT32 partition_id
///   INT32  user_data_len  (-1 = null)
///   bytes  user_data
fn parse_assignment(bytes: &[u8]) -> Vec<AssignedPartition> {
    if bytes.len() < 6 {
        return Vec::new();
    }
    let mut pos = 0;

    // version (INT16) — ignore value, format is identical for v0 and v1
    pos += 2;

    let topic_count = i32::from_be_bytes(match bytes[pos..pos + 4].try_into() {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    }) as usize;
    pos += 4;

    let mut out = Vec::new();
    for _ in 0..topic_count {
        if pos + 2 > bytes.len() {
            break;
        }
        let name_len_raw = i16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        pos += 2;
        if name_len_raw <= 0 {
            break;
        }
        let name_len = name_len_raw as usize;
        if pos + name_len > bytes.len() {
            break;
        }
        let topic = match std::str::from_utf8(&bytes[pos..pos + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => break,
        };
        pos += name_len;

        if pos + 4 > bytes.len() {
            break;
        }
        let part_count = i32::from_be_bytes(match bytes[pos..pos + 4].try_into() {
            Ok(b) => b,
            Err(_) => break,
        }) as usize;
        pos += 4;

        for _ in 0..part_count {
            if pos + 4 > bytes.len() {
                break;
            }
            let partition = i32::from_be_bytes(match bytes[pos..pos + 4].try_into() {
                Ok(b) => b,
                Err(_) => break,
            });
            pos += 4;
            out.push(AssignedPartition {
                topic: topic.clone(),
                partition,
            });
        }
    }
    out
}

#[tauri::command]
pub async fn delete_consumer_group(
    state: State<'_, AppState>,
    cluster_id: String,
    group_id: String,
) -> Result<Value, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new().request_timeout(Some(timeout));
    let results = bundle
        .admin
        .delete_groups(&[group_id.as_str()], &opts)
        .await
        .map_err(|e| format!("[KAFKA-DELETE-GROUP] {e}"))?;
    for r in results {
        if let Err((name, err)) = r {
            return Err(format!("[KAFKA-DELETE-GROUP] {name}: {err}"));
        }
    }
    Ok(json!({ "ok": true }))
}

/// `fetch_watermarks` with the error propagated instead of swallowed.
///
/// Silently substituting `(0, 0)` on failure would commit offset 0 as `latest`
/// — the opposite of what the caller asked for — or push a consumer behind the
/// log start on a topic whose earliest offset is not 0. A reset that fails
/// loudly is always better than one that commits the wrong position.
fn fetch_watermarks_strict(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<(i64, i64), String> {
    consumer
        .fetch_watermarks(topic, partition, timeout)
        .map_err(|e| format!("[KAFKA-WATERMARKS] partition {partition}: {e}"))
}

#[tauri::command]
pub async fn reset_offset(
    state: State<'_, AppState>,
    req: ResetOffsetRequest,
) -> Result<Value, String> {
    let cluster = state
        .pool
        .get_config(&req.cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{}` not found", req.cluster_id))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);

    tokio::task::spawn_blocking(move || -> Result<Value, String> {
        let password = config::load_sasl_password(&req.cluster_id).ok().flatten();
        let mut cfg = build_client_config(&cluster, password.as_deref());
        cfg.set("group.id", &req.group_id);
        cfg.set("enable.auto.commit", "false");
        let consumer: BaseConsumer = cfg
            .create()
            .map_err(|e| format!("[KAFKA-CONSUMER] create: {e}"))?;

        // Resetting offsets is only reliable when no active member holds the
        // partition (a live consumer would overwrite our commit). Require the
        // group to be Empty/Dead.
        let group_list = consumer
            .fetch_group_list(Some(&req.group_id), timeout)
            .map_err(|e| format!("[KAFKA-GROUPS] {e}"))?;
        // If the group isn't registered with the coordinator (not found here), allow
        // the reset — it effectively pre-sets offsets for a not-yet-active group.
        if let Some(g) = group_list
            .groups()
            .iter()
            .find(|g| g.name() == req.group_id)
        {
            let state = g.state();
            if state != "Empty" && state != "Dead" {
                return Err(format!(
                    "[KAFKA-RESET] group `{}` is `{state}`; offsets can only be reset when the group is Empty — stop all consumers first",
                    req.group_id
                ));
            }
        }

        let meta = consumer
            .fetch_metadata(Some(&req.topic), timeout)
            .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;
        let topic_meta = meta
            .topics()
            .iter()
            .find(|t| t.name() == req.topic)
            .ok_or_else(|| format!("[KAFKA] topic `{}` not found", req.topic))?;
        // A deleted or not-yet-propagated topic still comes back as an entry, just
        // with an error and no partitions. Without this the "no partitions" guard
        // below fires and sends the operator hunting for a partition problem on a
        // topic that simply is not there.
        if let Some(err) = topic_meta.error() {
            return Err(format!(
                "[KAFKA-METADATA] topic `{}`: {:?}",
                req.topic, err
            ));
        }

        // Target a single partition if requested, otherwise every partition.
        let target_parts: Vec<i32> = match req.partition {
            Some(p) => {
                if !topic_meta.partitions().iter().any(|tp| tp.id() == p) {
                    return Err(format!(
                        "[KAFKA] partition {p} not found in topic `{}`",
                        req.topic
                    ));
                }
                vec![p]
            }
            None => topic_meta.partitions().iter().map(|tp| tp.id()).collect(),
        };

        // A commit over an empty TPL succeeds and would be reported as a clean
        // reset, so refuse rather than claim to have moved nothing.
        if target_parts.is_empty() {
            return Err(format!(
                "[KAFKA-RESET] topic `{}` has no partitions to reset",
                req.topic
            ));
        }
        // Captured before the loop below consumes `target_parts`.
        let target_count = target_parts.len();

        // Resetting offsets is destructive, so never guess at the strategy.
        // `api.ts` hand-maps the frontend names (`to_offset` -> `specific`,
        // `to_timestamp` -> `timestamp`) while `earliest`/`latest` pass through
        // untouched; if that mapping ever drifts, defaulting to `earliest` here
        // would rewind every partition to the log start and report success.
        let strategy = req
            .strategy
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "[KAFKA-RESET] strategy is missing a `type` field".to_string())?;

        let mut tpl = TopicPartitionList::new();
        // Human-readable detail for the positions we could not honour verbatim.
        let mut warnings: Vec<String> = Vec::new();
        // Counted separately from `warnings` and from each other: a broker that
        // could not answer, data that aged out, and "nothing after that time" call
        // for different wording, and an empty partition warrants none at all. A
        // single counter made the dialog contradict its own detail lines.
        let mut fell_back = 0usize;
        let mut unresolved = 0usize;
        let mut aged_out = 0usize;
        let mut clamped_count = 0usize;

        if strategy == "timestamp" {
            let ts_ms = req
                .strategy
                .get("timestamp")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "[KAFKA] timestamp strategy requires `timestamp` field".to_string())?;
            if ts_ms < 0 {
                return Err("[KAFKA] timestamp must be a non-negative unix millisecond value".to_string());
            }

            // Build a TPL with the timestamp as the "offset" for each partition,
            // then resolve to actual offsets in a single broker round-trip.
            let mut ts_tpl = TopicPartitionList::new();
            for &pid in &target_parts {
                ts_tpl
                    .add_partition_offset(&req.topic, pid, Offset::Offset(ts_ms))
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            }
            let resolved = consumer
                .offsets_for_times(ts_tpl, timeout)
                .map_err(|e| format!("[KAFKA-TIMESTAMP] offsets_for_times: {e}"))?;

            for &pid in &target_parts {
                let (low, high) = fetch_watermarks_strict(&consumer, &req.topic, pid, timeout)?;
                let elem = resolved.find_partition(&req.topic, pid);
                let elem_err = elem.as_ref().and_then(|e| e.error().err());
                // Treat a negative sentinel the same as an absent value: both mean
                // "the broker returned no position", however rdkafka maps the raw -1.
                let candidate = elem.and_then(|e| e.offset().to_raw()).filter(|&o| o >= 0);

                // Four outcomes, each meaning something different to the operator.
                // Collapsing them into one counter produced dialogs that contradicted
                // their own detail lines, so keep them apart.
                let offset = if low > high {
                    // Mid-compaction / mid-leader-change: the range is unusable and
                    // committing `high` would sit below the log start.
                    unresolved += 1;
                    warnings.push(format!(
                        "partition {pid}: log range [{low}, {high}] is inconsistent (compaction or leader change in flight) — reset to the log start ({low})"
                    ));
                    Offset::Offset(low)
                } else if let Some(err) = elem_err {
                    unresolved += 1;
                    warnings.push(format!(
                        "partition {pid}: could not resolve the timestamp ({err}) — reset to latest ({high})"
                    ));
                    Offset::Offset(high)
                } else if let Some(o) = candidate {
                    if o > high {
                        // librdkafka leaves partitions it could not query holding the
                        // timestamp we passed in, which lands far above the log end.
                        unresolved += 1;
                        warnings.push(format!(
                            "partition {pid}: the broker returned no position for the requested time — reset to latest ({high})"
                        ));
                        Offset::Offset(high)
                    } else if o < low {
                        // Retention deleted that segment between the lookup and the
                        // watermark read. The oldest surviving message is the closest
                        // we can honour — and it is the same side of the log the user
                        // asked for, unlike jumping to the end.
                        aged_out += 1;
                        warnings.push(format!(
                            "partition {pid}: messages from that time have already been deleted by retention — reset to the oldest available offset ({low})"
                        ));
                        Offset::Offset(low)
                    } else {
                        Offset::Offset(o)
                    }
                } else {
                    // The broker answered: nothing at or after that timestamp.
                    if low < high {
                        fell_back += 1;
                        warnings.push(format!(
                            "partition {pid}: no message at or after the requested time — reset to latest ({high})"
                        ));
                    }
                    // An empty partition (low == high) has exactly one valid position
                    // and it is the one we commit, so there is nothing to report —
                    // counting it would raise a warning with no evidence behind it.
                    Offset::Offset(high)
                };
                tpl.add_partition_offset(&req.topic, pid, offset)
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            }
        } else {
            for pid in target_parts {
                let (low, high) = fetch_watermarks_strict(&consumer, &req.topic, pid, timeout)?;
                let offset = match strategy {
                    "earliest" => Offset::Offset(low),
                    "latest" => Offset::Offset(high),
                    "specific" => {
                        let off = req
                            .strategy
                            .get("offset")
                            .and_then(|v| v.as_i64())
                            .ok_or_else(|| "[KAFKA] specific requires `offset`".to_string())?;
                        // A partition mid-compaction can report low > high, which
                        // would make clamp() panic — normalize to an empty range.
                        let (lo, hi) = if low > high { (high, high) } else { (low, high) };
                        let clamped = off.clamp(lo, hi);
                        if clamped != off {
                            clamped_count += 1;
                            warnings.push(format!(
                                "partition {pid}: offset {off} is outside the log range [{lo}, {hi}] — committed {clamped} instead"
                            ));
                        }
                        Offset::Offset(clamped)
                    }
                    other => {
                        return Err(format!(
                            "[KAFKA-RESET] unknown strategy `{other}` — expected one of earliest, latest, specific, timestamp"
                        ))
                    }
                };
                tpl.add_partition_offset(&req.topic, pid, offset)
                    .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
            }
        }

        consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| {
                let raw = e.to_string();
                // A group whose members the coordinator can still see rejects a
                // commit from a non-member. The classic coordinator answers
                // ILLEGAL_GENERATION, the KIP-848 one UNKNOWN_MEMBER_ID; either
                // way the actionable advice is the same, and the raw broker text
                // reads like a transient fault the operator would just retry.
                let looks_like_active_members = raw.contains("Unknown member id")
                    || raw.contains("UNKNOWN_MEMBER_ID")
                    || raw.contains("Illegal generation")
                    || raw.contains("ILLEGAL_GENERATION")
                    || raw.contains("Rebalance in progress")
                    || raw.contains("REBALANCE_IN_PROGRESS");
                if looks_like_active_members {
                    format!(
                        "[KAFKA-COMMIT] group `{}` still has active members — stop all its consumers, then retry ({raw})",
                        req.group_id
                    )
                } else {
                    // The commit is one request covering every partition, but a
                    // partial application is possible (e.g. coordinator moved
                    // mid-flight), so don't imply nothing changed.
                    format!(
                        "[KAFKA-COMMIT] {raw} — some partitions may already have been committed; refresh and re-check the offsets before retrying"
                    )
                }
            })?;
        Ok(json!({
            "ok": true,
            "partitions": target_count,
            "fell_back": fell_back,
            "unresolved": unresolved,
            "aged_out": aged_out,
            "clamped": clamped_count,
            "warnings": warnings,
        }))
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

/// Fetch a group's committed offsets and lag for every partition of `topic`.
/// Works for Empty groups too: committed offsets are read from
/// __consumer_offsets, not from live member assignment.
///
/// A failed watermark read degrades that one row to `(0, 0)` rather than failing
/// the whole call: one slow partition must not blank the table and make the other
/// partitions unresettable. The offsets here are a display value and a best-effort
/// hint for the reset dialog — the dialog ignores an unusable range, and
/// `reset_offset` clamps authoritatively against watermarks it fetches itself.
fn fetch_group_topic_lag(
    cluster: &ClusterConfig,
    password: Option<&str>,
    topic: &str,
    group_id: &str,
    timeout: Duration,
) -> Result<Vec<PartitionLag>, String> {
    let mut cfg = build_client_config(cluster, password);
    cfg.set("group.id", group_id);
    cfg.set("enable.auto.commit", "false");
    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| format!("[KAFKA-CONSUMER] create: {e}"))?;

    let meta = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;
    let topic_meta = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("[KAFKA] topic `{topic}` not found"))?;

    let mut tpl = TopicPartitionList::new();
    for p in topic_meta.partitions() {
        tpl.add_partition_offset(topic, p.id(), Offset::Invalid)
            .map_err(|e| format!("[KAFKA] tpl: {e}"))?;
    }
    let committed = consumer
        .committed_offsets(tpl, timeout)
        .map_err(|e| format!("[KAFKA] committed: {e}"))?;

    // Collect committed offsets (already batched, fast)
    let elems: Vec<(i32, i64)> = committed.elements().iter()
        .map(|elem| {
            let current = match elem.offset() { Offset::Offset(o) => o, _ => -1 };
            (elem.partition(), current)
        })
        .collect();

    // Parallel watermark fetches
    use rayon::prelude::*;
    let mut out: Vec<PartitionLag> = elems.par_iter()
        .map(|&(partition, current)| {
            let (low, high) = consumer
                .fetch_watermarks(topic, partition, timeout)
                .unwrap_or((0, 0));
            let lag = if current < 0 { high } else { (high - current).max(0) };
            PartitionLag { partition, start_offset: low, current_offset: current, log_end_offset: high, lag }
        })
        .collect();
    out.sort_by_key(|p| p.partition);
    Ok(out)
}

#[tauri::command]
pub async fn get_topic_group_partition_lag(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
    group_id: String,
) -> Result<Vec<PartitionLag>, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);

    tokio::task::spawn_blocking(move || {
        let password = config::load_sasl_password(&cluster_id).ok().flatten();
        fetch_group_topic_lag(&cluster, password.as_deref(), &topic, &group_id, timeout)
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}

#[tauri::command]
pub async fn list_topic_consumer_groups(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
) -> Result<Vec<TopicConsumerGroup>, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();

    tokio::task::spawn_blocking(move || -> Result<Vec<TopicConsumerGroup>, String> {
        let password = config::load_sasl_password(&id).ok().flatten();
        let bundle = pool.get_or_create(&id)?;
        let groups = bundle
            .admin
            .inner()
            .fetch_group_list(None, timeout)
            .map_err(|e| format!("[KAFKA-GROUPS] {e}"))?;

        // Collect to owned data (GroupInfo is not Send)
        let relevant: Vec<(String, String)> = groups.groups().iter()
            .filter(|g| {
                let ptype = g.protocol_type();
                ptype.is_empty() || ptype == "consumer"
            })
            .map(|g| (g.name().to_string(), g.state().to_string()))
            .collect();

        use rayon::prelude::*;
        let mut out: Vec<TopicConsumerGroup> = relevant
            .par_iter()
            .filter_map(|(group_id, state)| {
                let lags = match fetch_group_topic_lag(&cluster, password.as_deref(), &topic, group_id, timeout) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("[list_topic_consumer_groups] skip group `{group_id}`: {e}");
                        return None;
                    }
                };
                if !lags.iter().any(|p| p.current_offset >= 0) {
                    return None;
                }
                let total_lag: i64 = lags.iter()
                    .filter(|p| p.current_offset >= 0)
                    .map(|p| p.lag)
                    .sum();
                Some(TopicConsumerGroup {
                    group_id: group_id.clone(),
                    state: state.clone(),
                    total_lag,
                })
            })
            .collect();
        out.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(out)
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?
}
