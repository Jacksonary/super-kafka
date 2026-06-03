use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::{
    AdminOptions, AlterConfig, ConfigEntry, ConfigSource, NewPartitions, NewTopic,
    ResourceSpecifier, TopicReplication,
};
use tauri::State;

use crate::types::{
    CreateTopicRequest, PartitionInfo, TopicConfig, TopicDetail, TopicSummary,
};
use crate::AppState;

fn timeout_for(state: &State<'_, AppState>, cluster_id: &str) -> Result<Duration, String> {
    let cluster = state
        .pool
        .get_config(cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    Ok(Duration::from_millis(cluster.request_timeout_ms as u64))
}

#[tauri::command]
pub async fn list_topics(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<Vec<TopicSummary>, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();

    let topics = tokio::task::spawn_blocking(move || -> Result<Vec<TopicSummary>, String> {
        let bundle = pool.get_or_create(&id)?;
        let client = bundle.admin.inner();
        let meta = client
            .fetch_metadata(None, timeout)
            .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;

        let mut out = Vec::with_capacity(meta.topics().len());
        for t in meta.topics() {
            let name = t.name().to_string();
            let is_internal = name.starts_with("__");
            let partition_count = t.partitions().len() as i32;
            let replication_factor = t
                .partitions()
                .first()
                .map(|p| p.replicas().len() as i32)
                .unwrap_or(0);

            out.push(TopicSummary {
                name,
                partition_count,
                replication_factor,
                is_internal,
                message_count: None,
                size_bytes: None,
            });
        }
        Ok(out)
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    Ok(topics)
}

#[tauri::command]
pub async fn get_topic_detail(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
) -> Result<TopicDetail, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let topic_name = topic.clone();

    let admin_opts_timeout = timeout;

    let detail_partitions = tokio::task::spawn_blocking(move || -> Result<(Vec<PartitionInfo>, String), String> {
        let bundle = pool.get_or_create(&id)?;
        let client = bundle.admin.inner();
        let meta = client
            .fetch_metadata(Some(&topic_name), timeout)
            .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;
        let topic_meta = meta
            .topics()
            .iter()
            .find(|t| t.name() == topic_name)
            .ok_or_else(|| format!("[KAFKA] topic `{topic_name}` not found"))?;

        // Collect partition metadata (leaders, replicas, isr) — no network, instant
        let part_meta: Vec<(i32, i32, Vec<i32>, Vec<i32>)> = topic_meta.partitions()
            .iter()
            .map(|p| (p.id(), p.leader(), p.replicas().to_vec(), p.isr().to_vec()))
            .collect();

        // Parallel watermark fetches — each is an independent broker round-trip
        use rayon::prelude::*;
        let mut partitions: Vec<PartitionInfo> = part_meta
            .par_iter()
            .map(|(id, leader, replicas, isr)| {
                let (low, high) = client
                    .fetch_watermarks(&topic_name, *id, timeout)
                    .unwrap_or((0, 0));
                PartitionInfo {
                    partition_id: *id,
                    leader: *leader,
                    replicas: replicas.clone(),
                    isr: isr.clone(),
                    log_start_offset: low,
                    log_end_offset: high,
                    message_count: (high - low).max(0),
                }
            })
            .collect();
        partitions.sort_by_key(|p| p.partition_id);
        Ok((partitions, topic_name))
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let (partitions, topic_name) = detail_partitions;

    // describe configs (async)
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let topic_for_cfg = topic.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new().request_timeout(Some(admin_opts_timeout));
    let configs = bundle
        .admin
        .describe_configs(&[ResourceSpecifier::Topic(&topic_for_cfg)], &opts)
        .await
        .map_err(|e| format!("[KAFKA-DESCRIBE-CONFIGS] {e}"))?;

    let mut topic_configs: Vec<TopicConfig> = Vec::new();
    for r in configs {
        match r {
            Ok(cr) => {
                for entry in &cr.entries {
                    topic_configs.push(config_entry_to_topic_config(entry));
                }
            }
            Err(err) => {
                return Err(format!("[KAFKA-DESCRIBE-CONFIGS] {err}"));
            }
        }
    }

    Ok(TopicDetail {
        name: topic_name,
        partitions,
        configs: topic_configs,
    })
}

fn config_entry_to_topic_config(entry: &ConfigEntry) -> TopicConfig {
    let is_default = matches!(
        entry.source,
        ConfigSource::Default | ConfigSource::StaticBroker
    );
    TopicConfig {
        name: entry.name.clone(),
        value: entry.value.clone(),
        is_default,
        is_read_only: entry.is_read_only,
    }
}

#[tauri::command]
pub async fn create_topic(
    state: State<'_, AppState>,
    cluster_id: String,
    req: CreateTopicRequest,
) -> Result<serde_json::Value, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));

    let mut new_topic = NewTopic::new(
        &req.name,
        req.partition_count,
        TopicReplication::Fixed(req.replication_factor),
    );
    for (k, v) in &req.configs {
        new_topic = new_topic.set(k, v);
    }

    let results = bundle
        .admin
        .create_topics(std::iter::once(&new_topic), &opts)
        .await
        .map_err(|e| format!("[KAFKA-CREATE-TOPIC] {e}"))?;
    for r in results {
        if let Err((name, err)) = r {
            return Err(format!("[KAFKA-CREATE-TOPIC] {name}: {err}"));
        }
    }
    Ok(serde_json::json!({ "ok": true }))
}

#[tauri::command]
pub async fn delete_topic(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
) -> Result<serde_json::Value, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));
    let results = bundle
        .admin
        .delete_topics(&[&topic], &opts)
        .await
        .map_err(|e| format!("[KAFKA-DELETE-TOPIC] {e}"))?;
    for r in results {
        if let Err((name, err)) = r {
            return Err(format!("[KAFKA-DELETE-TOPIC] {name}: {err}"));
        }
    }
    Ok(serde_json::json!({ "ok": true }))
}

#[tauri::command]
pub async fn update_topic_config(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
    configs: HashMap<String, String>,
) -> Result<serde_json::Value, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new().request_timeout(Some(timeout));

    let mut alter = AlterConfig::new(ResourceSpecifier::Topic(&topic));
    for (k, v) in &configs {
        alter = alter.set(k, v);
    }
    let results = bundle
        .admin
        .alter_configs(std::iter::once(&alter), &opts)
        .await
        .map_err(|e| format!("[KAFKA-ALTER-CONFIGS] {e}"))?;
    for r in results {
        if let Err((spec, err)) = r {
            return Err(format!("[KAFKA-ALTER-CONFIGS] {spec:?}: {err}"));
        }
    }
    Ok(serde_json::json!({ "ok": true }))
}

#[tauri::command]
pub async fn add_partitions(
    state: State<'_, AppState>,
    cluster_id: String,
    topic: String,
    new_count: i32,
) -> Result<serde_json::Value, String> {
    let timeout = timeout_for(&state, &cluster_id)?;
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let bundle = tokio::task::spawn_blocking(move || pool.get_or_create(&id))
        .await
        .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    let opts = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));
    let np = NewPartitions::new(&topic, new_count as usize);
    let results = bundle
        .admin
        .create_partitions(std::iter::once(&np), &opts)
        .await
        .map_err(|e| format!("[KAFKA-CREATE-PARTITIONS] {e}"))?;
    for r in results {
        if let Err((name, err)) = r {
            return Err(format!("[KAFKA-CREATE-PARTITIONS] {name}: {err}"));
        }
    }
    Ok(serde_json::json!({ "ok": true }))
}
