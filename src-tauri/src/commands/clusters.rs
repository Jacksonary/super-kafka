use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use tauri::State;

use crate::config;
use crate::kafka_client::{build_client_config, create_bundle};
use crate::types::{BrokerInfo, ClusterConfig, ClusterSummary, TestConnectionResult};
use crate::AppState;

#[tauri::command]
pub async fn list_clusters(_state: State<'_, AppState>) -> Result<Vec<ClusterConfig>, String> {
    config::load_clusters()
}

#[tauri::command]
pub async fn save_cluster(
    state: State<'_, AppState>,
    mut config: ClusterConfig,
) -> Result<serde_json::Value, String> {
    if config.id.trim().is_empty() {
        config.id = uuid::Uuid::new_v4().to_string();
    }
    if config.created_at == 0 {
        config.created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
    }

    let mut clusters = config::load_clusters()?;
    if let Some(pos) = clusters.iter().position(|c| c.id == config.id) {
        clusters[pos] = config.clone();
    } else {
        clusters.push(config.clone());
    }
    config::save_clusters(&clusters)?;
    state.pool.upsert_config(config.clone());
    state.pool.invalidate(&config.id);
    Ok(serde_json::json!({ "ok": true, "id": config.id }))
}

#[tauri::command]
pub async fn delete_cluster(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<serde_json::Value, String> {
    let mut clusters = config::load_clusters()?;
    clusters.retain(|c| c.id != cluster_id);
    config::save_clusters(&clusters)?;
    state.pool.remove_config(&cluster_id);
    let _ = config::delete_sasl_password(&cluster_id);
    Ok(serde_json::json!({ "ok": true }))
}

#[tauri::command]
pub async fn test_connection(
    config: ClusterConfig,
    password: Option<String>,
) -> Result<TestConnectionResult, String> {
    let timeout = Duration::from_millis(config.request_timeout_ms as u64);
    let pw = password;
    let cfg = config.clone();

    let result = tokio::task::spawn_blocking(move || -> TestConnectionResult {
        let start = Instant::now();
        let bundle = match create_bundle(&cfg, pw.as_deref()) {
            Ok(b) => b,
            Err(e) => {
                return TestConnectionResult {
                    success: false,
                    broker_count: None,
                    kafka_version: None,
                    error_message: Some(e),
                    latency_ms: Some(start.elapsed().as_millis() as u64),
                }
            }
        };
        match bundle.admin.inner().fetch_metadata(None, timeout) {
            Ok(meta) => TestConnectionResult {
                success: true,
                broker_count: Some(meta.brokers().len() as u32),
                kafka_version: None,
                error_message: None,
                latency_ms: Some(start.elapsed().as_millis() as u64),
            },
            Err(e) => TestConnectionResult {
                success: false,
                broker_count: None,
                kafka_version: None,
                error_message: Some(format!("[KAFKA-METADATA] {e}")),
                latency_ms: Some(start.elapsed().as_millis() as u64),
            },
        }
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?;

    Ok(result)
}

#[tauri::command]
pub async fn save_sasl_password(
    cluster_id: String,
    password: String,
) -> Result<serde_json::Value, String> {
    config::save_sasl_password(&cluster_id, &password)?;
    Ok(serde_json::json!({ "ok": true }))
}

#[tauri::command]
pub async fn get_cluster_summary(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<ClusterSummary, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .or_else(|| {
            config::load_clusters()
                .ok()
                .and_then(|cs| cs.into_iter().find(|c| c.id == cluster_id))
        })
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;

    state.pool.upsert_config(cluster.clone());

    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let name = cluster.name.clone();
    let bs = cluster.bootstrap_servers.clone();

    let summary = tokio::task::spawn_blocking(move || match pool.get_or_create(&id) {
        Ok(bundle) => match bundle.admin.inner().fetch_metadata(None, timeout) {
            Ok(meta) => ClusterSummary {
                id: id.clone(),
                name: name.clone(),
                bootstrap_servers: bs.clone(),
                status: "connected".to_string(),
                broker_count: Some(meta.brokers().len() as u32),
                kafka_version: None,
                error_message: None,
            },
            Err(e) => ClusterSummary {
                id: id.clone(),
                name: name.clone(),
                bootstrap_servers: bs.clone(),
                status: "error".to_string(),
                broker_count: None,
                kafka_version: None,
                error_message: Some(format!("[KAFKA-METADATA] {e}")),
            },
        },
        Err(e) => ClusterSummary {
            id: id.clone(),
            name: name.clone(),
            bootstrap_servers: bs.clone(),
            status: "error".to_string(),
            broker_count: None,
            kafka_version: None,
            error_message: Some(e),
        },
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))?;

    Ok(summary)
}

#[tauri::command]
pub async fn list_brokers(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<Vec<BrokerInfo>, String> {
    let cluster = state
        .pool
        .get_config(&cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let timeout = Duration::from_millis(cluster.request_timeout_ms as u64);
    let pool = state.pool.clone();
    let id = cluster_id.clone();

    let pw = config::load_sasl_password(&cluster_id).ok().flatten();
    let mut consumer_cfg = build_client_config(&cluster, pw.as_deref());
    consumer_cfg.set("group.id", format!("super-kafka-meta-{}", uuid::Uuid::new_v4()));

    let brokers = tokio::task::spawn_blocking(move || -> Result<Vec<BrokerInfo>, String> {
        let bundle = pool.get_or_create(&id)?;
        let meta = bundle
            .admin
            .inner()
            .fetch_metadata(None, timeout)
            .map_err(|e| format!("[KAFKA-METADATA] {e}"))?;

        let controller_id: i32 = match consumer_cfg.create::<BaseConsumer>() {
            Ok(c) => c
                .fetch_metadata(None, timeout)
                .map(|m| m.orig_broker_id())
                .unwrap_or(-1),
            Err(_) => -1,
        };

        Ok(meta
            .brokers()
            .iter()
            .map(|b| BrokerInfo {
                id: b.id(),
                host: b.host().to_string(),
                port: b.port(),
                rack: None,
                is_controller: b.id() == controller_id,
            })
            .collect())
    })
    .await
    .map_err(|e| format!("[RUNTIME] join: {e}"))??;

    Ok(brokers)
}
