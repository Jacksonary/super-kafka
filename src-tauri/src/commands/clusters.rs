use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use tauri::State;
use tokio::time::timeout as tokio_timeout;

use crate::config;
use crate::kafka_client::{build_client_config, create_bundle};
use crate::types::{BrokerInfo, ClusterConfig, ClusterSummary, TestConnectionResult};
use crate::AppState;

/// Send a raw Kafka ApiVersions request over TCP and return the max version
/// of the Produce API (key=0), which is the most reliable indicator of
/// the broker's Kafka version.
async fn fetch_produce_api_max_version(addr: &str, timeout: Duration) -> Option<i16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // ApiVersions Request v0:
    // [i32 length] [i16 api_key=18] [i16 api_version=0] [i32 correlation_id] [i16 client_id_len=-1]
    let mut req: Vec<u8> = Vec::with_capacity(22);
    let body: Vec<u8> = vec![
        0x00, 0x12, // api_key = 18 (ApiVersions)
        0x00, 0x00, // api_version = 0
        0x00, 0x00, 0x00, 0x01, // correlation_id = 1
        0xFF, 0xFF, // client_id = null (length -1)
    ];
    let body_len = body.len() as i32;
    req.extend_from_slice(&body_len.to_be_bytes());
    req.extend_from_slice(&body);

    let stream = tokio_timeout(timeout, TcpStream::connect(addr)).await.ok()?.ok()?;
    let (mut reader, mut writer) = stream.into_split();

    tokio_timeout(timeout, writer.write_all(&req)).await.ok()?.ok()?;

    // Read response: [i32 total_length] [i32 correlation_id] [i16 error_code] [i32 api_count] [entries...]
    let mut len_buf = [0u8; 4];
    tokio_timeout(timeout, reader.read_exact(&mut len_buf)).await.ok()?.ok()?;
    let total_len = i32::from_be_bytes(len_buf) as usize;
    if total_len < 10 || total_len > 65536 {
        return None;
    }

    let mut body = vec![0u8; total_len];
    tokio_timeout(timeout, reader.read_exact(&mut body)).await.ok()?.ok()?;

    // Skip correlation_id (4 bytes) + error_code (2 bytes)
    if body.len() < 10 {
        return None;
    }
    let error_code = i16::from_be_bytes([body[4], body[5]]);
    if error_code != 0 {
        return None;
    }

    let api_count = i32::from_be_bytes([body[6], body[7], body[8], body[9]]) as usize;
    // Each entry: [i16 api_key] [i16 min_version] [i16 max_version] = 6 bytes
    let entries_start = 10;
    for i in 0..api_count {
        let offset = entries_start + i * 6;
        if offset + 6 > body.len() {
            break;
        }
        let api_key = i16::from_be_bytes([body[offset], body[offset + 1]]);
        let max_version = i16::from_be_bytes([body[offset + 4], body[offset + 5]]);
        if api_key == 0 {
            // Produce API
            return Some(max_version);
        }
    }
    None
}

/// Map Produce API max version to a Kafka version string.
/// Reference: https://kafka.apache.org/protocol#api_versions
fn produce_version_to_kafka(max_version: i16) -> &'static str {
    match max_version {
        12.. => "3.3+",
        11 => "3.2",
        10 => "3.1",
        9  => "2.8",
        8  => "2.4",
        7  => "2.1",
        6  => "2.0",
        5  => "1.0",
        4  => "0.11",
        3  => "0.10",
        2  => "0.9",
        _  => "0.8",
    }
}

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

    // Grab the first broker address for the ApiVersions probe
    let first_broker = cfg
        .bootstrap_servers
        .split(',')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let probe_timeout = timeout.min(Duration::from_secs(5));

    // Run metadata fetch (blocking) and ApiVersions probe (async) concurrently
    let meta_task = tokio::task::spawn_blocking(move || -> (bool, Option<u32>, Option<String>, u64) {
        let start = Instant::now();
        let bundle = match create_bundle(&cfg, pw.as_deref()) {
            Ok(b) => b,
            Err(e) => return (false, None, Some(e), start.elapsed().as_millis() as u64),
        };
        match bundle.admin.inner().fetch_metadata(None, timeout) {
            Ok(meta) => (true, Some(meta.brokers().len() as u32), None, start.elapsed().as_millis() as u64),
            Err(e) => (false, None, Some(format!("[KAFKA-METADATA] {e}")), start.elapsed().as_millis() as u64),
        }
    });

    let version_task = fetch_produce_api_max_version(&first_broker, probe_timeout);

    let (meta_join, produce_ver) =
        tokio::join!(async { meta_task.await.map_err(|e| format!("[RUNTIME] join: {e}")) }, version_task);

    let (success, broker_count, error_message, latency_ms) = meta_join?;
    let kafka_version = produce_ver.map(|v| produce_version_to_kafka(v).to_string());

    Ok(TestConnectionResult {
        success,
        broker_count,
        kafka_version,
        error_message,
        latency_ms: Some(latency_ms),
    })
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
    let probe_timeout = timeout.min(Duration::from_secs(5));
    let pool = state.pool.clone();
    let id = cluster_id.clone();
    let name = cluster.name.clone();
    let bs = cluster.bootstrap_servers.clone();

    let first_broker = bs.split(',').next().unwrap_or("").trim().to_string();

    let meta_task = tokio::task::spawn_blocking(move || match pool.get_or_create(&id) {
        Ok(bundle) => match bundle.admin.inner().fetch_metadata(None, timeout) {
            Ok(meta) => Ok((meta.brokers().len() as u32, None::<String>)),
            Err(e) => Err(format!("[KAFKA-METADATA] {e}")),
        },
        Err(e) => Err(e),
    });

    let version_task = fetch_produce_api_max_version(&first_broker, probe_timeout);

    let (meta_result, produce_ver) = tokio::join!(
        async { meta_task.await.map_err(|e| format!("[RUNTIME] join: {e}")) },
        version_task
    );
    let meta_result = meta_result?;
    let kafka_version = produce_ver.map(|v| produce_version_to_kafka(v).to_string());

    let summary = match meta_result {
        Ok((broker_count, _)) => ClusterSummary {
            id: cluster_id,
            name,
            bootstrap_servers: bs,
            status: "connected".to_string(),
            broker_count: Some(broker_count),
            kafka_version,
            error_message: None,
        },
        Err(e) => ClusterSummary {
            id: cluster_id,
            name,
            bootstrap_servers: bs,
            status: "error".to_string(),
            broker_count: None,
            kafka_version: None,
            error_message: Some(e),
        },
    };

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
