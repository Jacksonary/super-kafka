use crate::connect_client::ConnectClient;
use crate::types::{ConnectorDetail, ConnectorSummary};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use tauri::State;

fn client_for(state: &State<'_, AppState>, cluster_id: &str) -> Result<ConnectClient, String> {
    let cluster = state
        .pool
        .get_config(cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    ConnectClient::from_cluster(&cluster)?
        .ok_or_else(|| "[CONNECT] connect_url is not configured".to_string())
}

#[tauri::command]
pub async fn list_connectors(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<Vec<ConnectorSummary>, String> {
    let client = client_for(&state, &cluster_id)?;
    client.list_connectors().await
}

#[tauri::command]
pub async fn get_connector_detail(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
) -> Result<ConnectorDetail, String> {
    let client = client_for(&state, &cluster_id)?;
    client.get_connector(&name).await
}

#[tauri::command]
pub async fn pause_connector(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.pause(&name).await?;
    Ok(json!({ "ok": true }))
}

#[tauri::command]
pub async fn resume_connector(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.resume(&name).await?;
    Ok(json!({ "ok": true }))
}

#[tauri::command]
pub async fn restart_connector(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.restart(&name).await?;
    Ok(json!({ "ok": true }))
}

#[tauri::command]
pub async fn delete_connector(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.delete(&name).await?;
    Ok(json!({ "ok": true }))
}

#[tauri::command]
pub async fn upsert_connector(
    state: State<'_, AppState>,
    cluster_id: String,
    name: String,
    config: HashMap<String, String>,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.upsert(&name, config).await
}
