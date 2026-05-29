use crate::config;
use crate::schema_client::SchemaClient;
use crate::types::{SchemaSubject, SchemaVersion};
use crate::AppState;
use serde_json::{json, Value};
use tauri::State;

fn client_for(state: &State<'_, AppState>, cluster_id: &str) -> Result<SchemaClient, String> {
    let cluster = state
        .pool
        .get_config(cluster_id)
        .ok_or_else(|| format!("[CONFIG] cluster `{cluster_id}` not found"))?;
    let password = config::load_sasl_password(cluster_id).ok().flatten();
    SchemaClient::from_cluster(&cluster, password.as_deref())?
        .ok_or_else(|| "[SCHEMA-REGISTRY] schema_registry_url is not configured".to_string())
}

#[tauri::command]
pub async fn list_schema_subjects(
    state: State<'_, AppState>,
    cluster_id: String,
) -> Result<Vec<SchemaSubject>, String> {
    let client = client_for(&state, &cluster_id)?;
    client.list_subjects().await
}

#[tauri::command]
pub async fn get_schema_version(
    state: State<'_, AppState>,
    cluster_id: String,
    subject: String,
    version: String,
) -> Result<SchemaVersion, String> {
    let client = client_for(&state, &cluster_id)?;
    client.get_version(&subject, &version).await
}

#[tauri::command]
pub async fn delete_schema_version(
    state: State<'_, AppState>,
    cluster_id: String,
    subject: String,
    version: String,
) -> Result<Value, String> {
    let client = client_for(&state, &cluster_id)?;
    client.delete_version(&subject, &version).await?;
    Ok(json!({ "ok": true }))
}
