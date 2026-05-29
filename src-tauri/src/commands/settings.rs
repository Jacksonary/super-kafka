use crate::config;
use crate::types::AppConfig;
use crate::AppState;
use serde_json::{json, Value};
use tauri::State;

#[tauri::command]
pub async fn get_app_config(_state: State<'_, AppState>) -> Result<AppConfig, String> {
    config::load_app_config()
}

#[tauri::command]
pub async fn save_app_config(
    _state: State<'_, AppState>,
    config: AppConfig,
) -> Result<Value, String> {
    crate::config::save_app_config(&config)?;
    Ok(json!({ "ok": true }))
}
