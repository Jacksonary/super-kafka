use serde_json::{json, Value};

#[tauri::command]
pub async fn check_update() -> Result<Value, String> {
    // Stub: tauri-plugin-updater drives the actual updater flow on the JS side.
    // This command exposes a hook for future server-side update metadata if needed.
    Ok(json!({
        "latest_version": env!("CARGO_PKG_VERSION"),
        "release_url": "https://github.com/Jacksonary/super-kafka/releases/latest"
    }))
}
