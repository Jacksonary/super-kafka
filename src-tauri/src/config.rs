use crate::types::{AppConfig, ClusterConfig};
use std::fs;
use std::path::PathBuf;

const APP_DIR: &str = "super-kafka";
const CLUSTERS_FILE: &str = "clusters.yaml";
const APP_CONFIG_FILE: &str = "app.yaml";

fn config_dir() -> Result<PathBuf, String> {
    let base = dirs::config_dir()
        .ok_or_else(|| "[CONFIG] cannot resolve user config directory".to_string())?;
    let dir = base.join(APP_DIR);
    if !dir.exists() {
        fs::create_dir_all(&dir)
            .map_err(|e| format!("[CONFIG] failed to create config dir: {e}"))?;
    }
    Ok(dir)
}

fn clusters_path() -> Result<PathBuf, String> {
    Ok(config_dir()?.join(CLUSTERS_FILE))
}

fn app_config_path() -> Result<PathBuf, String> {
    Ok(config_dir()?.join(APP_CONFIG_FILE))
}

pub fn load_clusters() -> Result<Vec<ClusterConfig>, String> {
    let path = clusters_path()?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(&path)
        .map_err(|e| format!("[CONFIG] failed to read {}: {e}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_yaml::from_str::<Vec<ClusterConfig>>(&raw)
        .map_err(|e| format!("[CONFIG] failed to parse clusters.yaml: {e}"))
}

pub fn save_clusters(configs: &[ClusterConfig]) -> Result<(), String> {
    let path = clusters_path()?;
    let raw = serde_yaml::to_string(configs)
        .map_err(|e| format!("[CONFIG] failed to serialize clusters: {e}"))?;
    fs::write(&path, raw)
        .map_err(|e| format!("[CONFIG] failed to write {}: {e}", path.display()))?;
    Ok(())
}

pub fn load_app_config() -> Result<AppConfig, String> {
    let path = app_config_path()?;
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let raw = fs::read_to_string(&path)
        .map_err(|e| format!("[CONFIG] failed to read {}: {e}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(AppConfig::default());
    }
    serde_yaml::from_str::<AppConfig>(&raw)
        .map_err(|e| format!("[CONFIG] failed to parse app.yaml: {e}"))
}

pub fn save_app_config(config: &AppConfig) -> Result<(), String> {
    let path = app_config_path()?;
    let raw = serde_yaml::to_string(config)
        .map_err(|e| format!("[CONFIG] failed to serialize app config: {e}"))?;
    fs::write(&path, raw)
        .map_err(|e| format!("[CONFIG] failed to write {}: {e}", path.display()))?;
    Ok(())
}

const KEYRING_SERVICE: &str = "super-kafka";

pub fn save_sasl_password(cluster_id: &str, password: &str) -> Result<(), String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, cluster_id)
        .map_err(|e| format!("[CONFIG] keyring entry: {e}"))?;
    entry
        .set_password(password)
        .map_err(|e| format!("[CONFIG] keyring set_password: {e}"))
}

pub fn load_sasl_password(cluster_id: &str) -> Result<Option<String>, String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, cluster_id)
        .map_err(|e| format!("[CONFIG] keyring entry: {e}"))?;
    match entry.get_password() {
        Ok(p) => Ok(Some(p)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(format!("[CONFIG] keyring get_password: {e}")),
    }
}

pub fn delete_sasl_password(cluster_id: &str) -> Result<(), String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, cluster_id)
        .map_err(|e| format!("[CONFIG] keyring entry: {e}"))?;
    match entry.delete_credential() {
        Ok(_) => Ok(()),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(format!("[CONFIG] keyring delete: {e}")),
    }
}
