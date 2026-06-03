pub mod cluster_pool;
pub mod commands;
pub mod config;
pub mod kafka_client;
pub mod types;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::cluster_pool::ClusterPool;
use crate::types::AppConfig;

pub struct LiveSession {
    pub running: Arc<AtomicBool>,
    #[allow(dead_code)]
    pub handle: std::thread::JoinHandle<()>,
}

pub struct AppState {
    pub pool: Arc<ClusterPool>,
    pub live_sessions: Mutex<HashMap<String, LiveSession>>,
    pub app_config: parking_lot::Mutex<AppConfig>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let pool = Arc::new(ClusterPool::new());
    if let Ok(configs) = config::load_clusters() {
        pool.load_configs(configs);
    }
    let app_config_init = config::load_app_config().unwrap_or_default();

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            use tauri::Manager;
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_process::init())
        .manage(AppState {
            pool,
            live_sessions: Mutex::new(HashMap::new()),
            app_config: parking_lot::Mutex::new(app_config_init),
        })
        .invoke_handler(tauri::generate_handler![
            commands::clusters::list_clusters,
            commands::clusters::save_cluster,
            commands::clusters::delete_cluster,
            commands::clusters::test_connection,
            commands::clusters::save_sasl_password,
            commands::clusters::get_cluster_summary,
            commands::clusters::list_brokers,
            commands::topics::list_topics,
            commands::topics::get_topic_detail,
            commands::topics::create_topic,
            commands::topics::delete_topic,
            commands::topics::update_topic_config,
            commands::topics::add_partitions,
            commands::messages::fetch_messages,
            commands::messages::produce_message,
            commands::messages::start_live_consume,
            commands::messages::stop_live_consume,
            commands::groups::list_consumer_groups,
            commands::groups::get_consumer_group_detail,
            commands::groups::delete_consumer_group,
            commands::groups::reset_offset,
            commands::groups::list_topic_consumer_groups,
            commands::groups::get_topic_group_partition_lag,
            commands::settings::get_app_config,
            commands::settings::save_app_config,
            commands::update::check_update,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
