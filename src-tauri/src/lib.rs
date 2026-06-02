pub mod cluster_pool;
pub mod commands;
pub mod config;
pub mod connect_client;
pub mod kafka_client;
pub mod schema_client;
pub mod types;

use std::sync::Arc;

use crate::cluster_pool::ClusterPool;

pub struct AppState {
    pub pool: Arc<ClusterPool>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let pool = Arc::new(ClusterPool::new());
    if let Ok(configs) = config::load_clusters() {
        pool.load_configs(configs);
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_process::init())
        .manage(AppState { pool })
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
            commands::groups::list_consumer_groups,
            commands::groups::get_consumer_group_detail,
            commands::groups::delete_consumer_group,
            commands::groups::reset_offset,
            commands::groups::list_topic_consumer_groups,
            commands::groups::get_topic_group_partition_lag,
            commands::schema::list_schema_subjects,
            commands::schema::get_schema_version,
            commands::schema::delete_schema_version,
            commands::connect::list_connectors,
            commands::connect::get_connector_detail,
            commands::connect::pause_connector,
            commands::connect::resume_connector,
            commands::connect::restart_connector,
            commands::connect::delete_connector,
            commands::connect::upsert_connector,
            commands::settings::get_app_config,
            commands::settings::save_app_config,
            commands::update::check_update,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
