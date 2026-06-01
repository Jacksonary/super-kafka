use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub ssl_ca_cert_path: Option<String>,
    pub ssl_client_cert_path: Option<String>,
    pub ssl_client_key_path: Option<String>,
    pub schema_registry_url: Option<String>,
    pub schema_registry_username: Option<String>,
    pub connect_url: Option<String>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u32,
    pub created_at: u64,
}
fn default_timeout() -> u32 {
    30_000
}

#[derive(Debug, Serialize)]
pub struct ClusterSummary {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: String,
    pub status: String,
    pub broker_count: Option<u32>,
    pub kafka_version: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub is_controller: bool,
}

#[derive(Debug, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub is_internal: bool,
    pub message_count: Option<i64>,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub message_count: i64,
}

#[derive(Debug, Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub configs: Vec<TopicConfig>,
}

#[derive(Debug, Serialize)]
pub struct TopicConfig {
    pub name: String,
    pub value: Option<String>,
    pub is_default: bool,
    pub is_read_only: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub configs: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct KafkaMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub timestamp_type: Option<String>,
    pub key_raw: Option<Vec<u8>>,
    pub key_text: Option<String>,
    pub value_raw: Vec<u8>,
    pub value_text: Option<String>,
    pub value_encoding: String,
    pub headers: Vec<MessageHeader>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FetchMessagesRequest {
    pub cluster_id: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub fetch_mode: serde_json::Value,
    pub limit: i32,
}

#[derive(Debug, Serialize)]
pub struct FetchMessagesResponse {
    pub messages: Vec<KafkaMessage>,
    pub total_fetched: i32,
    pub has_more: bool,
}

#[derive(Debug, Deserialize)]
pub struct ProduceMessageRequest {
    pub cluster_id: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<String>,
    pub value: String,
    pub headers: Vec<MessageHeader>,
}

#[derive(Debug, Serialize)]
pub struct ConsumerGroupSummary {
    pub group_id: String,
    pub state: String,
    pub member_count: i32,
    pub coordinator_id: i32,
    pub protocol_type: String,
    pub total_lag: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ConsumerGroupDetail {
    pub group_id: String,
    pub state: String,
    pub coordinator_id: i32,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<GroupMember>,
    pub topic_lag: Vec<TopicLag>,
}

#[derive(Debug, Serialize)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assigned_partitions: Vec<AssignedPartition>,
}

#[derive(Debug, Serialize)]
pub struct AssignedPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Serialize)]
pub struct TopicLag {
    pub topic: String,
    pub partitions: Vec<PartitionLag>,
    pub total_lag: i64,
}

#[derive(Debug, Serialize)]
pub struct PartitionLag {
    pub partition: i32,
    pub current_offset: i64,
    pub log_end_offset: i64,
    pub lag: i64,
}

#[derive(Debug, Deserialize)]
pub struct ResetOffsetRequest {
    pub cluster_id: String,
    pub group_id: String,
    pub topic: String,
    pub strategy: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct SchemaSubject {
    pub name: String,
    pub version_count: i32,
    pub latest_version: i32,
    pub schema_type: String,
}

#[derive(Debug, Serialize)]
pub struct SchemaVersion {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema_type: String,
    pub schema: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectorSummary {
    pub name: String,
    pub connector_type: String,
    pub state: String,
    pub task_count: i32,
    pub failed_tasks: i32,
    pub connector_class: String,
}

#[derive(Debug, Serialize)]
pub struct ConnectorDetail {
    pub name: String,
    pub connector_type: String,
    pub state: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<ConnectorTask>,
    pub error_trace: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ConnectorTask {
    pub task_id: i32,
    pub state: String,
    pub worker_id: String,
    pub error_trace: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TestConnectionResult {
    pub success: bool,
    pub broker_count: Option<u32>,
    pub kafka_version: Option<String>,
    pub error_message: Option<String>,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub theme: String,
    pub language: String,
    #[serde(default = "default_fetch_limit")]
    pub fetch_limit_default: i32,
    #[serde(default = "default_max_bytes")]
    pub max_message_display_bytes: usize,
}
fn default_fetch_limit() -> i32 {
    100
}
fn default_max_bytes() -> usize {
    1_048_576
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            theme: "light".to_string(),
            language: "zh-CN".to_string(),
            fetch_limit_default: default_fetch_limit(),
            max_message_display_bytes: default_max_bytes(),
        }
    }
}
