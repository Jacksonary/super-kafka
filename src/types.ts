// ── 集群连接 ──────────────────────────────────────────────

export type SaslMechanism =
  | "PLAIN"
  | "SCRAM-SHA-256"
  | "SCRAM-SHA-512"
  | "GSSAPI"   // Kerberos
  | "OAUTHBEARER";

export type SecurityProtocol =
  | "PLAINTEXT"
  | "SSL"
  | "SASL_PLAINTEXT"
  | "SASL_SSL";

/** 存在 YAML 中的集群配置（密码不在此，在 keychain） */
export interface ClusterConfig {
  id: string;                       // uuid v4
  name: string;
  bootstrap_servers: string;        // "host1:9092,host2:9092"
  security_protocol: SecurityProtocol;
  sasl_mechanism: SaslMechanism | null;
  sasl_username: string | null;
  // 密码通过 keyring 存储，key = cluster id
  ssl_ca_cert_path: string | null;
  ssl_client_cert_path: string | null;
  ssl_client_key_path: string | null;
  request_timeout_ms: number;       // default 30000
  created_at: number;               // unix timestamp ms
}

/** 前端展示用（含连接状态） */
export interface ClusterSummary {
  id: string;
  name: string;
  bootstrap_servers: string;
  status: "connected" | "disconnected" | "connecting" | "error";
  broker_count: number | null;
  kafka_version: string | null;
  error_message: string | null;
}

// ── Broker ────────────────────────────────────────────────

export interface BrokerInfo {
  id: number;
  host: string;
  port: number;
  rack: string | null;
  is_controller: boolean;
}

// ── Topic ─────────────────────────────────────────────────

export interface TopicSummary {
  name: string;
  partition_count: number;
  replication_factor: number;
  is_internal: boolean;
  message_count: number | null;     // sum of high-water-marks，近似值
  size_bytes: number | null;        // 如果 broker 不支持则 null
}

export interface PartitionInfo {
  partition_id: number;
  leader: number;                   // broker id
  replicas: number[];
  isr: number[];                    // in-sync replicas
  log_start_offset: number;
  log_end_offset: number;
  message_count: number;            // end - start
}

export interface TopicDetail {
  name: string;
  partitions: PartitionInfo[];
  configs: TopicConfig[];
}

export interface TopicConfig {
  name: string;
  value: string | null;
  is_default: boolean;
  is_read_only: boolean;
}

export interface CreateTopicRequest {
  name: string;
  partition_count: number;
  replication_factor: number;
  configs: Record<string, string>;  // e.g. {"retention.ms": "604800000"}
}

// ── 消息 ──────────────────────────────────────────────────

export type MessageEncoding = "json" | "text" | "binary" | "avro" | "protobuf";

export interface KafkaMessage {
  partition: number;
  offset: number;
  timestamp: number | null;         // unix ms
  timestamp_type: "create_time" | "log_append_time" | null;
  key_raw: number[] | null;         // raw bytes as u8 array
  key_text: string | null;          // UTF-8 decode attempt of key
  value_raw: number[];              // raw bytes
  value_text: string | null;        // UTF-8 decode attempt
  value_encoding: MessageEncoding;  // 后端检测结果
  headers: MessageHeader[];
}

export interface MessageHeader {
  key: string;
  value: string | null;
}

export interface FetchMessagesRequest {
  cluster_id: string;
  topic: string;
  partition: number | null;         // null = 所有分区
  fetch_mode: FetchMode;
  limit: number;                    // max messages to return, default 100
}

export type FetchMode =
  | { type: "latest"; count: number }
  | { type: "from_offset"; partition: number; offset: number }
  | { type: "time_range"; start_ms: number; end_ms: number };

export interface FetchMessagesResponse {
  messages: KafkaMessage[];
  total_fetched: number;
  has_more: boolean;
}

export type CompressionCodec = "none" | "gzip" | "snappy" | "lz4" | "zstd";

export interface ProduceMessageRequest {
  cluster_id: string;
  topic: string;
  partition: number | null;         // null = let Kafka choose
  key: string | null;
  value: string;                    // JSON string or plain text
  headers: MessageHeader[];
  compression: CompressionCodec;
}

// ── Consumer Group ────────────────────────────────────────

export type ConsumerGroupState =
  | "Stable"
  | "Empty"
  | "Dead"
  | "PreparingRebalance"
  | "CompletingRebalance"
  | "Unknown";

export interface ConsumerGroupSummary {
  group_id: string;
  state: ConsumerGroupState;
  member_count: number;
  coordinator_id: number;
  protocol_type: string;
}

export interface ConsumerGroupDetail {
  group_id: string;
  state: ConsumerGroupState;
  coordinator_id: number;
  protocol_type: string;
  protocol: string;
  members: GroupMember[];
}

export interface GroupMember {
  member_id: string;
  client_id: string;
  client_host: string;
  assigned_partitions: AssignedPartition[];
}

export interface AssignedPartition {
  topic: string;
  partition: number;
}

export interface PartitionLag {
  partition: number;
  start_offset: number;
  current_offset: number;
  log_end_offset: number;
  lag: number;
}

// Topic-centric view: a consumer group that consumes a given topic.
export interface TopicConsumerGroup {
  group_id: string;
  state: ConsumerGroupState;
  total_lag: number;
}

export type ResetOffsetStrategy =
  | { type: "earliest" }
  | { type: "latest" }
  | { type: "to_offset"; partition: number; offset: number }
  | { type: "to_timestamp"; timestamp_ms: number };

export interface ResetOffsetRequest {
  cluster_id: string;
  group_id: string;
  topic: string;
  // null/undefined = all partitions; a number = only that partition.
  partition?: number | null;
  strategy: ResetOffsetStrategy;
}

// ── 通用 ─────────────────────────────────────────────────

export interface TestConnectionResult {
  success: boolean;
  broker_count: number | null;
  kafka_version: string | null;
  error_message: string | null;
  latency_ms: number | null;
}

export interface AppConfig {
  theme: "light" | "dark" | "system";
  language: "zh" | "en";
  fetch_limit_default: number;
  max_message_display_bytes: number;
}
