import { invoke } from "@tauri-apps/api/core";
import type * as T from "./types";

// ─────────────────────────────────────────────────────────
// Mock helpers
// ─────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function utf8Bytes(s: string): number[] {
  return Array.from(new TextEncoder().encode(s));
}

// ─────────────────────────────────────────────────────────
// Mock data: clusters
// ─────────────────────────────────────────────────────────

const MOCK_CLUSTERS: T.ClusterConfig[] = [
  {
    id: "cluster-1",
    name: "Local Dev",
    bootstrap_servers: "localhost:9092",
    security_protocol: "PLAINTEXT",
    sasl_mechanism: null,
    sasl_username: null,
    ssl_ca_cert_path: null,
    ssl_client_cert_path: null,
    ssl_client_key_path: null,
    request_timeout_ms: 30000,
    created_at: Date.now() - 1000 * 60 * 60 * 24 * 30,
  },
  {
    id: "cluster-2",
    name: "Staging (SASL_SSL)",
    bootstrap_servers: "kafka-stg-1.example.com:9093,kafka-stg-2.example.com:9093,kafka-stg-3.example.com:9093",
    security_protocol: "SASL_SSL",
    sasl_mechanism: "SCRAM-SHA-512",
    sasl_username: "stg-app",
    ssl_ca_cert_path: "/etc/ssl/certs/stg-ca.pem",
    ssl_client_cert_path: null,
    ssl_client_key_path: null,
    request_timeout_ms: 30000,
    created_at: Date.now() - 1000 * 60 * 60 * 24 * 7,
  },
  {
    id: "cluster-3",
    name: "Production",
    bootstrap_servers: "kafka-prod-1.example.com:9093,kafka-prod-2.example.com:9093,kafka-prod-3.example.com:9093,kafka-prod-4.example.com:9093,kafka-prod-5.example.com:9093",
    security_protocol: "SSL",
    sasl_mechanism: null,
    sasl_username: null,
    ssl_ca_cert_path: "/etc/ssl/certs/prod-ca.pem",
    ssl_client_cert_path: "/etc/ssl/certs/prod-client.pem",
    ssl_client_key_path: "/etc/ssl/private/prod-client.key",
    request_timeout_ms: 60000,
    created_at: Date.now() - 1000 * 60 * 60 * 24 * 90,
  },
];

let clusters: T.ClusterConfig[] = [...MOCK_CLUSTERS];

const CLUSTER_SUMMARIES: Record<string, T.ClusterSummary> = {
  "cluster-1": {
    id: "cluster-1",
    name: "Local Dev",
    bootstrap_servers: "localhost:9092",
    status: "connected",
    broker_count: 1,
    kafka_version: "3.7.0",
    error_message: null,
  },
  "cluster-2": {
    id: "cluster-2",
    name: "Staging (SASL_SSL)",
    bootstrap_servers: "kafka-stg-1.example.com:9093,...",
    status: "connected",
    broker_count: 3,
    kafka_version: "3.6.1",
    error_message: null,
  },
  "cluster-3": {
    id: "cluster-3",
    name: "Production",
    bootstrap_servers: "kafka-prod-1.example.com:9093,...",
    status: "error",
    broker_count: null,
    kafka_version: null,
    error_message: "[KAFKA-CONN] SSL handshake failed: client cert expired",
  },
};

// ─────────────────────────────────────────────────────────
// Mock data: brokers
// ─────────────────────────────────────────────────────────

const BROKERS_BY_CLUSTER: Record<string, T.BrokerInfo[]> = {
  "cluster-1": [
    { id: 1, host: "localhost", port: 9092, rack: null, is_controller: true },
  ],
  "cluster-2": [
    { id: 1, host: "kafka-stg-1.example.com", port: 9093, rack: "rack-a", is_controller: true },
    { id: 2, host: "kafka-stg-2.example.com", port: 9093, rack: "rack-b", is_controller: false },
    { id: 3, host: "kafka-stg-3.example.com", port: 9093, rack: "rack-c", is_controller: false },
  ],
  "cluster-3": [],
};

// ─────────────────────────────────────────────────────────
// Mock data: topics
// ─────────────────────────────────────────────────────────

const TOPICS_BY_CLUSTER: Record<string, T.TopicSummary[]> = {
  "cluster-1": [
    { name: "user-events", partition_count: 3, replication_factor: 1, is_internal: false, message_count: 12_543, size_bytes: 4_982_124 },
    { name: "order-updates", partition_count: 6, replication_factor: 1, is_internal: false, message_count: 38_217, size_bytes: 18_322_900 },
    { name: "payment-tx", partition_count: 4, replication_factor: 1, is_internal: false, message_count: 904, size_bytes: 612_300 },
    { name: "audit-log", partition_count: 2, replication_factor: 1, is_internal: false, message_count: 87_400, size_bytes: 32_001_120 },
    { name: "click-stream", partition_count: 8, replication_factor: 1, is_internal: false, message_count: 1_234_567, size_bytes: 412_982_320 },
    { name: "__consumer_offsets", partition_count: 50, replication_factor: 1, is_internal: true, message_count: 2310, size_bytes: 145_022 },
    { name: "_schemas", partition_count: 1, replication_factor: 1, is_internal: true, message_count: 42, size_bytes: 18_330 },
  ],
  "cluster-2": [
    { name: "user-events", partition_count: 12, replication_factor: 3, is_internal: false, message_count: 8_421_092, size_bytes: 3_120_882_134 },
    { name: "order-updates", partition_count: 12, replication_factor: 3, is_internal: false, message_count: 12_400_330, size_bytes: 5_882_134_000 },
    { name: "payment-tx", partition_count: 8, replication_factor: 3, is_internal: false, message_count: 542_334, size_bytes: 312_887_200 },
    { name: "notifications", partition_count: 6, replication_factor: 3, is_internal: false, message_count: 2_983_400, size_bytes: 884_120_300 },
    { name: "user-profile-cdc", partition_count: 4, replication_factor: 3, is_internal: false, message_count: 18_223, size_bytes: 22_980_300 },
    { name: "dlq.user-events", partition_count: 1, replication_factor: 3, is_internal: false, message_count: 13, size_bytes: 8_120 },
    { name: "__consumer_offsets", partition_count: 50, replication_factor: 3, is_internal: true, message_count: 982_330, size_bytes: 84_002_120 },
    { name: "_schemas", partition_count: 1, replication_factor: 3, is_internal: true, message_count: 312, size_bytes: 412_990 },
  ],
  "cluster-3": [],
};

const DEFAULT_TOPIC_CONFIGS: T.TopicConfig[] = [
  { name: "cleanup.policy", value: "delete", is_default: true, is_read_only: false },
  { name: "retention.ms", value: "604800000", is_default: true, is_read_only: false },
  { name: "retention.bytes", value: "-1", is_default: true, is_read_only: false },
  { name: "segment.bytes", value: "1073741824", is_default: true, is_read_only: false },
  { name: "max.message.bytes", value: "1048588", is_default: true, is_read_only: false },
  { name: "compression.type", value: "producer", is_default: true, is_read_only: false },
  { name: "min.insync.replicas", value: "1", is_default: true, is_read_only: false },
  { name: "message.timestamp.type", value: "CreateTime", is_default: true, is_read_only: false },
];

function buildTopicDetail(topic: T.TopicSummary): T.TopicDetail {
  const partitions: T.PartitionInfo[] = Array.from({ length: topic.partition_count }, (_, i) => {
    const start = Math.floor(Math.random() * 100);
    const total = topic.message_count ?? 1000;
    const perPart = Math.floor(total / Math.max(1, topic.partition_count));
    const end = start + perPart + Math.floor(Math.random() * 50);
    const replicas: number[] = [];
    for (let r = 0; r < topic.replication_factor; r++) {
      replicas.push(((i + r) % 3) + 1);
    }
    return {
      partition_id: i,
      leader: replicas[0]!,
      replicas,
      isr: replicas,
      log_start_offset: start,
      log_end_offset: end,
      message_count: end - start,
    };
  });

  const configs: T.TopicConfig[] = DEFAULT_TOPIC_CONFIGS.map((c) => ({ ...c }));
  if (topic.name === "audit-log") {
    const r = configs.find((c) => c.name === "retention.ms");
    if (r) {
      r.value = "2592000000";
      r.is_default = false;
    }
  }
  if (topic.name.startsWith("dlq.")) {
    const r = configs.find((c) => c.name === "retention.ms");
    if (r) {
      r.value = "1209600000";
      r.is_default = false;
    }
  }

  return { name: topic.name, partitions, configs };
}

function buildMockMessages(topic: string, partitionFilter: number | null, limit: number): T.KafkaMessage[] {
  const samples: { value: unknown; encoding: T.MessageEncoding }[] = [
    { value: { user_id: "u-1024", event: "login", ts: Date.now() - 5000 }, encoding: "json" },
    { value: { order_id: "ord-9981", amount: 19.99, currency: "USD", items: [{ sku: "A-1", qty: 2 }] }, encoding: "json" },
    { value: "raw text payload from sensor 42", encoding: "text" },
    { value: { user_id: "u-2048", event: "purchase", amount: 199, items: 3 }, encoding: "json" },
    { value: { error: "timeout", retry: 3, host: "api-7" }, encoding: "json" },
  ];

  const out: T.KafkaMessage[] = [];
  const now = Date.now();
  const partitions = partitionFilter == null ? [0, 1, 2] : [partitionFilter];
  for (let i = 0; i < limit; i++) {
    const s = samples[i % samples.length]!;
    const partition = partitions[i % partitions.length]!;
    const valueStr = s.encoding === "json" ? JSON.stringify(s.value) : String(s.value);
    const keyStr = `${topic}-key-${i}`;
    out.push({
      partition,
      offset: 100_000 + i,
      timestamp: now - (limit - i) * 1500,
      timestamp_type: "create_time",
      key_raw: utf8Bytes(keyStr),
      key_text: keyStr,
      value_raw: utf8Bytes(valueStr),
      value_text: valueStr,
      value_encoding: s.encoding,
      headers: [
        { key: "trace-id", value: `trc-${1000 + i}` },
        { key: "source", value: "service-api" },
      ],
    });
  }
  return out;
}

const GROUPS_BY_CLUSTER: Record<string, T.ConsumerGroupSummary[]> = {
  "cluster-1": [
    { group_id: "order-processor", state: "Stable", member_count: 3, coordinator_id: 1, protocol_type: "consumer", total_lag: 1240 },
    { group_id: "audit-sink", state: "Stable", member_count: 1, coordinator_id: 1, protocol_type: "consumer", total_lag: 0 },
    { group_id: "legacy-batch", state: "Empty", member_count: 0, coordinator_id: 1, protocol_type: "consumer", total_lag: 980_000 },
  ],
  "cluster-2": [
    { group_id: "user-events-flink", state: "Stable", member_count: 6, coordinator_id: 2, protocol_type: "consumer", total_lag: 32_410 },
    { group_id: "notifications-worker", state: "PreparingRebalance", member_count: 4, coordinator_id: 1, protocol_type: "consumer", total_lag: 5_982 },
    { group_id: "payment-tx-archiver", state: "Stable", member_count: 2, coordinator_id: 3, protocol_type: "consumer", total_lag: 0 },
    { group_id: "dead-pipeline", state: "Dead", member_count: 0, coordinator_id: 2, protocol_type: "consumer", total_lag: null },
  ],
  "cluster-3": [],
};

function buildGroupDetail(clusterId: string, groupId: string): T.ConsumerGroupDetail {
  const summary = (GROUPS_BY_CLUSTER[clusterId] ?? []).find((g) => g.group_id === groupId);
  const members: T.GroupMember[] = summary && summary.member_count > 0
    ? Array.from({ length: summary.member_count }, (_, i) => ({
        member_id: `${groupId}-member-${i}-${Math.random().toString(36).slice(2, 8)}`,
        client_id: `${groupId}-${i}`,
        client_host: `/10.0.${i + 1}.${10 + i}`,
        assigned_partitions: [
          { topic: "user-events", partition: i },
          { topic: "order-updates", partition: i * 2 },
        ],
      }))
    : [];

  const topic_lag: T.TopicLag[] = [
    {
      topic: "user-events",
      total_lag: 800,
      partitions: [0, 1, 2].map((p) => ({
        partition: p,
        start_offset: 0,
        current_offset: 10_000 + p * 100,
        log_end_offset: 10_300 + p * 100,
        lag: 300 - p * 50,
      })),
    },
    {
      topic: "order-updates",
      total_lag: 440,
      partitions: [0, 1, 2, 3].map((p) => ({
        partition: p,
        start_offset: 0,
        current_offset: 50_000 + p * 200,
        log_end_offset: 50_110 + p * 200,
        lag: 110,
      })),
    },
  ];

  return {
    group_id: groupId,
    state: summary?.state ?? "Unknown",
    coordinator_id: summary?.coordinator_id ?? 1,
    protocol_type: summary?.protocol_type ?? "consumer",
    protocol: "range",
    members,
    topic_lag,
  };
}

let appConfig: T.AppConfig = {
  theme: "dark",
  language: "zh",
  fetch_limit_default: 100,
  max_message_display_bytes: 1_048_576,
};

const mockApi = {
  async listClusters(): Promise<T.ClusterConfig[]> {
    await sleep(50);
    return [...clusters];
  },

  async saveCluster(config: T.ClusterConfig): Promise<{ ok: boolean }> {
    await sleep(80);
    const idx = clusters.findIndex((c) => c.id === config.id);
    if (idx >= 0) {
      clusters[idx] = config;
    } else {
      clusters.push(config);
    }
    return { ok: true };
  },

  async deleteCluster(clusterId: string): Promise<{ ok: boolean }> {
    await sleep(80);
    clusters = clusters.filter((c) => c.id !== clusterId);
    return { ok: true };
  },

  async testConnection(config: T.ClusterConfig, _password: string | null): Promise<T.TestConnectionResult> {
    await sleep(700);
    const fail = config.name.toLowerCase().includes("fail");
    if (fail) {
      return {
        success: false,
        broker_count: null,
        kafka_version: null,
        error_message: "[KAFKA-CONN] Connection refused (mock)",
        latency_ms: null,
      };
    }
    return {
      success: true,
      broker_count: config.bootstrap_servers.split(",").length || 1,
      kafka_version: "3.7.0",
      error_message: null,
      latency_ms: 38,
    };
  },

  async saveSaslPassword(_clusterId: string, _password: string): Promise<{ ok: boolean }> {
    await sleep(40);
    return { ok: true };
  },

  async getClusterSummary(clusterId: string): Promise<T.ClusterSummary> {
    await sleep(60);
    const summary = CLUSTER_SUMMARIES[clusterId];
    if (summary) return { ...summary };
    const cfg = clusters.find((c) => c.id === clusterId);
    return {
      id: clusterId,
      name: cfg?.name ?? clusterId,
      bootstrap_servers: cfg?.bootstrap_servers ?? "",
      status: "disconnected",
      broker_count: null,
      kafka_version: null,
      error_message: null,
    };
  },

  async listBrokers(clusterId: string): Promise<T.BrokerInfo[]> {
    await sleep(80);
    return BROKERS_BY_CLUSTER[clusterId] ?? [];
  },

  async listTopics(clusterId: string): Promise<T.TopicSummary[]> {
    await sleep(120);
    return [...(TOPICS_BY_CLUSTER[clusterId] ?? [])];
  },

  async getTopicDetail(clusterId: string, topic: string): Promise<T.TopicDetail> {
    await sleep(100);
    const t = (TOPICS_BY_CLUSTER[clusterId] ?? []).find((x) => x.name === topic);
    if (!t) {
      throw new Error(`[MOCK] Topic not found: ${topic}`);
    }
    return buildTopicDetail(t);
  },

  async createTopic(clusterId: string, req: T.CreateTopicRequest): Promise<{ ok: boolean }> {
    await sleep(150);
    const list = TOPICS_BY_CLUSTER[clusterId] ?? (TOPICS_BY_CLUSTER[clusterId] = []);
    if (list.some((t) => t.name === req.name)) {
      throw new Error(`[MOCK] Topic already exists: ${req.name}`);
    }
    list.push({
      name: req.name,
      partition_count: req.partition_count,
      replication_factor: req.replication_factor,
      is_internal: req.name.startsWith("_"),
      message_count: 0,
      size_bytes: 0,
    });
    return { ok: true };
  },

  async deleteTopic(clusterId: string, topic: string): Promise<{ ok: boolean }> {
    await sleep(150);
    const list = TOPICS_BY_CLUSTER[clusterId];
    if (list) {
      TOPICS_BY_CLUSTER[clusterId] = list.filter((t) => t.name !== topic);
    }
    return { ok: true };
  },

  async updateTopicConfig(_clusterId: string, _topic: string, _configs: Record<string, string>): Promise<{ ok: boolean }> {
    await sleep(120);
    return { ok: true };
  },

  async addPartitions(clusterId: string, topic: string, newPartitionCount: number): Promise<{ ok: boolean }> {
    await sleep(120);
    const list = TOPICS_BY_CLUSTER[clusterId];
    if (list) {
      const t = list.find((x) => x.name === topic);
      if (t && newPartitionCount > t.partition_count) {
        t.partition_count = newPartitionCount;
      }
    }
    return { ok: true };
  },

  async fetchMessages(req: T.FetchMessagesRequest): Promise<T.FetchMessagesResponse> {
    await sleep(200);
    const messages = buildMockMessages(req.topic, req.partition, Math.min(req.limit, 200));
    return {
      messages,
      total_fetched: messages.length,
      has_more: messages.length >= req.limit,
    };
  },

  async produceMessage(req: T.ProduceMessageRequest): Promise<{ partition: number; offset: number }> {
    await sleep(120);
    return {
      partition: req.partition ?? Math.floor(Math.random() * 4),
      offset: 100_000 + Math.floor(Math.random() * 1000),
    };
  },

  async listConsumerGroups(clusterId: string): Promise<T.ConsumerGroupSummary[]> {
    await sleep(120);
    return [...(GROUPS_BY_CLUSTER[clusterId] ?? [])];
  },

  async getConsumerGroupDetail(clusterId: string, groupId: string): Promise<T.ConsumerGroupDetail> {
    await sleep(100);
    return buildGroupDetail(clusterId, groupId);
  },

  async deleteConsumerGroup(clusterId: string, groupId: string): Promise<{ ok: boolean }> {
    await sleep(120);
    const list = GROUPS_BY_CLUSTER[clusterId];
    if (list) {
      GROUPS_BY_CLUSTER[clusterId] = list.filter((g) => g.group_id !== groupId);
    }
    return { ok: true };
  },

  async resetOffset(_req: T.ResetOffsetRequest): Promise<{ ok: boolean }> {
    await sleep(120);
    return { ok: true };
  },

  async listTopicConsumerGroups(
    clusterId: string,
    _topic: string,
  ): Promise<T.TopicConsumerGroup[]> {
    await sleep(120);
    const groups = GROUPS_BY_CLUSTER[clusterId] ?? [];
    return groups.map((g) => ({ group_id: g.group_id, state: g.state, total_lag: 0 }));
  },

  async getTopicGroupPartitionLag(
    _clusterId: string,
    _topic: string,
    _groupId: string,
  ): Promise<T.PartitionLag[]> {
    await sleep(100);
    return [
      { partition: 0, start_offset: 0, current_offset: 50, log_end_offset: 100, lag: 50 },
    ];
  },

  async getAppConfig(): Promise<T.AppConfig> {
    await sleep(20);
    return { ...appConfig };
  },

  async saveAppConfig(config: T.AppConfig): Promise<{ ok: boolean }> {
    await sleep(40);
    appConfig = { ...config };
    return { ok: true };
  },

  async checkUpdate(): Promise<{ latest_version: string; release_url: string }> {
    await sleep(80);
    return {
      latest_version: "0.1.0",
      release_url: "https://github.com/example/super-kafka/releases/latest",
    };
  },
};

type UnknownRecord = Record<string, unknown>;

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function normalizeError(err: unknown): Error {
  if (err instanceof Error) return err;
  if (typeof err === "string") return new Error(err);
  try {
    return new Error(JSON.stringify(err));
  } catch {
    return new Error(String(err));
  }
}

async function tauriInvoke<R>(command: string, args?: UnknownRecord): Promise<R> {
  try {
    return await invoke<R>(command, args);
  } catch (err) {
    throw normalizeError(err);
  }
}

function mapTimestampType(v: unknown): T.KafkaMessage["timestamp_type"] {
  if (v === "CreateTime" || v === "create_time") return "create_time";
  if (v === "LogAppendTime" || v === "log_append_time") return "log_append_time";
  return null;
}

function mapEncoding(v: unknown): T.MessageEncoding {
  if (v === "json") return "json";
  if (v === "binary") return "binary";
  if (v === "avro") return "avro";
  if (v === "protobuf") return "protobuf";
  return "text";
}

function mapFetchMessagesResponse(raw: T.FetchMessagesResponse): T.FetchMessagesResponse {
  return {
    ...raw,
    messages: raw.messages.map((m) => ({
      ...m,
      timestamp_type: mapTimestampType(m.timestamp_type),
      value_encoding: mapEncoding(m.value_encoding),
      headers: (m.headers ?? []).map((h) => ({
        key: h.key,
        value: h.value ?? null,
      })),
    })),
  };
}

function toBackendResetOffsetRequest(req: T.ResetOffsetRequest): UnknownRecord {
  const s = req.strategy;
  if (s.type === "to_offset") {
    return {
      ...req,
      strategy: {
        type: "specific",
        offset: s.offset,
        partition: s.partition,
      },
    };
  }
  if (s.type === "to_timestamp") {
    return {
      ...req,
      strategy: {
        type: "timestamp",
        timestamp: s.timestamp_ms,
      },
    };
  }
  return req as unknown as UnknownRecord;
}

function toBackendFetchMode(mode: T.FetchMode): UnknownRecord {
  if (mode.type === "time_range") {
    return {
      type: "from_timestamp",
      timestamp: mode.start_ms,
      end_ms: mode.end_ms,
    };
  }
  return mode;
}

function normalizeAppConfig(cfg: T.AppConfig): T.AppConfig {
  const languageRaw = String((cfg as unknown as UnknownRecord).language ?? "zh").toLowerCase();
  const themeRaw = String((cfg as unknown as UnknownRecord).theme ?? "system").toLowerCase();
  return {
    theme: themeRaw === "dark" || themeRaw === "light" || themeRaw === "system" ? themeRaw : "system",
    language: languageRaw.startsWith("en") ? "en" : "zh",
    fetch_limit_default: Number((cfg as unknown as UnknownRecord).fetch_limit_default ?? 100),
    max_message_display_bytes: Number((cfg as unknown as UnknownRecord).max_message_display_bytes ?? 1_048_576),
  };
}

const realApi: typeof mockApi = {
  async listClusters() {
    return tauriInvoke("list_clusters");
  },

  async saveCluster(config) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_cluster", { config });
    return { ok: r.ok ?? true };
  },

  async deleteCluster(clusterId) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_cluster", { clusterId });
    return { ok: r.ok ?? true };
  },

  async testConnection(config, password) {
    return tauriInvoke("test_connection", { config, password });
  },

  async saveSaslPassword(clusterId, password) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_sasl_password", { clusterId, password });
    return { ok: r.ok ?? true };
  },

  async getClusterSummary(clusterId) {
    return tauriInvoke("get_cluster_summary", { clusterId });
  },

  async listBrokers(clusterId) {
    return tauriInvoke("list_brokers", { clusterId });
  },

  async listTopics(clusterId) {
    return tauriInvoke("list_topics", { clusterId });
  },

  async getTopicDetail(clusterId, topic) {
    return tauriInvoke("get_topic_detail", { clusterId, topic });
  },

  async createTopic(clusterId, req) {
    const r = await tauriInvoke<{ ok?: boolean }>("create_topic", { clusterId, req });
    return { ok: r.ok ?? true };
  },

  async deleteTopic(clusterId, topic) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_topic", { clusterId, topic });
    return { ok: r.ok ?? true };
  },

  async updateTopicConfig(clusterId, topic, configs) {
    const r = await tauriInvoke<{ ok?: boolean }>("update_topic_config", { clusterId, topic, configs });
    return { ok: r.ok ?? true };
  },

  async addPartitions(clusterId, topic, newPartitionCount) {
    const r = await tauriInvoke<{ ok?: boolean }>("add_partitions", {
      clusterId,
      topic,
      newCount: newPartitionCount,
    });
    return { ok: r.ok ?? true };
  },

  async fetchMessages(req) {
    const raw = await tauriInvoke<T.FetchMessagesResponse>("fetch_messages", {
      req: {
        ...req,
        fetch_mode: toBackendFetchMode(req.fetch_mode),
      },
    });
    return mapFetchMessagesResponse(raw);
  },

  async produceMessage(req) {
    await tauriInvoke<{ ok?: boolean }>("produce_message", { req });
    return {
      partition: req.partition ?? -1,
      offset: -1,
    };
  },

  async listConsumerGroups(clusterId) {
    return tauriInvoke("list_consumer_groups", { clusterId });
  },

  async getConsumerGroupDetail(clusterId, groupId) {
    return tauriInvoke("get_consumer_group_detail", { clusterId, groupId });
  },

  async deleteConsumerGroup(clusterId, groupId) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_consumer_group", { clusterId, groupId });
    return { ok: r.ok ?? true };
  },

  async resetOffset(req) {
    const r = await tauriInvoke<{ ok?: boolean }>("reset_offset", {
      req: toBackendResetOffsetRequest(req),
    });
    return { ok: r.ok ?? true };
  },

  async listTopicConsumerGroups(clusterId, topic) {
    return tauriInvoke("list_topic_consumer_groups", { clusterId, topic });
  },

  async getTopicGroupPartitionLag(clusterId, topic, groupId) {
    return tauriInvoke("get_topic_group_partition_lag", { clusterId, topic, groupId });
  },

  async getAppConfig() {
    const raw = await tauriInvoke<T.AppConfig>("get_app_config");
    return normalizeAppConfig(raw);
  },

  async saveAppConfig(config) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_app_config", { config });
    return { ok: r.ok ?? true };
  },

  async checkUpdate() {
    return tauriInvoke("check_update");
  },
};

export const api: typeof mockApi = isTauriRuntime() ? realApi : mockApi;
