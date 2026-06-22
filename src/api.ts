import { invoke } from "@tauri-apps/api/core";
import type * as T from "./types";

type UnknownRecord = Record<string, unknown>;

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
  const raw = cfg as unknown as UnknownRecord;
  const languageRaw = String(raw.language ?? "en").toLowerCase();
  const themeRaw = String(raw.theme ?? "dark").toLowerCase();
  return {
    theme: themeRaw === "dark" || themeRaw === "light" || themeRaw === "system" ? themeRaw : "dark",
    language: languageRaw.startsWith("en") ? "en" : "zh",
    fetch_limit_default: Number(raw.fetch_limit_default ?? 100),
    max_message_display_bytes: Number(raw.max_message_display_bytes ?? 1_048_576),
    allow_multiple_instances: Boolean(raw.allow_multiple_instances ?? false),
    check_updates_on_startup: raw.check_updates_on_startup === undefined ? true : Boolean(raw.check_updates_on_startup),
  };
}

export const api = {
  async listClusters() {
    return tauriInvoke<T.ClusterConfig[]>("list_clusters");
  },

  async saveCluster(config: T.ClusterConfig) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_cluster", { config });
    return { ok: r.ok ?? true };
  },

  async deleteCluster(clusterId: string) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_cluster", { clusterId });
    return { ok: r.ok ?? true };
  },

  async testConnection(config: T.ClusterConfig, password: string | null) {
    return tauriInvoke<T.TestConnectionResult>("test_connection", { config, password });
  },

  async saveSaslPassword(clusterId: string, password: string) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_sasl_password", { clusterId, password });
    return { ok: r.ok ?? true };
  },

  async getClusterSummary(clusterId: string) {
    return tauriInvoke<T.ClusterSummary>("get_cluster_summary", { clusterId });
  },

  async pingCluster(clusterId: string) {
    return tauriInvoke<T.ClusterSummary>("ping_cluster", { clusterId });
  },

  async listBrokers(clusterId: string) {
    return tauriInvoke<T.BrokerInfo[]>("list_brokers", { clusterId });
  },

  async listTopics(clusterId: string) {
    return tauriInvoke<T.TopicSummary[]>("list_topics", { clusterId });
  },

  async getTopicDetail(clusterId: string, topic: string) {
    return tauriInvoke<T.TopicDetail>("get_topic_detail", { clusterId, topic });
  },

  async createTopic(clusterId: string, req: T.CreateTopicRequest) {
    const r = await tauriInvoke<{ ok?: boolean }>("create_topic", { clusterId, req });
    return { ok: r.ok ?? true };
  },

  async deleteTopic(clusterId: string, topic: string) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_topic", { clusterId, topic });
    return { ok: r.ok ?? true };
  },

  async updateTopicConfig(clusterId: string, topic: string, configs: Record<string, string>) {
    const r = await tauriInvoke<{ ok?: boolean }>("update_topic_config", { clusterId, topic, configs });
    return { ok: r.ok ?? true };
  },

  async addPartitions(clusterId: string, topic: string, newPartitionCount: number) {
    const r = await tauriInvoke<{ ok?: boolean }>("add_partitions", {
      clusterId,
      topic,
      newCount: newPartitionCount,
    });
    return { ok: r.ok ?? true };
  },

  async fetchMessages(req: T.FetchMessagesRequest) {
    const raw = await tauriInvoke<T.FetchMessagesResponse>("fetch_messages", {
      req: {
        ...req,
        fetch_mode: toBackendFetchMode(req.fetch_mode),
      },
    });
    return mapFetchMessagesResponse(raw);
  },

  async produceMessage(req: T.ProduceMessageRequest) {
    await tauriInvoke<{ ok?: boolean }>("produce_message", { req });
    return {
      partition: req.partition ?? -1,
      offset: -1,
    };
  },

  async startLiveConsume(
    req: T.FetchMessagesRequest,
    sessionId: string,
    onMessage: (msg: T.KafkaMessage) => void,
  ) {
    const { Channel } = await import("@tauri-apps/api/core");
    const ch = new Channel<T.KafkaMessage>();
    ch.onmessage = (raw) => {
      onMessage({
        ...raw,
        timestamp_type: mapTimestampType(raw.timestamp_type),
        value_encoding: mapEncoding(raw.value_encoding),
        headers: (raw.headers ?? []).map((h) => ({ key: h.key, value: h.value ?? null })),
      });
    };
    await tauriInvoke<void>("start_live_consume", {
      req: { ...req, fetch_mode: toBackendFetchMode(req.fetch_mode) },
      sessionId,
      channel: ch,
    });
    return ch;
  },

  async stopLiveConsume(sessionId: string) {
    await tauriInvoke<void>("stop_live_consume", { sessionId });
  },

  /**
   * 流式导出消息到 CSV。后端边拉边写文件，通过 Channel 回传进度。
   * 返回的 Promise 在导出结束（完成/取消/出错）时 resolve。
   */
  async exportMessages(
    req: T.ExportRequest,
    sessionId: string,
    onProgress: (p: T.ExportProgress) => void,
  ) {
    const { Channel } = await import("@tauri-apps/api/core");
    const ch = new Channel<T.ExportProgress>();
    ch.onmessage = (p) => onProgress(p);
    await tauriInvoke<void>("export_messages", {
      req: { ...req, fetch_mode: toBackendFetchMode(req.fetch_mode) },
      sessionId,
      channel: ch,
    });
  },

  async stopExport(sessionId: string) {
    await tauriInvoke<void>("stop_export", { sessionId });
  },

  async listConsumerGroups(clusterId: string) {
    return tauriInvoke<T.ConsumerGroupSummary[]>("list_consumer_groups", { clusterId });
  },

  async getConsumerGroupDetail(clusterId: string, groupId: string) {
    return tauriInvoke<T.ConsumerGroupDetail>("get_consumer_group_detail", { clusterId, groupId });
  },

  async deleteConsumerGroup(clusterId: string, groupId: string) {
    const r = await tauriInvoke<{ ok?: boolean }>("delete_consumer_group", { clusterId, groupId });
    return { ok: r.ok ?? true };
  },

  async resetOffset(req: T.ResetOffsetRequest) {
    const r = await tauriInvoke<{ ok?: boolean }>("reset_offset", {
      req: toBackendResetOffsetRequest(req),
    });
    return { ok: r.ok ?? true };
  },

  async listTopicConsumerGroups(clusterId: string, topic: string) {
    return tauriInvoke<T.TopicConsumerGroup[]>("list_topic_consumer_groups", { clusterId, topic });
  },

  async getTopicGroupPartitionLag(clusterId: string, topic: string, groupId: string) {
    return tauriInvoke<T.PartitionLag[]>("get_topic_group_partition_lag", { clusterId, topic, groupId });
  },

  async getAppConfig() {
    const raw = await tauriInvoke<T.AppConfig>("get_app_config");
    return normalizeAppConfig(raw);
  },

  async saveAppConfig(config: T.AppConfig) {
    const r = await tauriInvoke<{ ok?: boolean }>("save_app_config", { config });
    return { ok: r.ok ?? true };
  },

  checkUpdate(): Promise<{ latestVersion: string; releaseUrl: string }> {
    return tauriInvoke("check_update");
  },
};
