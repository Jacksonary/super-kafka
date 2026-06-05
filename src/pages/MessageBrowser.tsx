import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  Badge,
  Button,
  Card,
  DatePicker,
  Form,
  Input,
  InputNumber,
  Segmented,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { DownloadOutlined, PauseCircleOutlined, PlayCircleOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import type { Dayjs } from "dayjs";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { FetchMode, KafkaMessage, TopicSummary } from "../types";
import { formatTimestamp, truncate } from "../utils/format";
import MessageDetailDrawer from "../components/Message/MessageDetailDrawer";
import { exportMessages } from "../utils/export";

const { Text } = Typography;
const { RangePicker } = DatePicker;

type FetchModeKind = "earliest" | "latest" | "from_offset" | "time_range";
type ViewMode = "fetch" | "live";

const LIVE_MAX_BUFFER = 500;

interface Props {
  embeddedTopic?: string;
  embeddedPartitionCount?: number;
  // When provided, partition selection is controlled by the parent (used by
  // TopicDetail to show the selected partition's offsets in the summary header).
  partition?: number | null;
  onPartitionChange?: (partition: number | null) => void;
}

export default function MessageBrowser({
  embeddedTopic,
  embeddedPartitionCount,
  partition: controlledPartition,
  onPartitionChange,
}: Props) {
  const { currentClusterId } = useClusterStore();
  const { message } = AntdApp.useApp();
  const navigate = useNavigate();

  const [topics, setTopics] = useState<TopicSummary[]>([]);
  const [topic, setTopic] = useState<string | null>(embeddedTopic ?? null);
  const [internalPartition, setInternalPartition] = useState<number | null>(null);
  const controlled = onPartitionChange !== undefined;
  const partition = controlled ? (controlledPartition ?? null) : internalPartition;
  const setPartition = controlled ? onPartitionChange : setInternalPartition;
  const [modeKind, setModeKind] = useState<FetchModeKind>("latest");
  const [fromOffset, setFromOffset] = useState<number>(0);
  const [timeRange, setTimeRange] = useState<[Dayjs, Dayjs] | null>(null);
  const [limit, setLimit] = useState<number>(100);

  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [filterText, setFilterText] = useState("");
  const [selected, setSelected] = useState<KafkaMessage | null>(null);

  // Live mode state
  const [viewMode, setViewMode] = useState<ViewMode>("fetch");
  const [liveRunning, setLiveRunning] = useState(false);
  const [liveMessages, setLiveMessages] = useState<KafkaMessage[]>([]);
  const liveSessionIdRef = useRef<string | null>(null);
  const liveChannelRef = useRef<unknown>(null);
  const liveRunningRef = useRef(false);
  useEffect(() => { liveRunningRef.current = liveRunning; }, [liveRunning]);

  // Load topics for selector when not embedded
  useEffect(() => {
    if (!currentClusterId || embeddedTopic) return;
    void api
      .listTopics(currentClusterId)
      .then(setTopics)
      .catch((e) => message.error(String(e)));
  }, [currentClusterId, embeddedTopic, message]);

  useEffect(() => {
    if (embeddedTopic) setTopic(embeddedTopic);
  }, [embeddedTopic]);

  const fetchMode = useMemo<FetchMode>(() => {
    if (modeKind === "earliest") {
      return { type: "earliest" };
    }
    if (modeKind === "from_offset") {
      return { type: "from_offset", offset: fromOffset };
    }
    if (modeKind === "time_range") {
      const start = timeRange?.[0]?.valueOf() ?? Date.now() - 3600_000;
      const end = timeRange?.[1]?.valueOf() ?? Date.now();
      return { type: "time_range", start_ms: start, end_ms: end };
    }
    return { type: "latest" };
  }, [modeKind, fromOffset, timeRange]);

  const handleFetch = useCallback(async () => {
    if (!currentClusterId || !topic) {
      message.warning("Select a topic first");
      return;
    }
    setLoading(true);
    try {
      const res = await api.fetchMessages({
        cluster_id: currentClusterId,
        topic,
        partition,
        fetch_mode: fetchMode,
        limit,
      });
      setMessages(res.messages);
      message.success(`Fetched ${res.total_fetched} messages${res.has_more ? " (more available)" : ""}`);
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, topic, partition, fetchMode, limit, message]);

  const stopLive = useCallback(async () => {
    const sid = liveSessionIdRef.current;
    if (!sid) return;
    liveSessionIdRef.current = null;
    liveChannelRef.current = null;
    setLiveRunning(false);
    try { await api.stopLiveConsume(sid); } catch { /* ignore */ }
  }, []);

  const handleStartLive = useCallback(async () => {
    if (!currentClusterId || !topic) {
      message.warning("Select a topic first");
      return;
    }
    // 防重入：已在运行或有 session 时直接返回
    if (liveSessionIdRef.current) return;
    const sessionId = crypto.randomUUID();
    liveSessionIdRef.current = sessionId;
    setLiveMessages([]);
    setLiveRunning(true);
    try {
      const ch = await api.startLiveConsume(
        { cluster_id: currentClusterId, topic, partition, fetch_mode: { type: "latest" }, limit: 0 },
        sessionId,
        (msg) => {
          setLiveMessages((prev) => {
            const next = [msg, ...prev];
            return next.length > LIVE_MAX_BUFFER ? next.slice(0, LIVE_MAX_BUFFER) : next;
          });
        },
      );
      liveChannelRef.current = ch; // 持有 Channel 引用，防止 GC 回收
    } catch (e) {
      message.error(`Live consume failed: ${String(e)}`);
      setLiveRunning(false);
      liveSessionIdRef.current = null;
    }
  }, [currentClusterId, topic, partition, message]);

  // 组件卸载时停止
  useEffect(() => {
    return () => {
      const sid = liveSessionIdRef.current;
      if (sid) {
        liveSessionIdRef.current = null;
        void api.stopLiveConsume(sid);
      }
    };
  }, []);

  // topic 或 cluster 切换时停止 live
  useEffect(() => {
    if (liveRunningRef.current) {
      void stopLive();
    }
  }, [topic, currentClusterId, stopLive]);


  const filtered = useMemo(() => {
    const q = filterText.trim().toLowerCase();
    if (!q) return messages;
    return messages.filter(
      (m) =>
        (m.key_text ?? "").toLowerCase().includes(q) ||
        (m.value_text ?? "").toLowerCase().includes(q),
    );
  }, [messages, filterText]);

  const columns: ColumnsType<KafkaMessage> = [
    { title: "Partition", dataIndex: "partition", width: 90, key: "partition" },
    { title: "Offset", dataIndex: "offset", width: 110, key: "offset" },
    {
      title: "Timestamp",
      dataIndex: "timestamp",
      key: "timestamp",
      width: 200,
      render: (t: number | null) => formatTimestamp(t),
    },
    {
      title: "Key",
      dataIndex: "key_text",
      key: "key_text",
      width: 180,
      render: (k: string | null) =>
        k ? <Text code style={{ fontSize: 12 }}>{truncate(k, 30)}</Text> : <Text type="secondary">null</Text>,
    },
    {
      title: "Value",
      dataIndex: "value_text",
      key: "value_text",
      ellipsis: true,
      render: (v: string | null) =>
        v ? (
          <Text style={{ fontFamily: "ui-monospace, Menlo, monospace", fontSize: 12 }}>
            {truncate(v, 150)}
          </Text>
        ) : (
          <Text type="secondary">(binary)</Text>
        ),
    },
  ];

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }

  const partitionOptions = (() => {
    const count = embeddedTopic
      ? (embeddedPartitionCount ?? null)
      : (topics.find((x) => x.name === topic)?.partition_count ?? null);
    if (count == null) return null;
    return [
      { value: -1, label: "All" },
      ...Array.from({ length: count }, (_, i) => ({ value: i, label: String(i) })),
    ];
  })();

  return (
    <Space direction="vertical" size={12} style={{ width: "100%" }}>
      <Card size="small">
        <Space direction="vertical" size={8} style={{ width: "100%" }}>
          {/* 模式切换 */}
          <Segmented
            value={viewMode}
            onChange={(v) => {
              const next = v as ViewMode;
              if (next === "fetch" && liveRunning) void stopLive();
              setViewMode(next);
            }}
            options={[
              { label: "Fetch", value: "fetch" },
              { label: "Live", value: "live" },
            ]}
          />

          <Form layout="inline">
            {!embeddedTopic && (
              <Form.Item label="Topic">
                <Select
                  style={{ width: 240 }}
                  value={topic ?? undefined}
                  onChange={(v: string) => setTopic(v)}
                  showSearch
                  placeholder="Select topic"
                  options={topics.map((t) => ({ value: t.name, label: t.name }))}
                />
              </Form.Item>
            )}
            <Form.Item label="Partition">
              <Select
                style={{ width: 160 }}
                value={partition ?? -1}
                onChange={(v: number) => setPartition(v === -1 ? null : v)}
                options={partitionOptions ?? [{ value: -1, label: "All" }]}
                disabled={partitionOptions === null}
              />
            </Form.Item>

            {/* Fetch 模式专属控件 */}
            {viewMode === "fetch" && (
              <>
                <Form.Item label="Mode">
                  <Select
                    style={{ width: 160 }}
                    value={modeKind}
                    onChange={setModeKind}
                    options={[
                      { value: "latest", label: "Latest" },
                      { value: "earliest", label: "Earliest" },
                      { value: "from_offset", label: "From Offset" },
                      { value: "time_range", label: "Time Range" },
                    ]}
                  />
                </Form.Item>
                {modeKind === "from_offset" && (
                  <Form.Item label="Offset">
                    <InputNumber min={0} value={fromOffset} onChange={(v) => setFromOffset(v ?? 0)} />
                  </Form.Item>
                )}
                {modeKind === "time_range" && (
                  <Form.Item label="Range">
                    <RangePicker
                      showTime
                      value={timeRange ?? undefined}
                      onChange={(r) => setTimeRange(r as [Dayjs, Dayjs] | null)}
                    />
                  </Form.Item>
                )}
                <Form.Item label="Limit">
                  <InputNumber min={1} max={1000} value={limit} onChange={(v) => setLimit(v ?? 100)} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" icon={<ReloadOutlined />} loading={loading} onClick={handleFetch}>
                    Fetch
                  </Button>
                </Form.Item>
              </>
            )}

            {/* Live 模式控件 */}
            {viewMode === "live" && (
              <>
                <Form.Item>
                  {liveRunning ? (
                    <Button danger icon={<PauseCircleOutlined />} onClick={stopLive}>
                      Stop
                    </Button>
                  ) : (
                    <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleStartLive}>
                      Start
                    </Button>
                  )}
                </Form.Item>
                {liveRunning && (
                  <Form.Item>
                    <Badge status="processing" text="Live" />
                  </Form.Item>
                )}
              </>
            )}
          </Form>
        </Space>
      </Card>

      <Space style={{ width: "100%", justifyContent: "space-between" }}>
        {viewMode === "fetch" ? (
          <Input
            allowClear
            prefix={<SearchOutlined />}
            placeholder="Filter loaded messages by key/value"
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            style={{ width: 360 }}
          />
        ) : (
          <span />
        )}
        <Space>
          <Button
            icon={<DownloadOutlined />}
            disabled={(viewMode === "live" ? liveMessages : filtered).length === 0}
            onClick={() => {
              const data = viewMode === "live" ? liveMessages : filtered;
              if (topic) exportMessages(data, topic);
            }}
          >
            Export CSV
          </Button>
          <Text type="secondary">
            {viewMode === "live"
              ? `${liveMessages.length} messages (max ${LIVE_MAX_BUFFER})`
              : `${filtered.length} / ${messages.length} messages`}
          </Text>
        </Space>
      </Space>

      <Table<KafkaMessage>
        size="small"
        rowKey={(m) => `${m.partition}-${m.offset}`}
        columns={columns}
        dataSource={viewMode === "live" ? liveMessages : filtered}
        loading={viewMode === "fetch" ? loading : false}
        pagination={{ defaultPageSize: 20, showSizeChanger: true, pageSizeOptions: [20, 50, 100] }}
        onRow={(record) => ({
          onClick: () => setSelected(record),
          style: { cursor: "pointer" },
        })}
        scroll={{ x: 900 }}
      />

      <MessageDetailDrawer
        open={selected !== null}
        message={selected}
        onClose={() => setSelected(null)}
        topic={topic}
        onReplay={(msg, t) => {
          navigate("/producer", { state: { replayMessage: msg, replayTopic: t } });
        }}
      />
    </Space>
  );
}
