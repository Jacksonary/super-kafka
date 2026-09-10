import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  Badge,
  Button,
  Card,
  Checkbox,
  DatePicker,
  Form,
  Input,
  InputNumber,
  Progress,
  Segmented,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  App as AntdApp,
} from "antd";
import { DownloadOutlined, LeftOutlined, PauseCircleOutlined, PlayCircleOutlined, ReloadOutlined, RightOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import type { Dayjs } from "dayjs";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { FetchMode, KafkaMessage, SearchProgress, SearchSummary, TopicSummary } from "../types";
import { formatBytes, formatTimestamp, truncate } from "../utils/format";
import MessageDetailDrawer from "../components/Message/MessageDetailDrawer";
import ExportColumnsModal from "../components/Message/ExportColumnsModal";

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
  // Limit 语义:Fetch = 最多拉取条数;Search = 最多扫描条数(勾 No limit 时忽略)。
  const [limit, setLimit] = useState<number>(1000);

  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [filterText, setFilterText] = useState("");
  const [selected, setSelected] = useState<KafkaMessage | null>(null);
  const [exportOpen, setExportOpen] = useState(false);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  // Table 表体滚动高度测量（定义在 filtered 之后，见下方 useTableScroll）。

  // Live mode state
  const [viewMode, setViewMode] = useState<ViewMode>("fetch");
  const [liveRunning, setLiveRunning] = useState(false);
  const [liveMessages, setLiveMessages] = useState<KafkaMessage[]>([]);
  const liveSessionIdRef = useRef<string | null>(null);
  const liveChannelRef = useRef<unknown>(null);
  const liveRunningRef = useRef(false);
  useEffect(() => { liveRunningRef.current = liveRunning; }, [liveRunning]);

  // Search (全量检索) state
  const [searchRunning, setSearchRunning] = useState(false);
  const [searchStopAfterFirst, setSearchStopAfterFirst] = useState(true);
  const [searchNoLimit, setSearchNoLimit] = useState(false);
  const [searchProgress, setSearchProgress] = useState<SearchProgress | null>(null);
  const [searchQueryUsed, setSearchQueryUsed] = useState("");
  const searchSessionIdRef = useRef<string | null>(null);
  const searchChannelRef = useRef<Awaited<ReturnType<typeof api.startSearch>> | null>(null);
  const searchRunningRef = useRef(false);
  useEffect(() => { searchRunningRef.current = searchRunning; }, [searchRunning]);
  // 命中批量追加:先攒在 ref,每 120ms flush 一次,避免 5k 条命中 O(n²) 逐条 setState。
  const pendingHitsRef = useRef<KafkaMessage[]>([]);
  const searchFlushTimerRef = useRef<number | null>(null);

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

  // 结束/停止时清理批量追加定时器，并把剩余命中一次性刷出。
  const clearSearchFlush = useCallback(() => {
    if (searchFlushTimerRef.current !== null) {
      window.clearInterval(searchFlushTimerRef.current);
      searchFlushTimerRef.current = null;
    }
    const leftover = pendingHitsRef.current;
    if (leftover.length > 0) {
      pendingHitsRef.current = [];
      setMessages((prev) => [...prev, ...leftover]);
    }
  }, []);

  const finishSearch = useCallback((sessionId: string, summary: SearchSummary) => {
    // 旧会话保护：Stop 或重启后迟到的摘要直接丢弃
    if (searchSessionIdRef.current !== sessionId) return;
    searchSessionIdRef.current = null;
    searchChannelRef.current = null;
    clearSearchFlush();
    setSearchRunning(false);
    setSearchProgress(null);
    const elapsed = `${(summary.elapsed_ms / 1000).toFixed(1)}s`;
    if (summary.reason === "error") {
      message.error(`Search failed in ${elapsed}: ${summary.error ?? "unknown error"}`);
    } else if (summary.reason === "stopped_by_user") {
      message.info(
        `Search stopped in ${elapsed}: ${summary.hits} matched (scanned ${summary.scanned})`,
      );
    } else if (summary.reason === "hit_cap") {
      message.warning(
        `Search stopped at ${summary.hits} matched (scanned ${summary.scanned}, ${elapsed}) — search supports up to 5,000 results`,
      );
    } else {
      message.success(
        `Search finished in ${elapsed}: ${summary.hits} matched (scanned ${summary.scanned})`,
      );
    }
  }, [message, clearSearchFlush]);

  const stopSearch = useCallback(async () => {
    const sid = searchSessionIdRef.current;
    if (!sid) return;
    searchSessionIdRef.current = null;
    searchChannelRef.current = null;
    clearSearchFlush();
    setSearchRunning(false);
    setSearchProgress(null);
    try { await api.stopSearch(sid); } catch { /* ignore */ }
  }, [clearSearchFlush]);

  const handleSearch = useCallback(async () => {
    if (!currentClusterId || !topic) {
      message.warning("Select a topic first");
      return;
    }
    const q = filterText.trim();
    if (!q) {
      message.warning("Enter a search query first");
      return;
    }
    // 防重入 + 与 Fetch 互斥
    if (searchSessionIdRef.current || searchRunningRef.current || loading) return;
    const sessionId = crypto.randomUUID();
    searchSessionIdRef.current = sessionId;
    setSearchRunning(true);
    setSearchQueryUsed(q);
    setSearchProgress({ scanned: 0, total: 0, hits: 0, elapsed_ms: 0 });
    setMessages([]); // 结果替换当前列表
    setPage(1);
    // 命中先攒进 ref，每 120ms 批量追加一次，避免逐条 setState 造成 O(n²) 卡顿。
    pendingHitsRef.current = [];
    searchFlushTimerRef.current = window.setInterval(() => {
      const batch = pendingHitsRef.current;
      if (batch.length === 0) return;
      pendingHitsRef.current = [];
      setMessages((prev) => [...prev, ...batch]);
    }, 120);
    try {
      const chans = await api.startSearch(
        { cluster_id: currentClusterId, topic, partition, fetch_mode: fetchMode, limit },
        q,
        searchStopAfterFirst,
        searchNoLimit,
        sessionId,
        (msg) => {
          // 旧会话保护：Stop 后的迟到命中不进入列表
          if (searchSessionIdRef.current !== sessionId) return;
          pendingHitsRef.current.push(msg);
        },
        (summary) => finishSearch(sessionId, summary),
        (p) => {
          if (searchSessionIdRef.current !== sessionId) return;
          setSearchProgress(p);
        },
      );
      searchChannelRef.current = chans; // 持有三 Channel，防止 GC 回收
    } catch (e) {
      message.error(`Search failed: ${String(e)}`);
      clearSearchFlush();
      setSearchRunning(false);
      setSearchProgress(null);
      searchSessionIdRef.current = null;
    }
  }, [currentClusterId, topic, partition, fetchMode, limit, filterText, searchStopAfterFirst, searchNoLimit, loading, message, finishSearch, clearSearchFlush]);

  // 组件卸载时停止
  useEffect(() => {
    return () => {
      const sid = liveSessionIdRef.current;
      if (sid) {
        liveSessionIdRef.current = null;
        void api.stopLiveConsume(sid);
      }
      const ssid = searchSessionIdRef.current;
      if (ssid) {
        searchSessionIdRef.current = null;
        void api.stopSearch(ssid);
      }
      // 清掉批量追加定时器，避免卸载后仍持有 state 更新
      if (searchFlushTimerRef.current !== null) {
        window.clearInterval(searchFlushTimerRef.current);
        searchFlushTimerRef.current = null;
      }
      pendingHitsRef.current = [];
    };
  }, []);

  // topic 或 cluster 切换时停止 live/search
  useEffect(() => {
    if (liveRunningRef.current) {
      void stopLive();
    }
    if (searchRunningRef.current) {
      void stopSearch();
    }
  }, [topic, currentClusterId, stopLive, stopSearch]);


  const filtered = useMemo(() => {
    const q = filterText.trim().toLowerCase();
    if (!q) return messages;
    return messages.filter(
      (m) =>
        (m.key_text ?? "").toLowerCase().includes(q) ||
        (m.value_text ?? "").toLowerCase().includes(q),
    );
  }, [messages, filterText]);

  // 分页数据：只切当前页（分页器独立于表格，pagination={false}）
  const list = viewMode === "live" ? liveMessages : filtered;
  // 搜索进度百分比：total 由后端预先算出（各分区 end_offset − start_offset 之和）
  const searchPercent =
    searchProgress && searchProgress.total > 0
      ? Math.min(100, Math.round((searchProgress.scanned / searchProgress.total) * 100))
      : 0;
  // 进度数字区宽度按 total 动态计算(ch = 一个等宽数字的宽度):
  // scanned 最大等于 total、hits 最大等于 limit(≤1000),据此算最大串长。
  // 数字符按 1ch、非数字符(逗号/斜杠/空格/字母)按 ~0.5ch 加权,
  // 避免固定宽度截断,也避免符号按 1ch 高估造成大段留白。
  const progressTextWidth = useMemo(() => {
    const totalStr = (searchProgress?.total ?? 0).toLocaleString();
    const maxHits = Math.min(limit, 1000);
    const template = `${totalStr} / ${totalStr} · ${maxHits.toLocaleString()} hits`;
    let units = 0.5; // 收尾余量
    for (const c of template) {
      units += /\d/.test(c) ? 1 : 0.5;
    }
    return `${units.toFixed(1)}ch`;
  }, [searchProgress?.total, limit]);
  const maxPage = Math.max(1, Math.ceil(list.length / pageSize));
  const safePage = Math.min(page, maxPage);
  const pagedData = useMemo(
    () => list.slice((safePage - 1) * pageSize, safePage * pageSize),
    [list, safePage, pageSize],
  );

  // Table 表体滚动高度（scroll.y = 容器高 − 表头高）。
  // 分页器已移出滚动容器，无需再扣分页器高度。ResizeObserver 处理窗口/外层尺寸变化。
  const tableWrapRef = useRef<HTMLDivElement>(null);
  const [tableScrollY, setTableScrollY] = useState<number>(400);
  useEffect(() => {
    const el = tableWrapRef.current;
    if (!el) return;
    const compute = () => {
      const headerH =
        el.querySelector<HTMLElement>(".ant-table-header")?.getBoundingClientRect().height ?? 0;
      const y = Math.floor(el.clientHeight - headerH);
      setTableScrollY(y > 0 ? y : 0);
    };
    compute();
    const ro = new ResizeObserver(compute); // 处理窗口/外层尺寸变化
    ro.observe(el);
    return () => ro.disconnect();
  }, [viewMode]);

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
      title: "Size",
      key: "size",
      width: 90,
      align: "right",
      render: (_, m) => (
        <Tooltip title="Value byte size before compression, excluding protocol overhead">
          <Text type="secondary" style={{ fontSize: 12 }}>
            {formatBytes(m.value_raw.length)}
          </Text>
        </Tooltip>
      ),
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
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 12,
        width: "100%",
        flex: 1,
        minHeight: 0,
      }}
    >
      <Card size="small">
        <Space direction="vertical" size={8} style={{ width: "100%" }}>
          {/* 模式切换 */}
          <Segmented
            value={viewMode}
            onChange={(v) => {
              const next = v as ViewMode;
              if (next === "fetch" && liveRunning) void stopLive();
              if (next === "live" && searchRunning) void stopSearch();
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
                style={{ width: 70 }}
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
                    style={{ width: 120 }}
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
                      style={{ width: 365 }}
                      value={timeRange ?? undefined}
                      onChange={(r) => setTimeRange(r as [Dayjs, Dayjs] | null)}
                    />
                  </Form.Item>
                )}
                <Form.Item label="Limit">
                  <InputNumber
                    min={1}
                    max={5000}
                    value={limit}
                    disabled={searchNoLimit}
                    onChange={(v) => setLimit(v ?? 1000)}
                  />
                </Form.Item>
                <Form.Item>
                  <Button
                    type="primary"
                    icon={<ReloadOutlined />}
                    loading={loading}
                    disabled={searchRunning}
                    onClick={handleFetch}
                  >
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

      <Space style={{ width: "100%", justifyContent: "space-between" }} wrap>
        {viewMode === "fetch" ? (
          <Space wrap size={12}>
            {searchRunning ? (
              <>
                {/* 搜索中：收起输入框，改为进度组（关键词 + 进度条 + 已扫/总量 + 命中） */}
                <Badge status="processing" />
                <Text
                  type="secondary"
                  style={{ fontSize: 12, maxWidth: 160 }}
                  ellipsis={{ tooltip: searchQueryUsed }}
                >
                  Searching “{searchQueryUsed}”
                </Text>
                <Progress
                  percent={searchPercent}
                  size="small"
                  status="active"
                  style={{ width: 140, margin: 0 }}
                />
                <Text
                  type="secondary"
                  style={{
                    fontSize: 12,
                    // 宽度由 total/limit 动态算出(ch),等宽数字;位数变化不推动后续元素
                    display: "inline-block",
                    width: progressTextWidth,
                    fontVariantNumeric: "tabular-nums",
                  }}
                >
                  {(searchProgress?.scanned ?? 0).toLocaleString()} /{" "}
                  {(searchProgress?.total ?? 0).toLocaleString()} · {searchProgress?.hits ?? 0} hits
                </Text>
                <Button danger icon={<PauseCircleOutlined />} onClick={stopSearch}>
                  Stop
                </Button>
                <Checkbox checked={searchStopAfterFirst} disabled>
                  Stop on first match
                </Checkbox>
              </>
            ) : (
              <>
                <Input
                  allowClear
                  prefix={<SearchOutlined />}
                  placeholder="Filter / search messages by key/value"
                  value={filterText}
                  onChange={(e) => setFilterText(e.target.value)}
                  style={{ width: 320 }}
                />
                <Button icon={<SearchOutlined />} disabled={loading} onClick={handleSearch}>
                  Search
                </Button>
                <Checkbox
                  checked={searchStopAfterFirst}
                  onChange={(e) => setSearchStopAfterFirst(e.target.checked)}
                >
                  Stop on first match
                </Checkbox>
                <Tooltip title="Ignore the Limit above and scan the whole range — can run for a long time on large topics">
                  <Checkbox
                    checked={searchNoLimit}
                    onChange={(e) => setSearchNoLimit(e.target.checked)}
                  >
                    No limit
                  </Checkbox>
                </Tooltip>
              </>
            )}
          </Space>
        ) : (
          <span />
        )}
        <Space>
          <Button
            icon={<DownloadOutlined />}
            disabled={(viewMode === "live" ? liveMessages : filtered).length === 0}
            onClick={() => setExportOpen(true)}
          >
            Export CSV
          </Button>
        </Space>
      </Space>

      <div
        ref={tableWrapRef}
        style={{
          flex: 1,
          minHeight: 0,
          overflow: "hidden",
          // antd wrapper 要撑满这个 flex 容器，scroll.y 才不再把分页器顶出视口
          display: "flex",
          flexDirection: "column",
        }}
      >
        <Table<KafkaMessage>
          size="small"
          rowKey={(m) => `${m.partition}-${m.offset}`}
          columns={columns}
          dataSource={pagedData}
          loading={viewMode === "fetch" ? loading : false}
          pagination={false}
          onRow={(record) => ({
            onClick: () => setSelected(record),
            style: { cursor: "pointer" },
          })}
          scroll={{ x: 900, y: tableScrollY }}
        />
      </div>

      {/* 独立分页器：放在滚动容器外，避免被 overflow:hidden 裁掉底部；极简文字风格，居中。
          手写布局：总数在左，‹ 3/5 › 在中间，每页条数在右。 */}
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          gap: 16,
          flexShrink: 0,
        }}
      >
        <Text type="secondary">
          total {list.length}
        </Text>
        <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <Button
            type="text"
            size="small"
            icon={<LeftOutlined />}
            disabled={safePage <= 1}
            onClick={() => setPage(safePage - 1)}
          />
          <Text type="secondary">
            {safePage} / {maxPage}
          </Text>
          <Button
            type="text"
            size="small"
            icon={<RightOutlined />}
            disabled={safePage >= maxPage}
            onClick={() => setPage(safePage + 1)}
          />
        </span>
        <Select
          size="small"
          variant="borderless"
          value={pageSize}
          style={{ width: 100 }}
          onChange={(v) => {
            setPageSize(v);
            setPage(1);
          }}
          options={[
            { value: 20, label: "20 / page" },
            { value: 50, label: "50 / page" },
            { value: 100, label: "100 / page" },
          ]}
        />
      </div>

      <MessageDetailDrawer
        open={selected !== null}
        message={selected}
        onClose={() => setSelected(null)}
        topic={topic}
        onReplay={(msg, t) => {
          navigate("/producer", { state: { replayMessage: msg, replayTopic: t } });
        }}
      />

      <ExportColumnsModal
        open={exportOpen}
        clusterId={currentClusterId}
        topic={topic ?? ""}
        partition={partition}
        fetchMode={fetchMode}
        sampleMessages={viewMode === "live" ? liveMessages : filtered}
        onClose={() => setExportOpen(false)}
      />
    </div>
  );
}
