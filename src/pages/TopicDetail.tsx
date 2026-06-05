import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Alert,
  Breadcrumb,
  Button,
  Card,
  Checkbox,
  Descriptions,
  Input,
  InputNumber,
  Modal,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  App as AntdApp,
} from "antd";
import { ArrowLeftOutlined, EditOutlined, PlusOutlined, ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { PartitionInfo, TopicConfig, TopicDetail as TopicDetailType } from "../types";
import { formatDurationMs, formatNumber } from "../utils/format";
import DurationInput from "../components/Common/DurationInput";
import MessageBrowser from "./MessageBrowser";
import TopicConsumerGroups from "../components/Topic/TopicConsumerGroups";

const { Text, Title } = Typography;

export default function TopicDetail() {
  const { topicName: rawName } = useParams<{ topicName: string }>();
  const topicName = rawName ? decodeURIComponent(rawName) : "";
  const navigate = useNavigate();
  const { currentClusterId } = useClusterStore();
  const { message } = AntdApp.useApp();
  const [detail, setDetail] = useState<TopicDetailType | null>(null);
  const [loading, setLoading] = useState(false);
  const [editingConfig, setEditingConfig] = useState<Record<string, string>>({});
  const [activeTab, setActiveTab] = useState("partitions");
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [partitionsModalOpen, setPartitionsModalOpen] = useState(false);
  const [retentionModalOpen, setRetentionModalOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const currentPartitionCount = detail?.partitions.length ?? 0;
  const currentRetentionMs = useMemo(() => {
    const raw = detail?.configs.find((c) => c.name === "retention.ms")?.value;
    if (raw == null || raw === "") return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }, [detail]);

  const load = useCallback(async () => {
    if (!currentClusterId || !topicName) return;
    setLoading(true);
    try {
      const d = await api.getTopicDetail(currentClusterId, topicName);
      setDetail(d);
      setEditingConfig({});
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, topicName, message]);

  useEffect(() => {
    void load();
  }, [load]);

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }
  if (!topicName) {
    return <Alert type="warning" showIcon message="Missing topic name in URL." />;
  }

  const partitionColumns: ColumnsType<PartitionInfo> = [
    { title: "Partition", dataIndex: "partition_id", key: "partition_id", width: 100 },
    { title: "Leader", dataIndex: "leader", key: "leader", width: 100 },
    {
      title: "Replicas",
      dataIndex: "replicas",
      key: "replicas",
      render: (r: number[]) => r.map((id) => <Tag key={id}>{id}</Tag>),
    },
    {
      title: "ISR",
      dataIndex: "isr",
      key: "isr",
      render: (r: number[], rec) =>
        r.map((id) => (
          <Tag key={id} color={rec.replicas.length === r.length ? "green" : "orange"}>
            {id}
          </Tag>
        )),
    },
    {
      title: "Start Offset",
      dataIndex: "log_start_offset",
      key: "log_start_offset",
      align: "right",
      render: formatNumber,
    },
    {
      title: "End Offset",
      dataIndex: "log_end_offset",
      key: "log_end_offset",
      align: "right",
      render: formatNumber,
    },
    {
      title: "Messages",
      dataIndex: "message_count",
      key: "message_count",
      align: "right",
      render: formatNumber,
    },
  ];

  const configColumns: ColumnsType<TopicConfig> = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      width: 280,
      render: (n: string) => <Text code style={{ fontSize: 12 }}>{n}</Text>,
    },
    {
      title: "Value",
      key: "value",
      render: (_, c) => {
        if (c.is_read_only) {
          return <Text type="secondary">{c.value ?? "-"}</Text>;
        }
        const current = editingConfig[c.name] ?? c.value ?? "";
        const isDuration = c.name === "retention.ms";
        const ms = isDuration && current !== "" ? Number(current) : null;
        const showHint = isDuration && ms != null && Number.isFinite(ms);
        return (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            <Input
              value={current}
              onChange={(e) => setEditingConfig((p) => ({ ...p, [c.name]: e.target.value }))}
              size="small"
            />
            {showHint && (
              <Tooltip title={`retention.ms = ${ms}`}>
                <Text type="secondary" style={{ fontSize: 11 }}>
                  ≈ {formatDurationMs(ms)}
                </Text>
              </Tooltip>
            )}
          </Space>
        );
      },
    },
    {
      title: "Source",
      key: "source",
      width: 120,
      render: (_, c) =>
        c.is_default ? <Tag color="default">default</Tag> : <Tag color="cyan">override</Tag>,
    },
    {
      title: "Read Only",
      dataIndex: "is_read_only",
      key: "is_read_only",
      width: 100,
      render: (v: boolean) => (v ? <Tag color="orange">yes</Tag> : null),
    },
  ];

  async function handleSaveConfig() {
    if (!currentClusterId || Object.keys(editingConfig).length === 0) return;
    try {
      await api.updateTopicConfig(currentClusterId, topicName, editingConfig);
      message.success("Topic configs updated");
      await load();
    } catch (e) {
      if (e instanceof Error) message.error(e.message);
    }
  }

  return (
    <Spin spinning={loading}>
      <Space direction="vertical" size={16} style={{ width: "100%" }}>
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate("/topics")}>
            Back
          </Button>
          <Breadcrumb
            items={[
              { title: <a onClick={() => navigate("/topics")}>Topics</a> },
              { title: topicName },
            ]}
          />
        </Space>

        <Card>
          <Space direction="vertical" size={4} style={{ width: "100%" }}>
            <Title level={4} style={{ margin: 0 }}>
              {topicName}
            </Title>
            {detail && (() => {
              const sel =
                activeTab === "messages" && selectedPartition !== null
                  ? detail.partitions.find((p) => p.partition_id === selectedPartition)
                  : undefined;
              return (
                <Descriptions column={3} size="small">
                  <Descriptions.Item label="Partitions">
                    <Space size={4}>
                      {detail.partitions.length}
                      <Tooltip title="增加分区数">
                        <Button
                          size="small"
                          type="text"
                          icon={<PlusOutlined />}
                          onClick={() => setPartitionsModalOpen(true)}
                        />
                      </Tooltip>
                    </Space>
                  </Descriptions.Item>
                  <Descriptions.Item label="Replication Factor">
                    {detail.partitions[0]?.replicas.length ?? "-"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Retention">
                    <Space size={4}>
                      <Tooltip title={currentRetentionMs == null ? undefined : `retention.ms = ${currentRetentionMs}`}>
                        <span>{formatDurationMs(currentRetentionMs)}</span>
                      </Tooltip>
                      <Tooltip title="修改保留时间">
                        <Button
                          size="small"
                          type="text"
                          icon={<EditOutlined />}
                          onClick={() => setRetentionModalOpen(true)}
                        />
                      </Tooltip>
                    </Space>
                  </Descriptions.Item>
                  <Descriptions.Item label="Total Messages">
                    {formatNumber(
                      detail.partitions.reduce((sum, p) => sum + p.message_count, 0),
                    )}
                  </Descriptions.Item>
                  {sel && (
                    <>
                      <Descriptions.Item label={`Partition ${sel.partition_id} Start`}>
                        {formatNumber(sel.log_start_offset)}
                      </Descriptions.Item>
                      <Descriptions.Item label={`Partition ${sel.partition_id} End`}>
                        {formatNumber(sel.log_end_offset)}
                      </Descriptions.Item>
                      <Descriptions.Item label={`Partition ${sel.partition_id} Messages`}>
                        {formatNumber(sel.message_count)}
                      </Descriptions.Item>
                    </>
                  )}
                </Descriptions>
              );
            })()}
          </Space>
        </Card>

        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: "partitions",
              label: "Partitions",
              children: (
                <Card
                  size="small"
                  extra={
                    <Button icon={<ReloadOutlined />} size="small" onClick={load}>
                      Refresh
                    </Button>
                  }
                >
                  <Table<PartitionInfo>
                    rowKey="partition_id"
                    size="small"
                    columns={partitionColumns}
                    dataSource={detail?.partitions ?? []}
                    pagination={false}
                  />
                </Card>
              ),
            },
            {
              key: "config",
              label: "Config",
              children: (
                <Card
                  size="small"
                  extra={
                    <Space>
                      <Button
                        type="primary"
                        size="small"
                        disabled={Object.keys(editingConfig).length === 0}
                        onClick={handleSaveConfig}
                      >
                        Save Changes
                      </Button>
                      <Button
                        size="small"
                        disabled={Object.keys(editingConfig).length === 0}
                        onClick={() => setEditingConfig({})}
                      >
                        Reset
                      </Button>
                    </Space>
                  }
                >
                  <Table<TopicConfig>
                    rowKey="name"
                    size="small"
                    columns={configColumns}
                    dataSource={detail?.configs ?? []}
                    pagination={false}
                  />
                </Card>
              ),
            },
            {
              key: "messages",
              label: "Messages",
              children: (
                <MessageBrowser
                  embeddedTopic={topicName}
                  embeddedPartitionCount={detail?.partitions.length}
                  partition={selectedPartition}
                  onPartitionChange={setSelectedPartition}
                />
              ),
            },
            {
              key: "consumer-groups",
              label: "Consumer Groups",
              children: (
                <TopicConsumerGroups clusterId={currentClusterId} topic={topicName} />
              ),
            },
          ]}
        />
      </Space>

      <PartitionsEditModal
        open={partitionsModalOpen}
        currentCount={currentPartitionCount}
        submitting={submitting}
        onClose={() => setPartitionsModalOpen(false)}
        onSubmit={async (newCount) => {
          if (!currentClusterId) return;
          setSubmitting(true);
          try {
            await api.addPartitions(currentClusterId, topicName, newCount);
            message.success(`分区数已增加到 ${newCount}`);
            setPartitionsModalOpen(false);
            await load();
          } catch (e) {
            message.error(String(e));
          } finally {
            setSubmitting(false);
          }
        }}
      />

      <RetentionEditModal
        open={retentionModalOpen}
        currentMs={currentRetentionMs}
        submitting={submitting}
        onClose={() => setRetentionModalOpen(false)}
        onSubmit={async (newMs) => {
          if (!currentClusterId) return;
          setSubmitting(true);
          try {
            await api.updateTopicConfig(currentClusterId, topicName, {
              "retention.ms": String(newMs),
            });
            message.success("保留时间已更新");
            setRetentionModalOpen(false);
            await load();
          } catch (e) {
            message.error(String(e));
          } finally {
            setSubmitting(false);
          }
        }}
      />
    </Spin>
  );
}

interface PartitionsEditModalProps {
  open: boolean;
  currentCount: number;
  submitting: boolean;
  onClose: () => void;
  onSubmit: (newCount: number) => void;
}

function PartitionsEditModal({
  open,
  currentCount,
  submitting,
  onClose,
  onSubmit,
}: PartitionsEditModalProps) {
  const minCount = currentCount + 1;
  const [newCount, setNewCount] = useState<number>(minCount);
  const [acknowledged, setAcknowledged] = useState(false);

  useEffect(() => {
    if (open) {
      setNewCount(minCount);
      setAcknowledged(false);
    }
  }, [open, minCount]);

  return (
    <Modal
      title="增加分区数"
      open={open}
      onCancel={onClose}
      onOk={() => onSubmit(newCount)}
      okText="确认增加"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: !acknowledged || newCount < minCount }}
    >
      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="当前分区数">{currentCount}</Descriptions.Item>
        </Descriptions>
        <div>
          <Typography.Text>新分区数</Typography.Text>
          <InputNumber
            min={minCount}
            step={1}
            precision={0}
            style={{ width: "100%", marginTop: 4 }}
            value={newCount}
            onChange={(v) => {
              if (typeof v === "number" && Number.isFinite(v)) {
                setNewCount(Math.floor(v));
              }
            }}
          />
        </div>
        <Alert
          type="warning"
          showIcon
          message="分区数只能增加，且增加后会改变 key 的分区分布"
          description="新增分区后，原本同一 key 落在同一分区的消息可能会被分散到新分区。如果业务依赖 key 有序性，请评估影响后再操作。"
        />
        <Checkbox
          checked={acknowledged}
          onChange={(e) => setAcknowledged(e.target.checked)}
        >
          我知道这会改变 key 分布
        </Checkbox>
      </Space>
    </Modal>
  );
}

interface RetentionEditModalProps {
  open: boolean;
  currentMs: number | null;
  submitting: boolean;
  onClose: () => void;
  onSubmit: (newMs: number) => void;
}

function RetentionEditModal({
  open,
  currentMs,
  submitting,
  onClose,
  onSubmit,
}: RetentionEditModalProps) {
  // null = 用户未填（保存按钮禁用，提交时跳过后端调用）
  const [draftMs, setDraftMs] = useState<number | null>(currentMs);

  useEffect(() => {
    if (open) {
      setDraftMs(currentMs);
    }
  }, [open, currentMs]);

  const canSubmit = draftMs != null && draftMs !== currentMs;

  return (
    <Modal
      title="修改保留时间"
      open={open}
      onCancel={onClose}
      onOk={() => {
        if (draftMs == null) return;
        onSubmit(draftMs);
      }}
      okText="保存"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: !canSubmit }}
    >
      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Descriptions column={1} size="small">
          <Descriptions.Item label="当前保留时间">
            {formatDurationMs(currentMs)}
            {currentMs != null && (
              <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
                (retention.ms = {currentMs})
              </Typography.Text>
            )}
          </Descriptions.Item>
        </Descriptions>
        <DurationInput value={draftMs} onChange={setDraftMs} />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          注意：留空或与当前值一致不会触发更新；保留时间还受 retention.bytes 限制；修改后已有消息不会立即清理（log cleaner 异步执行）。
        </Typography.Text>
      </Space>
    </Modal>
  );
}
