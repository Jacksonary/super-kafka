import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Breadcrumb,
  Button,
  Card,
  Descriptions,
  Input,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { ArrowLeftOutlined, ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { PartitionInfo, TopicConfig, TopicDetail as TopicDetailType } from "../types";
import { formatNumber } from "../utils/format";
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
        return (
          <Input
            value={current}
            onChange={(e) => setEditingConfig((p) => ({ ...p, [c.name]: e.target.value }))}
            size="small"
          />
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
                    {detail.partitions.length}
                  </Descriptions.Item>
                  <Descriptions.Item label="Replication Factor">
                    {detail.partitions[0]?.replicas.length ?? "-"}
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
    </Spin>
  );
}
