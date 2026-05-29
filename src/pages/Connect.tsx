import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Popconfirm,
  Space,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import {
  DeleteOutlined,
  PauseOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  RedoOutlined,
} from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { ConnectorState, ConnectorSummary, ConnectorType } from "../types";
import ConnectorDetailDrawer from "../components/Connect/ConnectorDetailDrawer";

const { Text } = Typography;

const STATE_COLORS: Record<ConnectorState, string> = {
  RUNNING: "green",
  PAUSED: "orange",
  FAILED: "red",
  UNASSIGNED: "default",
};

const TYPE_COLORS: Record<ConnectorType, string> = {
  source: "blue",
  sink: "purple",
};

export default function Connect() {
  const { currentClusterId, currentCluster } = useClusterStore();
  const { message } = AntdApp.useApp();
  const [connectors, setConnectors] = useState<ConnectorSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!currentClusterId) return;
    setLoading(true);
    try {
      const list = await api.listConnectors(currentClusterId);
      setConnectors(list);
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, message]);

  useEffect(() => {
    void load();
  }, [load]);

  const action = useCallback(
    async (verb: "pause" | "resume" | "restart" | "delete", name: string) => {
      if (!currentClusterId) return;
      try {
        if (verb === "pause") await api.pauseConnector(currentClusterId, name);
        else if (verb === "resume") await api.resumeConnector(currentClusterId, name);
        else if (verb === "restart") await api.restartConnector(currentClusterId, name, null);
        else if (verb === "delete") await api.deleteConnector(currentClusterId, name);
        message.success(`${verb}: ${name}`);
        void load();
      } catch (e) {
        message.error(String(e));
      }
    },
    [currentClusterId, load, message],
  );

  const columns: ColumnsType<ConnectorSummary> = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (n: string) => (
        <a onClick={() => setSelected(n)}>
          <Text code style={{ fontSize: 12 }}>{n}</Text>
        </a>
      ),
    },
    {
      title: "Type",
      dataIndex: "connector_type",
      key: "connector_type",
      width: 100,
      render: (t: ConnectorType) => <Tag color={TYPE_COLORS[t]}>{t}</Tag>,
    },
    {
      title: "State",
      dataIndex: "state",
      key: "state",
      width: 130,
      render: (s: ConnectorState) => <Tag color={STATE_COLORS[s]}>{s}</Tag>,
    },
    { title: "Tasks", dataIndex: "task_count", key: "task_count", width: 90, align: "right" },
    {
      title: "Failed",
      dataIndex: "failed_tasks",
      key: "failed_tasks",
      width: 90,
      align: "right",
      render: (n: number) =>
        n > 0 ? <Text type="danger">{n}</Text> : <Text type="secondary">0</Text>,
    },
    {
      title: "Class",
      dataIndex: "connector_class",
      key: "connector_class",
      ellipsis: true,
      render: (c: string) => <Text style={{ fontSize: 12 }} code>{c.split(".").pop()}</Text>,
    },
    {
      title: "Actions",
      key: "actions",
      width: 240,
      render: (_, c) => (
        <Space size={4}>
          {c.state === "PAUSED" ? (
            <Button size="small" icon={<PlayCircleOutlined />} onClick={() => action("resume", c.name)}>
              Resume
            </Button>
          ) : (
            <Button size="small" icon={<PauseOutlined />} onClick={() => action("pause", c.name)}>
              Pause
            </Button>
          )}
          <Button size="small" icon={<RedoOutlined />} onClick={() => action("restart", c.name)}>
            Restart
          </Button>
          <Popconfirm
            title={`Delete connector "${c.name}"?`}
            okButtonProps={{ danger: true }}
            onConfirm={() => action("delete", c.name)}
          >
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }

  if (!currentCluster?.connect_url) {
    return (
      <Alert
        type="warning"
        showIcon
        message="Kafka Connect URL not configured"
        description="Set Connect URL in Settings to enable this page."
      />
    );
  }

  return (
    <Card
      title="Kafka Connect"
      extra={
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
          Refresh
        </Button>
      }
    >
      <Table<ConnectorSummary>
        rowKey="name"
        size="middle"
        columns={columns}
        dataSource={connectors}
        loading={loading}
        pagination={false}
        locale={{ emptyText: <Empty description="No connectors" /> }}
      />

      <ConnectorDetailDrawer
        open={selected !== null}
        clusterId={currentClusterId}
        connectorName={selected}
        onClose={() => setSelected(null)}
      />
    </Card>
  );
}
