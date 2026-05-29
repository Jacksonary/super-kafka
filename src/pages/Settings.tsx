import { useEffect, useState } from "react";
import {
  Button,
  Card,
  Space,
  Table,
  Tag,
  Popconfirm,
  Typography,
  App as AntdApp,
  Tooltip,
} from "antd";
import { PlusOutlined, EditOutlined, DeleteOutlined, ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { api } from "../api";
import type { ClusterConfig, ClusterSummary } from "../types";
import { useClusterStore } from "../store/clusterStore";
import ClusterFormModal from "../components/Cluster/ClusterFormModal";

const { Text } = Typography;

function StatusTag({ status }: { status: ClusterSummary["status"] | undefined }) {
  switch (status) {
    case "connected":
      return <Tag color="green">Connected</Tag>;
    case "connecting":
      return <Tag color="orange">Connecting</Tag>;
    case "error":
      return <Tag color="red">Error</Tag>;
    case "disconnected":
      return <Tag color="default">Disconnected</Tag>;
    default:
      return <Tag color="default">Unknown</Tag>;
  }
}

export default function Settings() {
  const { message } = AntdApp.useApp();
  const { clusters, refreshClusters } = useClusterStore();
  const [summaries, setSummaries] = useState<Record<string, ClusterSummary>>({});
  const [loadingSummaries, setLoadingSummaries] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ClusterConfig | null>(null);

  async function loadSummaries(list: ClusterConfig[]) {
    setLoadingSummaries(true);
    try {
      const entries = await Promise.all(
        list.map(async (c) => [c.id, await api.getClusterSummary(c.id)] as const),
      );
      setSummaries(Object.fromEntries(entries));
    } finally {
      setLoadingSummaries(false);
    }
  }

  useEffect(() => {
    if (clusters.length > 0) {
      void loadSummaries(clusters);
    } else {
      setSummaries({});
    }
  }, [clusters]);

  async function handleDelete(id: string) {
    try {
      await api.deleteCluster(id);
      message.success("Cluster deleted");
      await refreshClusters();
    } catch (e) {
      if (e instanceof Error) message.error(e.message);
    }
  }

  function handleAdd() {
    setEditing(null);
    setModalOpen(true);
  }
  function handleEdit(c: ClusterConfig) {
    setEditing(c);
    setModalOpen(true);
  }

  const columns: ColumnsType<ClusterConfig> = [
    {
      title: "Status",
      key: "status",
      width: 130,
      render: (_, c) => <StatusTag status={summaries[c.id]?.status} />,
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (n: string) => <Text strong>{n}</Text>,
    },
    {
      title: "Bootstrap Servers",
      dataIndex: "bootstrap_servers",
      key: "bootstrap_servers",
      ellipsis: true,
      render: (s: string) => (
        <Tooltip title={s}>
          <Text code style={{ fontSize: 12 }}>
            {s}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: "Security",
      dataIndex: "security_protocol",
      key: "security_protocol",
      width: 130,
      render: (p: string) => <Tag>{p}</Tag>,
    },
    {
      title: "Brokers",
      key: "brokers",
      width: 90,
      render: (_, c) => summaries[c.id]?.broker_count ?? "-",
    },
    {
      title: "Version",
      key: "version",
      width: 100,
      render: (_, c) => summaries[c.id]?.kafka_version ?? "-",
    },
    {
      title: "Created",
      dataIndex: "created_at",
      key: "created_at",
      width: 170,
      render: (t: number) => dayjs(t).format("YYYY-MM-DD HH:mm"),
    },
    {
      title: "Actions",
      key: "actions",
      width: 150,
      fixed: "right",
      render: (_, c) => (
        <Space size={4}>
          <Button size="small" icon={<EditOutlined />} onClick={() => handleEdit(c)}>
            Edit
          </Button>
          <Popconfirm
            title="Delete this cluster?"
            description="The credential in keychain will also be removed."
            onConfirm={() => handleDelete(c.id)}
            okButtonProps={{ danger: true }}
          >
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      title="Cluster Management"
      extra={
        <Space>
          <Button
            icon={<ReloadOutlined />}
            loading={loadingSummaries}
            onClick={() => loadSummaries(clusters)}
          >
            Refresh Status
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
            Add Cluster
          </Button>
        </Space>
      }
    >
      <Table<ClusterConfig>
        rowKey="id"
        columns={columns}
        dataSource={clusters}
        pagination={false}
        size="middle"
        scroll={{ x: 900 }}
      />

      <ClusterFormModal
        open={modalOpen}
        initialConfig={editing}
        onClose={() => setModalOpen(false)}
        onSaved={async () => {
          await refreshClusters();
        }}
      />
    </Card>
  );
}
