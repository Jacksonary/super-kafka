import { useState } from "react";
import {
  Button,
  Card,
  Empty,
  Popconfirm,
  Space,
  Tag,
  Typography,
  theme,
  App as AntdApp,
} from "antd";
import { PlusOutlined, EditOutlined, DeleteOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import type { ClusterConfig } from "../types";
import { useClusterStore } from "../store/clusterStore";
import ClusterFormModal from "../components/Cluster/ClusterFormModal";

const { Text } = Typography;

export default function Cluster() {
  const { message } = AntdApp.useApp();
  const { token } = theme.useToken();
  const navigate = useNavigate();
  const { clusters, refreshClusters, currentClusterId } = useClusterStore();
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ClusterConfig | null>(null);

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

  return (
    <Card
      title="Cluster Management"
      extra={
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
          Add Cluster
        </Button>
      }
    >
      {clusters.length === 0 ? (
        <Empty description="No clusters configured. Add one to get started." />
      ) : (
        <Space direction="vertical" size={12} style={{ width: "100%" }}>
          {clusters.map((c) => {
            const active = c.id === currentClusterId;
            return (
              <Card
                key={c.id}
                hoverable
                styles={{ body: { padding: 16, cursor: "pointer" } }}
                style={{
                  borderColor: active ? token.colorSuccess : token.colorBorder,
                  boxShadow: active ? `0 0 0 1px ${token.colorSuccess}` : undefined,
                }}
                onClick={() => navigate(`/cluster/${encodeURIComponent(c.id)}`)}
              >
                {/* row 1: name + actions */}
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    marginBottom: 8,
                  }}
                >
                  <Space size={8}>
                    <Text strong>{c.name}</Text>
                    {active && <Tag color="success">Active</Tag>}
                  </Space>
                  <Space size={4} onClick={(e) => e.stopPropagation()}>
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
                </div>

                {/* row 2: bootstrap servers */}
                <div style={{ marginBottom: 6 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Bootstrap
                  </Text>
                  <Text code ellipsis style={{ fontSize: 12, display: "block" }}>
                    {c.bootstrap_servers}
                  </Text>
                </div>

                {/* row 3: security protocol */}
                <div>
                  <Tag>{c.security_protocol}</Tag>
                </div>
              </Card>
            );
          })}
        </Space>
      )}

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
