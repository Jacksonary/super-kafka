import { useMemo, useState } from "react";
import {
  Button,
  Card,
  Empty,
  Popconfirm,
  Space,
  Tag,
  Tooltip,
  Typography,
  App as AntdApp,
} from "antd";
import { PlusOutlined, EditOutlined, DeleteOutlined, CheckCircleFilled } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import type { ClusterConfig } from "../types";
import { useClusterStore } from "../store/clusterStore";
import ClusterFormModal from "../components/Cluster/ClusterFormModal";

const { Text } = Typography;

export default function Cluster() {
  const { message } = AntdApp.useApp();
  const navigate = useNavigate();
  const { clusters, refreshClusters, currentClusterId, setCurrentClusterId } = useClusterStore();
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ClusterConfig | null>(null);

  // HashMap-backed list_configs returns an unstable order; sort by creation time
  // so cards keep a stable, predictable order across restarts and edits.
  const sortedClusters = useMemo(
    () => [...clusters].sort((a, b) => a.created_at - b.created_at),
    [clusters],
  );

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
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
            gap: 12,
          }}
        >
          {sortedClusters.map((c) => {
            const active = c.id === currentClusterId;
            return (
              <Card
                key={c.id}
                hoverable
                size="small"
                styles={{ body: { padding: 12, cursor: "pointer" } }}
                onClick={() => navigate(`/cluster/${encodeURIComponent(c.id)}`)}
              >
                {/* row 1: name + actions */}
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: 8,
                    marginBottom: 6,
                  }}
                >
                  <Text strong ellipsis style={{ minWidth: 0, flex: 1 }}>
                    {c.name}
                  </Text>
                  <Space size={2} onClick={(e) => e.stopPropagation()}>
                    {active ? (
                      <Tag icon={<CheckCircleFilled />} color="success" style={{ marginInlineEnd: 0 }}>
                        Active
                      </Tag>
                    ) : (
                      <Tooltip title="Switch to this cluster">
                        <Tag
                          color="default"
                          style={{ marginInlineEnd: 0, cursor: "pointer" }}
                          onClick={() => setCurrentClusterId(c.id)}
                        >
                          Set active
                        </Tag>
                      </Tooltip>
                    )}
                    <Button type="text" size="small" icon={<EditOutlined />} onClick={() => handleEdit(c)} />
                    <Popconfirm
                      title="Delete this cluster?"
                      description="The credential in keychain will also be removed."
                      onConfirm={() => handleDelete(c.id)}
                      okButtonProps={{ danger: true }}
                    >
                      <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                    </Popconfirm>
                  </Space>
                </div>

                {/* row 2: address */}
                <Text
                  code
                  ellipsis={{ tooltip: c.bootstrap_servers }}
                  style={{ fontSize: 12, display: "block", marginBottom: 6 }}
                >
                  {c.bootstrap_servers}
                </Text>

                {/* row 3: security protocol */}
                <Tag style={{ marginInlineEnd: 0 }}>{c.security_protocol}</Tag>
              </Card>
            );
          })}
        </div>
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
