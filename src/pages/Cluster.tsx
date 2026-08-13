import { useEffect, useState } from "react";
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
import { PlusOutlined, EditOutlined, DeleteOutlined, LinkOutlined, DisconnectOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import {
  DndContext,
  PointerSensor,
  useSensor,
  useSensors,
  closestCenter,
  type DragEndEvent,
} from "@dnd-kit/core";
import { SortableContext, arrayMove, rectSortingStrategy, useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { api } from "../api";
import type { ClusterConfig } from "../types";
import { useClusterStore } from "../store/clusterStore";
import { useClusterOrder } from "../hooks/useClusterOrder";
import ClusterFormModal from "../components/Cluster/ClusterFormModal";

const { Text } = Typography;

interface SortableCardProps {
  c: ClusterConfig;
  active: boolean;
  onNavigate: (id: string) => void;
  onSetActive: (id: string) => void;
  onEdit: (c: ClusterConfig) => void;
  onDelete: (id: string) => void;
}

function SortableClusterCard({ c, active, onNavigate, onSetActive, onEdit, onDelete }: SortableCardProps) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: c.id });

  return (
    <Card
      ref={setNodeRef}
      hoverable
      size="small"
      styles={{ body: { padding: 12, cursor: isDragging ? "grabbing" : "grab" } }}
      style={{
        transform: CSS.Transform.toString(transform),
        transition,
        opacity: isDragging ? 0.5 : 1,
      }}
      {...attributes}
      {...listeners}
      onClick={() => onNavigate(c.id)}
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
              <Tooltip title="Active">
                <Button type="text" size="small" icon={<LinkOutlined style={{ color: "#52c41a" }} />} />
              </Tooltip>
            ) : (
              <Tooltip title="Set as active">
                <Button type="text" size="small" icon={<DisconnectOutlined />} onClick={() => onSetActive(c.id)} />
              </Tooltip>
            )}
          <Button type="text" size="small" icon={<EditOutlined />} onClick={() => onEdit(c)} />
          <Popconfirm
            title="Delete this cluster?"
            description="The credential in keychain will also be removed."
            onConfirm={() => onDelete(c.id)}
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
}

export default function Cluster() {
  const { message } = AntdApp.useApp();
  const navigate = useNavigate();
  const { clusters, refreshClusters, currentClusterId, setCurrentClusterId, pendingAdd, consumeAddRequest } = useClusterStore();
  const { applySortOrder, saveOrder } = useClusterOrder();
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ClusterConfig | null>(null);

  useEffect(() => {
    if (!pendingAdd) return;
    consumeAddRequest();
    setEditing(null);
    setModalOpen(true);
  }, [pendingAdd, consumeAddRequest]);

  const sortedClusters = applySortOrder(clusters);

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
  );

  async function handleDragEnd({ active, over }: DragEndEvent) {
    if (!over || active.id === over.id) return;
    const oldIndex = sortedClusters.findIndex((c) => c.id === active.id);
    const newIndex = sortedClusters.findIndex((c) => c.id === over.id);
    const reordered = arrayMove(sortedClusters, oldIndex, newIndex);
    await saveOrder(reordered.map((c) => c.id));
    await refreshClusters();
  }

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
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
      styles={{ body: { flex: 1, minHeight: 0, overflow: "auto" } }}
      extra={
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
          Add Cluster
        </Button>
      }
    >
      {clusters.length === 0 ? (
        <Empty description="No clusters configured. Add one to get started." />
      ) : (
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
          <SortableContext items={sortedClusters.map((c) => c.id)} strategy={rectSortingStrategy}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
                gap: 12,
              }}
            >
              {sortedClusters.map((c) => (
                <SortableClusterCard
                  key={c.id}
                  c={c}
                  active={c.id === currentClusterId}
                  onNavigate={(id) => navigate(`/cluster/${encodeURIComponent(id)}`)}
                  onSetActive={setCurrentClusterId}
                  onEdit={handleEdit}
                  onDelete={handleDelete}
                />
              ))}
            </div>
          </SortableContext>
        </DndContext>
      )}

      <ClusterFormModal
        key={editing?.id ?? "__new__"}
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
