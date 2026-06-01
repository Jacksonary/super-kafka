import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Popconfirm,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, ReloadOutlined, ThunderboltOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type {
  AssignedPartition,
  ConsumerGroupDetail,
  ConsumerGroupState,
  ConsumerGroupSummary,
  GroupMember,
  PartitionLag,
  TopicLag,
} from "../types";
import { formatNumber } from "../utils/format";
import ResetOffsetModal from "../components/Group/ResetOffsetModal";

const { Text } = Typography;

const STATE_COLORS: Record<ConsumerGroupState, string> = {
  Stable: "green",
  Empty: "default",
  Dead: "red",
  PreparingRebalance: "orange",
  CompletingRebalance: "orange",
  Unknown: "default",
};

export default function ConsumerGroups() {
  const { currentClusterId } = useClusterStore();
  const { message } = AntdApp.useApp();
  const [groups, setGroups] = useState<ConsumerGroupSummary[]>([]);
  const [details, setDetails] = useState<Record<string, ConsumerGroupDetail>>({});
  const [loading, setLoading] = useState(false);
  const [resetTarget, setResetTarget] = useState<{ groupId: string; topics: string[] } | null>(null);

  const load = useCallback(async () => {
    if (!currentClusterId) return;
    setLoading(true);
    try {
      const list = await api.listConsumerGroups(currentClusterId);
      setGroups(list);
      setDetails({});
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, message]);

  useEffect(() => {
    void load();
  }, [load]);

  const loadDetail = useCallback(
    async (groupId: string): Promise<ConsumerGroupDetail | null> => {
      if (!currentClusterId) return null;
      if (details[groupId]) return details[groupId];
      try {
        const d = await api.getConsumerGroupDetail(currentClusterId, groupId);
        setDetails((prev) => ({ ...prev, [groupId]: d }));
        return d;
      } catch (e) {
        message.error(String(e));
        return null;
      }
    },
    [currentClusterId, details, message],
  );

  const handleDelete = useCallback(
    async (groupId: string) => {
      if (!currentClusterId) return;
      try {
        await api.deleteConsumerGroup(currentClusterId, groupId);
        message.success(`Deleted group ${groupId}`);
        void load();
      } catch (e) {
        message.error(String(e));
      }
    },
    [currentClusterId, load, message],
  );

  const columns: ColumnsType<ConsumerGroupSummary> = [
    {
      title: "Group ID",
      dataIndex: "group_id",
      key: "group_id",
      render: (g: string) => <Text code style={{ fontSize: 12 }}>{g}</Text>,
    },
    {
      title: "State",
      dataIndex: "state",
      key: "state",
      width: 180,
      render: (s: ConsumerGroupState) => <Tag color={STATE_COLORS[s]}>{s}</Tag>,
    },
    { title: "Members", dataIndex: "member_count", key: "member_count", width: 100, align: "right" },
    {
      title: "Total Lag",
      dataIndex: "total_lag",
      key: "total_lag",
      width: 130,
      align: "right",
      render: (v: number | null) =>
        v == null ? <Text type="secondary">-</Text> : (
          <Text type={v > 10000 ? "danger" : v > 1000 ? "warning" : undefined}>
            {formatNumber(v)}
          </Text>
        ),
    },
    {
      title: "Coordinator",
      dataIndex: "coordinator_id",
      key: "coordinator_id",
      width: 110,
      align: "right",
    },
    {
      title: "Actions",
      key: "actions",
      width: 220,
      render: (_, g) => (
        <Space size={4}>
          <Button
            size="small"
            icon={<ThunderboltOutlined />}
            onClick={async () => {
              const det = await loadDetail(g.group_id);
              const topics = det ? det.topic_lag.map((t) => t.topic) : [];
              setResetTarget({ groupId: g.group_id, topics });
            }}
          >
            Reset Offset
          </Button>
          <Popconfirm
            title="Delete this group?"
            description="The group must be in Empty/Dead state."
            onConfirm={() => handleDelete(g.group_id)}
            okButtonProps={{ danger: true }}
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

  return (
    <Card
      title="Consumer Groups"
      extra={
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
          Refresh
        </Button>
      }
    >
      <Table<ConsumerGroupSummary>
        rowKey="group_id"
        size="middle"
        columns={columns}
        dataSource={groups}
        loading={loading}
        pagination={false}
        expandable={{
          onExpand: (expanded, record) => {
            if (expanded) void loadDetail(record.group_id);
          },
          expandedRowRender: (record) => {
            const detail = details[record.group_id];
            if (!detail) {
              return (
                <div style={{ padding: 12 }}>
                  <Spin size="small" />
                </div>
              );
            }
            return (
              <Space direction="vertical" size={12} style={{ width: "100%" }}>
                {detail.members.length > 0 && <MembersTable members={detail.members} />}
                <TopicLagTable topicLag={detail.topic_lag} />
              </Space>
            );
          },
        }}
      />

      <ResetOffsetModal
        open={resetTarget !== null}
        clusterId={currentClusterId}
        groupId={resetTarget?.groupId ?? ""}
        topics={resetTarget?.topics ?? []}
        onClose={() => setResetTarget(null)}
      />
    </Card>
  );
}

function MembersTable({ members }: { members: GroupMember[] }) {
  return (
    <Card size="small" title={<Text strong>Members ({members.length})</Text>}>
      <Table<GroupMember>
        size="small"
        rowKey="member_id"
        pagination={false}
        dataSource={members}
        columns={[
          { title: "Member ID", dataIndex: "member_id", key: "member_id", ellipsis: true },
          { title: "Client ID", dataIndex: "client_id", key: "client_id", ellipsis: true },
          { title: "Host", dataIndex: "client_host", key: "client_host", width: 160 },
          {
            title: "Assigned Partitions",
            key: "assigned_partitions",
            render: (_: unknown, m: GroupMember) =>
              m.assigned_partitions.length === 0 ? (
                <Text type="secondary">none</Text>
              ) : (
                <Space size={4} wrap>
                  {m.assigned_partitions.map((ap: AssignedPartition) => (
                    <Tag key={`${ap.topic}-${ap.partition}`} bordered={false}>
                      {ap.topic}:{ap.partition}
                    </Tag>
                  ))}
                </Space>
              ),
          },
        ]}
      />
    </Card>
  );
}

function TopicLagTable({ topicLag }: { topicLag: TopicLag[] }) {
  return (
    <Space direction="vertical" size={12} style={{ width: "100%" }}>
      {topicLag.map((tl) => (
        <Card key={tl.topic} size="small" title={<Text code>{tl.topic}</Text>} extra={<Text type="secondary">total lag: {formatNumber(tl.total_lag)}</Text>}>
          <Table<PartitionLag>
            size="small"
            rowKey="partition"
            pagination={false}
            dataSource={tl.partitions}
            columns={[
              { title: "Partition", dataIndex: "partition", width: 100 },
              {
                title: "Current Offset",
                dataIndex: "current_offset",
                align: "right",
                render: formatNumber,
              },
              {
                title: "End Offset",
                dataIndex: "log_end_offset",
                align: "right",
                render: formatNumber,
              },
              {
                title: "Lag",
                dataIndex: "lag",
                align: "right",
                render: (v: number) => (
                  <Text type={v > 1000 ? "danger" : v > 100 ? "warning" : undefined}>
                    {formatNumber(v)}
                  </Text>
                ),
              },
            ]}
          />
        </Card>
      ))}
    </Space>
  );
}
