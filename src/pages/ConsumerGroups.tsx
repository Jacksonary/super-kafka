import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Input,
  Popconfirm,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type {
  AssignedPartition,
  ConsumerGroupDetail,
  ConsumerGroupState,
  ConsumerGroupSummary,
  GroupMember,
} from "../types";

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
  const [search, setSearch] = useState<string>("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return groups;
    return groups.filter((g) => g.group_id.toLowerCase().includes(q));
  }, [groups, search]);

  const load = useCallback(async () => {
    if (!currentClusterId) return;
    setLoading(true);
    try {
      const list = await api.listConsumerGroups(currentClusterId);
      // Backend returns groups in broker fetch-list order, which is not stable
      // across refreshes; sort by group_id for a predictable order.
      list.sort((a, b) => a.group_id.localeCompare(b.group_id));
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
    async (groupId: string) => {
      if (!currentClusterId) return;
      try {
        const d = await api.getConsumerGroupDetail(currentClusterId, groupId);
        setDetails((prev) => ({ ...prev, [groupId]: d }));
      } catch (e) {
        message.error(String(e));
      }
    },
    [currentClusterId, message],
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
      render: (g: string) => <Text code>{g}</Text>,
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
      title: "Actions",
      key: "actions",
      width: 100,
      render: (_, g) => (
        <Popconfirm
          title="Delete this group?"
          description="The group must be in Empty/Dead state."
          onConfirm={() => handleDelete(g.group_id)}
          okButtonProps={{ danger: true }}
        >
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }

  return (
    <Card
      title="Consumer Groups"
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
      styles={{ body: { flex: 1, minHeight: 0, overflow: "hidden", padding: 0 } }}
      extra={
        <Space>
          <Input
            allowClear
            prefix={<SearchOutlined />}
            placeholder="Filter groups by ID"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ width: 260 }}
          />
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
            Refresh
          </Button>
        </Space>
      }
    >
      <Table<ConsumerGroupSummary>
        rowKey="group_id"
        size="middle"
        columns={columns}
        dataSource={filtered}
        loading={loading}
        pagination={false}
        scroll={{ y: "max(200px, calc(100vh - 200px))" }}
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
            if (detail.members.length === 0) {
              return (
                <div style={{ padding: 12 }}>
                  <Text type="secondary">No active members (group is {detail.state}).</Text>
                </div>
              );
            }
            return <MembersTable members={detail.members} />;
          },
        }}
      />
    </Card>
  );
}

function MembersTable({ members }: { members: GroupMember[] }) {
  return (
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
  );
}
