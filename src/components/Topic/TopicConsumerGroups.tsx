import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Spin,
  Table,
  Tag,
  Tooltip,
  Typography,
  App as AntdApp,
} from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../../api";
import type { ConsumerGroupState, PartitionLag, TopicConsumerGroup } from "../../types";
import { formatNumber } from "../../utils/format";
import ResetOffsetModal from "../Group/ResetOffsetModal";

const { Text } = Typography;

const STATE_COLORS: Record<ConsumerGroupState, string> = {
  Stable: "green",
  Empty: "default",
  Dead: "red",
  PreparingRebalance: "orange",
  CompletingRebalance: "orange",
  Unknown: "default",
};

interface Props {
  clusterId: string;
  topic: string;
}

export default function TopicConsumerGroups({ clusterId, topic }: Props) {
  const { message } = AntdApp.useApp();
  const [groups, setGroups] = useState<TopicConsumerGroup[]>([]);
  const [loadingGroups, setLoadingGroups] = useState(false);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [partitions, setPartitions] = useState<Record<string, PartitionLag[]>>({});
  const [loadingParts, setLoadingParts] = useState<Record<string, boolean>>({});
  const [resetTarget, setResetTarget] = useState<{
    group: TopicConsumerGroup;
    partition: number;
  } | null>(null);

  const loadGroups = useCallback(async () => {
    setLoadingGroups(true);
    setExpandedKeys([]);
    setPartitions({});
    setLoadingParts({});
    try {
      setGroups(await api.listTopicConsumerGroups(clusterId, topic));
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoadingGroups(false);
    }
  }, [clusterId, topic, message]);

  useEffect(() => {
    void loadGroups();
  }, [loadGroups]);

  const loadPartitions = useCallback(
    async (groupId: string) => {
      setLoadingParts((prev) => ({ ...prev, [groupId]: true }));
      try {
        const lag = await api.getTopicGroupPartitionLag(clusterId, topic, groupId);
        setPartitions((prev) => ({ ...prev, [groupId]: lag }));
      } catch (e) {
        message.error(String(e));
      } finally {
        setLoadingParts((prev) => ({ ...prev, [groupId]: false }));
      }
    },
    [clusterId, topic, message],
  );

  const groupColumns: ColumnsType<TopicConsumerGroup> = [
    {
      title: "Group ID",
      dataIndex: "group_id",
      key: "group_id",
      render: (g: string) => (
        <Text code style={{ fontSize: 12 }}>
          {g}
        </Text>
      ),
    },
    {
      title: "State",
      dataIndex: "state",
      key: "state",
      width: 180,
      render: (s: ConsumerGroupState) => <Tag color={STATE_COLORS[s]}>{s}</Tag>,
    },
    {
      title: "Lag (this topic)",
      dataIndex: "total_lag",
      key: "total_lag",
      width: 160,
      align: "right",
      render: (v: number) => (
        <Text type={v > 10000 ? "danger" : v > 1000 ? "warning" : undefined}>
          {formatNumber(v)}
        </Text>
      ),
    },
  ];

  const renderPartitions = (group: TopicConsumerGroup) => {
    const canReset = group.state === "Empty" || group.state === "Dead";
    const rows = partitions[group.group_id];

    if (loadingParts[group.group_id] && !rows) {
      return (
        <div style={{ padding: 12 }}>
          <Spin size="small" />
        </div>
      );
    }

    const partColumns: ColumnsType<PartitionLag> = [
      { title: "Partition", dataIndex: "partition", key: "partition", width: 100 },
      {
        title: "Start",
        dataIndex: "start_offset",
        key: "start_offset",
        align: "right",
        render: formatNumber,
      },
      {
        title: "End",
        dataIndex: "log_end_offset",
        key: "log_end_offset",
        align: "right",
        render: formatNumber,
      },
      {
        title: "Current",
        dataIndex: "current_offset",
        key: "current_offset",
        align: "right",
        render: (v: number) => (v < 0 ? <Text type="secondary">-</Text> : formatNumber(v)),
      },
      {
        title: "Lag",
        dataIndex: "lag",
        key: "lag",
        align: "right",
        render: (v: number) => (
          <Text type={v > 1000 ? "danger" : v > 100 ? "warning" : undefined}>
            {formatNumber(v)}
          </Text>
        ),
      },
      {
        title: "Action",
        key: "action",
        width: 100,
        render: (_: unknown, p: PartitionLag) => (
          <Tooltip
            title={canReset ? "" : "Group must be Empty/Dead to reset — stop its consumers first"}
          >
            <Button
              size="small"
              disabled={!canReset}
              onClick={() => setResetTarget({ group, partition: p.partition })}
            >
              Reset
            </Button>
          </Tooltip>
        ),
      },
    ];

    return (
      <div style={{ padding: "4px 0" }}>
        {!canReset && (
          <Alert
            type="info"
            showIcon
            style={{ marginBottom: 8 }}
            message="Offsets can only be reset when the group is Empty (no active consumers)."
          />
        )}
        <Table<PartitionLag>
          rowKey="partition"
          size="small"
          loading={loadingParts[group.group_id] ?? false}
          columns={partColumns}
          dataSource={rows ?? []}
          pagination={false}
        />
      </div>
    );
  };

  return (
    <Card
      size="small"
      title={<Text strong>Consumer Groups</Text>}
      extra={
        <Button icon={<ReloadOutlined />} size="small" loading={loadingGroups} onClick={loadGroups}>
          Refresh
        </Button>
      }
    >
      {groups.length === 0 && !loadingGroups ? (
        <Empty description="No consumer group consumes this topic" />
      ) : (
        <Table<TopicConsumerGroup>
          rowKey="group_id"
          size="small"
          loading={loadingGroups}
          columns={groupColumns}
          dataSource={groups}
          pagination={false}
          expandable={{
            showExpandColumn: false,
            expandedRowKeys: expandedKeys,
            expandedRowRender: renderPartitions,
          }}
          onRow={(g) => ({
            onClick: () => {
              const isOpen = expandedKeys.includes(g.group_id);
              setExpandedKeys((prev) =>
                isOpen ? prev.filter((k) => k !== g.group_id) : [...prev, g.group_id],
              );
              if (!isOpen && !partitions[g.group_id]) {
                void loadPartitions(g.group_id);
              }
            },
            style: { cursor: "pointer" },
          })}
        />
      )}

      <ResetOffsetModal
        open={resetTarget !== null}
        clusterId={clusterId}
        groupId={resetTarget?.group.group_id ?? ""}
        topics={[topic]}
        fixedPartition={resetTarget?.partition}
        onClose={() => {
          const gid = resetTarget?.group.group_id;
          setResetTarget(null);
          if (gid) void loadPartitions(gid);
          void loadGroups();
        }}
      />
    </Card>
  );
}
