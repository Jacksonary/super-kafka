import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Space,
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
  const [selected, setSelected] = useState<TopicConsumerGroup | null>(null);
  const [partitions, setPartitions] = useState<PartitionLag[]>([]);
  const [loadingParts, setLoadingParts] = useState(false);
  const [resetTarget, setResetTarget] = useState<{ groupId: string; partition: number } | null>(
    null,
  );

  const loadGroups = useCallback(async () => {
    setLoadingGroups(true);
    try {
      setGroups(await api.listTopicConsumerGroups(clusterId, topic));
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoadingGroups(false);
    }
  }, [clusterId, topic, message]);

  useEffect(() => {
    setSelected(null);
    setPartitions([]);
    void loadGroups();
  }, [loadGroups]);

  const loadPartitions = useCallback(
    async (groupId: string) => {
      setLoadingParts(true);
      try {
        setPartitions(await api.getTopicGroupPartitionLag(clusterId, topic, groupId));
      } catch (e) {
        message.error(String(e));
      } finally {
        setLoadingParts(false);
      }
    },
    [clusterId, topic, message],
  );

  const selectGroup = useCallback(
    (g: TopicConsumerGroup) => {
      setSelected(g);
      void loadPartitions(g.group_id);
    },
    [loadPartitions],
  );

  const canReset = selected?.state === "Empty" || selected?.state === "Dead";

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
        <Text type={v > 1000 ? "danger" : v > 100 ? "warning" : undefined}>{formatNumber(v)}</Text>
      ),
    },
    {
      title: "Action",
      key: "action",
      width: 100,
      render: (_: unknown, p: PartitionLag) => (
        <Tooltip title={canReset ? "" : "Group must be Empty/Dead to reset — stop its consumers first"}>
          <Button
            size="small"
            disabled={!canReset}
            onClick={() =>
              selected && setResetTarget({ groupId: selected.group_id, partition: p.partition })
            }
          >
            Reset
          </Button>
        </Tooltip>
      ),
    },
  ];

  return (
    <Space direction="vertical" size={12} style={{ width: "100%" }}>
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
            onRow={(g) => ({
              onClick: () => selectGroup(g),
              style: { cursor: "pointer" },
            })}
            rowClassName={(g) => (g.group_id === selected?.group_id ? "ant-table-row-selected" : "")}
          />
        )}
      </Card>

      {selected && (
        <Card
          size="small"
          title={
            <Space>
              <Text>Partitions ·</Text>
              <Text code>{selected.group_id}</Text>
              <Tag color={STATE_COLORS[selected.state]}>{selected.state}</Tag>
            </Space>
          }
          extra={
            <Button
              icon={<ReloadOutlined />}
              size="small"
              loading={loadingParts}
              onClick={() => loadPartitions(selected.group_id)}
            >
              Refresh
            </Button>
          }
        >
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
            loading={loadingParts}
            columns={partColumns}
            dataSource={partitions}
            pagination={false}
          />
        </Card>
      )}

      <ResetOffsetModal
        open={resetTarget !== null}
        clusterId={clusterId}
        groupId={resetTarget?.groupId ?? ""}
        topics={[topic]}
        fixedPartition={resetTarget?.partition}
        partitionCount={partitions.length > 0 ? partitions.length : undefined}
        onClose={() => {
          const gid = selected?.group_id;
          setResetTarget(null);
          if (gid) void loadPartitions(gid);
          // Group-list total_lag should reflect the reset too.
          void loadGroups();
        }}
      />
    </Space>
  );
}
