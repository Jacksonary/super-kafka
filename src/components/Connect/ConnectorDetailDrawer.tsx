import { useEffect, useState } from "react";
import { Alert, Drawer, Empty, Space, Spin, Table, Tag, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { api } from "../../api";
import type { ConnectorDetail, ConnectorState, ConnectorTask, TaskState } from "../../types";

const { Text, Title } = Typography;

const STATE_COLORS: Record<ConnectorState | TaskState, string> = {
  RUNNING: "green",
  PAUSED: "orange",
  FAILED: "red",
  UNASSIGNED: "default",
};

interface Props {
  open: boolean;
  clusterId: string;
  connectorName: string | null;
  onClose: () => void;
}

export default function ConnectorDetailDrawer({ open, clusterId, connectorName, onClose }: Props) {
  const [detail, setDetail] = useState<ConnectorDetail | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open || !connectorName) return;
    setLoading(true);
    setDetail(null);
    api
      .getConnectorDetail(clusterId, connectorName)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setLoading(false));
  }, [open, clusterId, connectorName]);

  const taskColumns: ColumnsType<ConnectorTask> = [
    { title: "Task ID", dataIndex: "task_id", key: "task_id", width: 80 },
    {
      title: "State",
      dataIndex: "state",
      key: "state",
      width: 130,
      render: (s: TaskState) => <Tag color={STATE_COLORS[s]}>{s}</Tag>,
    },
    { title: "Worker", dataIndex: "worker_id", key: "worker_id" },
    {
      title: "Error",
      dataIndex: "error_trace",
      key: "error_trace",
      ellipsis: true,
      render: (e: string | null) => (e ? <Text type="danger">{e.split("\n")[0]}</Text> : null),
    },
  ];

  return (
    <Drawer
      title={connectorName ? `Connector: ${connectorName}` : "Connector Detail"}
      placement="right"
      width={720}
      open={open}
      onClose={onClose}
      destroyOnClose
    >
      {loading ? (
        <Spin />
      ) : !detail ? (
        <Empty description="No connector loaded" />
      ) : (
        <Space direction="vertical" size={16} style={{ width: "100%" }}>
          <Space>
            <Tag color={STATE_COLORS[detail.state]}>{detail.state}</Tag>
            <Tag>{detail.connector_type}</Tag>
          </Space>

          {detail.error_trace && (
            <Alert type="error" showIcon message="Connector error" description={detail.error_trace} />
          )}

          <div>
            <Title level={5}>Configuration</Title>
            <Table
              size="small"
              rowKey={(rec) => rec.key}
              pagination={false}
              dataSource={Object.entries(detail.config).map(([k, v]) => ({ key: k, value: v }))}
              columns={[
                {
                  title: "Key",
                  dataIndex: "key",
                  width: 280,
                  render: (k: string) => <Text code style={{ fontSize: 12 }}>{k}</Text>,
                },
                {
                  title: "Value",
                  dataIndex: "value",
                  render: (v: string) => <Text style={{ fontSize: 12 }}>{v}</Text>,
                },
              ]}
            />
          </div>

          <div>
            <Title level={5}>Tasks</Title>
            <Table<ConnectorTask>
              size="small"
              rowKey="task_id"
              pagination={false}
              dataSource={detail.tasks}
              columns={taskColumns}
            />
          </div>
        </Space>
      )}
    </Drawer>
  );
}
