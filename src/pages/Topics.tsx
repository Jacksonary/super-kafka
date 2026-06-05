import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Empty,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Space,
  Table,
  Tag,
  theme,
  Typography,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, PlusOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { CreateTopicRequest, TopicSummary } from "../types";
import DurationInput from "../components/Common/DurationInput";



export default function Topics() {
  const { currentClusterId } = useClusterStore();
  const navigate = useNavigate();
  const { message } = AntdApp.useApp();
  const { token } = theme.useToken();

  const [topics, setTopics] = useState<TopicSummary[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [search, setSearch] = useState<string>("");
  const [createOpen, setCreateOpen] = useState<boolean>(false);
  const [creating, setCreating] = useState<boolean>(false);
  const [form] = Form.useForm<CreateTopicRequest & { retention_ms: number | null }>();

  const load = useCallback(async () => {
    if (!currentClusterId) return;
    setLoading(true);
    try {
      const data = await api.listTopics(currentClusterId);
      setTopics(data);
    } catch (err) {
      message.error(String(err));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, message]);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return topics;
    return topics.filter((t) => t.name.toLowerCase().includes(q));
  }, [topics, search]);

  const handleCreate = useCallback(async () => {
    if (!currentClusterId) return;
    try {
      const values = await form.validateFields();
      setCreating(true);
      const configs: Record<string, string> = {};
      if (values.retention_ms != null) {
        configs["retention.ms"] = String(values.retention_ms);
      }
      await api.createTopic(currentClusterId, {
        name: values.name,
        partition_count: values.partition_count,
        replication_factor: values.replication_factor,
        configs,
      });
      message.success(`Topic "${values.name}" created`);
      setCreateOpen(false);
      form.resetFields();
      void load();
    } catch (err) {
      if (err instanceof Error) message.error(err.message);
    } finally {
      setCreating(false);
    }
  }, [currentClusterId, form, load, message]);

  const handleDelete = useCallback(
    async (name: string) => {
      if (!currentClusterId) return;
      try {
        await api.deleteTopic(currentClusterId, name);
        message.success(`Topic "${name}" deleted`);
        void load();
      } catch (err) {
        message.error(String(err));
      }
    },
    [currentClusterId, load, message],
  );

  const columns: ColumnsType<TopicSummary> = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name: string, record) => (
        <Space size={6}>
          <a
            onClick={() => navigate(`/topics/${encodeURIComponent(name)}`)}
            style={{
              fontFamily: "ui-monospace, 'JetBrains Mono', Menlo, monospace",
              color: token.colorText,
            }}
          >
            {name}
          </a>
          {record.is_internal && (
            <Tag color="default" bordered={false} style={{ fontSize: 11 }}>
              internal
            </Tag>
          )}
        </Space>
      ),
    },
    {
      title: "Partitions",
      dataIndex: "partition_count",
      key: "partition_count",
      width: 130,
      align: "right",
      sorter: (a, b) => a.partition_count - b.partition_count,
    },
    {
      title: "Replication",
      dataIndex: "replication_factor",
      key: "replication_factor",
      width: 140,
      align: "right",
      sorter: (a, b) => a.replication_factor - b.replication_factor,
    },
    {
      title: "",
      key: "actions",
      width: 60,
      align: "right",
      render: (_, record) => (
        <Popconfirm
          title={`Delete topic "${record.name}"?`}
          description="This action cannot be undone."
          okText="Delete"
          okButtonProps={{ danger: true }}
          onConfirm={() => handleDelete(record.name)}
        >
          <Button type="text" size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ];

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected. Configure one in the Cluster page." />;
  }

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <Space style={{ marginBottom: 12, width: "100%", justifyContent: "space-between", flexShrink: 0 }}>
        <Input
          allowClear
          prefix={<SearchOutlined />}
          placeholder="Filter topics by name"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ width: 320 }}
        />
        <Space>
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
            Refresh
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            Create Topic
          </Button>
        </Space>
      </Space>

      <Table<TopicSummary>
        size="small"
        rowKey="name"
        dataSource={filtered}
        columns={columns}
        loading={loading}
        pagination={{
          defaultPageSize: 20,
          showSizeChanger: true,
          pageSizeOptions: [10, 20, 50],
          showLessItems: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total}`,
        }}
        locale={{
          emptyText: <Empty description="No topics" />,
        }}
        scroll={{ y: "max(200px, calc(100vh - 220px))" }}
      />

      <Modal
        title="Create Topic"
        open={createOpen}
        onOk={handleCreate}
        onCancel={() => setCreateOpen(false)}
        okText="Create"
        confirmLoading={creating}
        destroyOnClose
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{ partition_count: 3, replication_factor: 1 }}
          preserve={false}
        >
          <Form.Item
            label="Name"
            name="name"
            rules={[{ required: true, message: "Name is required" }]}
          >
            <Input placeholder="my-topic" autoFocus />
          </Form.Item>
          <Form.Item
            label="Partitions"
            name="partition_count"
            rules={[{ required: true, message: "Partition count is required" }]}
          >
            <InputNumber min={1} max={10000} style={{ width: "100%" }} />
          </Form.Item>
          <Form.Item
            label="Replication factor"
            name="replication_factor"
            rules={[{ required: true, message: "Replication factor is required" }]}
          >
            <InputNumber min={1} max={10} style={{ width: "100%" }} />
          </Form.Item>
          <Form.Item
            label="Retention"
            name="retention_ms"
            extra="Leave blank to use cluster default. Toggle Forever for indefinite retention (-1)."
          >
            <DurationInput defaultUnit="d" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
