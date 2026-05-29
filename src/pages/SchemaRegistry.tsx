import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Empty,
  Space,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { SchemaSubject, SchemaType, SchemaVersion } from "../types";

const { Text, Title } = Typography;

const TYPE_COLORS: Record<SchemaType, string> = {
  AVRO: "blue",
  PROTOBUF: "purple",
  JSON: "cyan",
};

export default function SchemaRegistry() {
  const { currentClusterId } = useClusterStore();
  const { message } = AntdApp.useApp();
  const [subjects, setSubjects] = useState<SchemaSubject[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<SchemaVersion | null>(null);
  const [loadingVersion, setLoadingVersion] = useState(false);

  const load = useCallback(async () => {
    if (!currentClusterId) return;
    setLoading(true);
    try {
      const list = await api.listSchemaSubjects(currentClusterId);
      setSubjects(list);
      setSelected(null);
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoading(false);
    }
  }, [currentClusterId, message]);

  useEffect(() => {
    void load();
  }, [load]);

  async function handleSelect(subject: SchemaSubject) {
    if (!currentClusterId) return;
    setLoadingVersion(true);
    try {
      const v = await api.getSchemaVersion(currentClusterId, subject.name, "latest");
      setSelected(v);
    } catch (e) {
      message.error(String(e));
    } finally {
      setLoadingVersion(false);
    }
  }

  const columns: ColumnsType<SchemaSubject> = [
    {
      title: "Subject",
      dataIndex: "name",
      key: "name",
      render: (n: string, rec) => (
        <a onClick={() => void handleSelect(rec)}>
          <Text code style={{ fontSize: 12 }}>{n}</Text>
        </a>
      ),
    },
    {
      title: "Type",
      dataIndex: "schema_type",
      key: "schema_type",
      width: 130,
      render: (t: SchemaType) => <Tag color={TYPE_COLORS[t]}>{t}</Tag>,
    },
    { title: "Versions", dataIndex: "version_count", key: "version_count", width: 110, align: "right" },
    { title: "Latest", dataIndex: "latest_version", key: "latest_version", width: 110, align: "right" },
  ];

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }

  return (
    <Space direction="vertical" size={16} style={{ width: "100%" }}>
      <Card
        title="Schema Subjects"
        extra={
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
            Refresh
          </Button>
        }
      >
        <Table<SchemaSubject>
          rowKey="name"
          size="middle"
          columns={columns}
          dataSource={subjects}
          loading={loading}
          pagination={false}
          locale={{ emptyText: <Empty description="No schemas" /> }}
        />
      </Card>

      {selected && (
        <Card
          title={
            <Space>
              <Title level={5} style={{ margin: 0 }}>
                <Text code>{selected.subject}</Text>
              </Title>
              <Tag color={TYPE_COLORS[selected.schema_type]}>{selected.schema_type}</Tag>
              <Tag>v{selected.version}</Tag>
              <Tag color="default">id: {selected.id}</Tag>
            </Space>
          }
          loading={loadingVersion}
        >
          <pre
            style={{
              background: "#0a0e14",
              border: "1px solid #1f242c",
              borderRadius: 4,
              padding: 12,
              margin: 0,
              fontSize: 12,
              maxHeight: 480,
              overflow: "auto",
            }}
          >
            {selected.schema}
          </pre>
        </Card>
      )}
    </Space>
  );
}
