import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Breadcrumb,
  Button,
  Card,
  Descriptions,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
  App as AntdApp,
} from "antd";
import { ArrowLeftOutlined, ReloadOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { useNavigate, useParams } from "react-router-dom";
import dayjs from "dayjs";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { BrokerInfo, ClusterSummary } from "../types";

const { Text } = Typography;

function StatusTag({ status }: { status: ClusterSummary["status"] | undefined }) {
  switch (status) {
    case "connected":
      return <Tag color="green">Connected</Tag>;
    case "connecting":
      return <Tag color="orange">Connecting</Tag>;
    case "error":
      return <Tag color="red">Error</Tag>;
    case "disconnected":
      return <Tag color="default">Disconnected</Tag>;
    default:
      return <Tag color="default">Unknown</Tag>;
  }
}

export default function ClusterDetail() {
  const { clusterId: rawId } = useParams<{ clusterId: string }>();
  const clusterId = rawId ? decodeURIComponent(rawId) : "";
  const navigate = useNavigate();
  const { clusters } = useClusterStore();
  const { message } = AntdApp.useApp();

  const config = clusters.find((c) => c.id === clusterId) ?? null;

  const [summary, setSummary] = useState<ClusterSummary | null>(null);
  const [brokers, setBrokers] = useState<BrokerInfo[]>([]);
  const [brokerError, setBrokerError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    if (!clusterId) return;
    setLoading(true);
    setBrokerError(null);
    const [sumRes, brokerRes] = await Promise.allSettled([
      api.getClusterSummary(clusterId),
      api.listBrokers(clusterId),
    ]);
    if (sumRes.status === "fulfilled") {
      setSummary(sumRes.value);
    } else {
      setSummary(null);
      message.error(String(sumRes.reason));
    }
    if (brokerRes.status === "fulfilled") {
      setBrokers(brokerRes.value);
    } else {
      setBrokers([]);
      setBrokerError(String(brokerRes.reason));
    }
    setLoading(false);
  }, [clusterId, message]);

  useEffect(() => {
    void load();
  }, [load]);

  if (!config) {
    return (
      <Space direction="vertical" size={12} style={{ width: "100%" }}>
        <Breadcrumb
          items={[
            { title: <a onClick={() => navigate("/cluster")}>Cluster</a> },
            { title: "Not found" },
          ]}
        />
        <Alert
          type="warning"
          showIcon
          message="Cluster not found"
          description="It may have been deleted. Go back to the cluster list."
          action={
            <Button size="small" onClick={() => navigate("/cluster")}>
              Back
            </Button>
          }
        />
      </Space>
    );
  }

  const brokerColumns: ColumnsType<BrokerInfo> = [
    { title: "ID", dataIndex: "id", key: "id", width: 100 },
    { title: "Host", dataIndex: "host", key: "host" },
    { title: "Port", dataIndex: "port", key: "port", width: 120 },
  ];

  return (
    <Space direction="vertical" size={12} style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <a onClick={() => navigate("/cluster")}>Cluster</a> },
          { title: config.name },
        ]}
      />

      <Space style={{ justifyContent: "space-between", width: "100%" }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate("/cluster")}>
          Back
        </Button>
        <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void load()}>
          Refresh
        </Button>
      </Space>

      <Spin spinning={loading}>
        <Card size="small" title="Connection" style={{ marginBottom: 12 }}>
          <Descriptions column={2} size="small">
            <Descriptions.Item label="Status">
              <StatusTag status={summary?.status} />
            </Descriptions.Item>
            <Descriptions.Item label="Kafka Version">
              {summary?.kafka_version ?? "-"}
            </Descriptions.Item>
            <Descriptions.Item label="Broker Count">
              {summary?.broker_count ?? "-"}
            </Descriptions.Item>
            {summary?.error_message && (
              <Descriptions.Item label="Error" span={2}>
                <Text type="danger">{summary.error_message}</Text>
              </Descriptions.Item>
            )}
          </Descriptions>
        </Card>

        <Card size="small" title="Brokers" style={{ marginBottom: 12 }}>
          {brokerError ? (
            <Alert type="error" showIcon message="Cannot list brokers" description={brokerError} />
          ) : (
            <Table<BrokerInfo>
              rowKey="id"
              size="small"
              columns={brokerColumns}
              dataSource={brokers}
              pagination={false}
            />
          )}
        </Card>

        <Card size="small" title="Security" style={{ marginBottom: 12 }}>
          <Descriptions column={2} size="small">
            <Descriptions.Item label="Protocol">
              <Tag>{config.security_protocol}</Tag>
            </Descriptions.Item>
            {config.sasl_mechanism && (
              <Descriptions.Item label="SASL Mechanism">{config.sasl_mechanism}</Descriptions.Item>
            )}
            {config.sasl_username && (
              <Descriptions.Item label="SASL Username">{config.sasl_username}</Descriptions.Item>
            )}
            {config.ssl_ca_cert_path && (
              <Descriptions.Item label="CA Cert" span={2}>
                <Text code style={{ fontSize: 12 }}>{config.ssl_ca_cert_path}</Text>
              </Descriptions.Item>
            )}
            {config.ssl_client_cert_path && (
              <Descriptions.Item label="Client Cert" span={2}>
                <Text code style={{ fontSize: 12 }}>{config.ssl_client_cert_path}</Text>
              </Descriptions.Item>
            )}
            {config.ssl_client_key_path && (
              <Descriptions.Item label="Client Key" span={2}>
                <Text code style={{ fontSize: 12 }}>{config.ssl_client_key_path}</Text>
              </Descriptions.Item>
            )}
          </Descriptions>
        </Card>

        <Card size="small" title="Config">
          <Descriptions column={2} size="small">
            <Descriptions.Item label="Name">{config.name}</Descriptions.Item>
            <Descriptions.Item label="Request Timeout">
              {config.request_timeout_ms} ms
            </Descriptions.Item>
            <Descriptions.Item label="Bootstrap Servers" span={2}>
              <Text code style={{ fontSize: 12 }}>{config.bootstrap_servers}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="Created">
              {dayjs(config.created_at).format("YYYY-MM-DD HH:mm")}
            </Descriptions.Item>
            <Descriptions.Item label="ID">
              <Text code style={{ fontSize: 12 }}>{config.id}</Text>
            </Descriptions.Item>
          </Descriptions>
        </Card>
      </Spin>
    </Space>
  );
}
