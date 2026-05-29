import { useEffect, useMemo, useState } from "react";
import {
  Modal,
  Form,
  Input,
  InputNumber,
  Select,
  Button,
  Space,
  Divider,
  Alert,
  App as AntdApp,
  Typography,
} from "antd";
import { CheckCircleFilled, CloseCircleFilled } from "@ant-design/icons";
import { api } from "../../api";
import type {
  ClusterConfig,
  SaslMechanism,
  SecurityProtocol,
  TestConnectionResult,
} from "../../types";
import { uuidv4 } from "../../utils/format";

const { Text } = Typography;

interface FormValues {
  name: string;
  bootstrap_servers: string;
  security_protocol: SecurityProtocol;
  sasl_mechanism: SaslMechanism | null;
  sasl_username: string | null;
  sasl_password: string | null;
  ssl_ca_cert_path: string | null;
  ssl_client_cert_path: string | null;
  ssl_client_key_path: string | null;
  schema_registry_url: string | null;
  schema_registry_username: string | null;
  connect_url: string | null;
  request_timeout_ms: number;
}

interface Props {
  open: boolean;
  initialConfig: ClusterConfig | null;
  onClose: () => void;
  onSaved: () => void;
}

export default function ClusterFormModal({ open, initialConfig, onClose, onSaved }: Props) {
  const [form] = Form.useForm<FormValues>();
  const { message } = AntdApp.useApp();
  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<TestConnectionResult | null>(null);
  const [securityProtocol, setSecurityProtocol] = useState<SecurityProtocol>("PLAINTEXT");

  const isEdit = !!initialConfig;

  useEffect(() => {
    if (!open) return;
    setTestResult(null);
    if (initialConfig) {
      form.setFieldsValue({
        name: initialConfig.name,
        bootstrap_servers: initialConfig.bootstrap_servers,
        security_protocol: initialConfig.security_protocol,
        sasl_mechanism: initialConfig.sasl_mechanism,
        sasl_username: initialConfig.sasl_username,
        sasl_password: null,
        ssl_ca_cert_path: initialConfig.ssl_ca_cert_path,
        ssl_client_cert_path: initialConfig.ssl_client_cert_path,
        ssl_client_key_path: initialConfig.ssl_client_key_path,
        schema_registry_url: initialConfig.schema_registry_url,
        schema_registry_username: initialConfig.schema_registry_username,
        connect_url: initialConfig.connect_url,
        request_timeout_ms: initialConfig.request_timeout_ms,
      });
      setSecurityProtocol(initialConfig.security_protocol);
    } else {
      form.resetFields();
      form.setFieldsValue({
        security_protocol: "PLAINTEXT",
        request_timeout_ms: 30000,
      });
      setSecurityProtocol("PLAINTEXT");
    }
  }, [open, initialConfig, form]);

  const showSasl = useMemo(
    () => securityProtocol === "SASL_PLAINTEXT" || securityProtocol === "SASL_SSL",
    [securityProtocol],
  );
  const showSsl = useMemo(
    () => securityProtocol === "SSL" || securityProtocol === "SASL_SSL",
    [securityProtocol],
  );

  function buildConfig(values: FormValues): { config: ClusterConfig; password: string | null } {
    const config: ClusterConfig = {
      id: initialConfig?.id ?? uuidv4(),
      name: values.name.trim(),
      bootstrap_servers: values.bootstrap_servers.trim(),
      security_protocol: values.security_protocol,
      sasl_mechanism: showSasl ? values.sasl_mechanism ?? null : null,
      sasl_username: showSasl ? values.sasl_username || null : null,
      ssl_ca_cert_path: showSsl ? values.ssl_ca_cert_path || null : null,
      ssl_client_cert_path: showSsl ? values.ssl_client_cert_path || null : null,
      ssl_client_key_path: showSsl ? values.ssl_client_key_path || null : null,
      schema_registry_url: values.schema_registry_url || null,
      schema_registry_username: values.schema_registry_username || null,
      connect_url: values.connect_url || null,
      request_timeout_ms: values.request_timeout_ms ?? 30000,
      created_at: initialConfig?.created_at ?? Date.now(),
    };
    return { config, password: values.sasl_password || null };
  }

  async function handleTest() {
    try {
      const values = await form.validateFields();
      setTesting(true);
      setTestResult(null);
      const { config, password } = buildConfig(values);
      const result = await api.testConnection(config, password);
      setTestResult(result);
    } catch (e) {
      if (e instanceof Error) message.error(e.message);
    } finally {
      setTesting(false);
    }
  }

  async function handleSave() {
    try {
      const values = await form.validateFields();
      setSaving(true);
      const { config, password } = buildConfig(values);
      await api.saveCluster(config);
      if (password) {
        await api.saveSaslPassword(config.id, password);
      }
      message.success(isEdit ? "Cluster updated" : "Cluster created");
      onSaved();
      onClose();
    } catch (e) {
      if (e instanceof Error) message.error(e.message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <Modal
      title={isEdit ? `Edit Cluster: ${initialConfig?.name}` : "New Cluster"}
      open={open}
      onCancel={onClose}
      width={680}
      destroyOnClose
      footer={
        <Space>
          <Button onClick={onClose}>Cancel</Button>
          <Button onClick={handleTest} loading={testing}>
            Test Connection
          </Button>
          <Button type="primary" onClick={handleSave} loading={saving}>
            {isEdit ? "Save" : "Create"}
          </Button>
        </Space>
      }
    >
      <Form form={form} layout="vertical" preserve={false}>
        <Form.Item name="name" label="Name" rules={[{ required: true, message: "Name is required" }]}>
          <Input placeholder="Production / Staging / Local Dev" />
        </Form.Item>

        <Form.Item
          name="bootstrap_servers"
          label="Bootstrap Servers"
          rules={[{ required: true, message: "Bootstrap servers required" }]}
          extra="Comma-separated host:port list"
        >
          <Input placeholder="kafka-1:9092,kafka-2:9092" />
        </Form.Item>

        <Form.Item name="security_protocol" label="Security Protocol" rules={[{ required: true }]}>
          <Select
            onChange={(v: SecurityProtocol) => setSecurityProtocol(v)}
            options={[
              { value: "PLAINTEXT", label: "PLAINTEXT" },
              { value: "SSL", label: "SSL" },
              { value: "SASL_PLAINTEXT", label: "SASL_PLAINTEXT" },
              { value: "SASL_SSL", label: "SASL_SSL" },
            ]}
          />
        </Form.Item>

        {showSasl && (
          <>
            <Divider orientation="left" plain>
              <Text type="secondary">SASL</Text>
            </Divider>
            <Form.Item name="sasl_mechanism" label="SASL Mechanism" rules={[{ required: showSasl }]}>
              <Select
                options={[
                  { value: "PLAIN", label: "PLAIN" },
                  { value: "SCRAM-SHA-256", label: "SCRAM-SHA-256" },
                  { value: "SCRAM-SHA-512", label: "SCRAM-SHA-512" },
                  { value: "GSSAPI", label: "GSSAPI (Kerberos)" },
                  { value: "OAUTHBEARER", label: "OAUTHBEARER" },
                ]}
              />
            </Form.Item>
            <Form.Item name="sasl_username" label="SASL Username">
              <Input autoComplete="off" />
            </Form.Item>
            <Form.Item
              name="sasl_password"
              label="SASL Password"
              extra={isEdit ? "Leave empty to keep existing password" : "Stored in OS keychain"}
            >
              <Input.Password autoComplete="new-password" />
            </Form.Item>
          </>
        )}

        {showSsl && (
          <>
            <Divider orientation="left" plain>
              <Text type="secondary">SSL</Text>
            </Divider>
            <Form.Item name="ssl_ca_cert_path" label="CA Certificate Path">
              <Input placeholder="/etc/ssl/certs/ca.pem" />
            </Form.Item>
            <Form.Item name="ssl_client_cert_path" label="Client Certificate Path">
              <Input placeholder="/etc/ssl/certs/client.pem" />
            </Form.Item>
            <Form.Item name="ssl_client_key_path" label="Client Key Path">
              <Input placeholder="/etc/ssl/private/client.key" />
            </Form.Item>
          </>
        )}

        <Divider orientation="left" plain>
          <Text type="secondary">Optional Services</Text>
        </Divider>
        <Form.Item name="schema_registry_url" label="Schema Registry URL">
          <Input placeholder="http://localhost:8081" />
        </Form.Item>
        <Form.Item name="schema_registry_username" label="Schema Registry Username">
          <Input autoComplete="off" />
        </Form.Item>
        <Form.Item name="connect_url" label="Kafka Connect URL">
          <Input placeholder="http://localhost:8083" />
        </Form.Item>
        <Form.Item
          name="request_timeout_ms"
          label="Request Timeout (ms)"
          rules={[{ required: true }]}
        >
          <InputNumber min={1000} max={300000} step={1000} style={{ width: "100%" }} />
        </Form.Item>

        {testResult && (
          <Alert
            type={testResult.success ? "success" : "error"}
            showIcon
            icon={testResult.success ? <CheckCircleFilled /> : <CloseCircleFilled />}
            message={
              testResult.success
                ? `Connected — ${testResult.broker_count} brokers, Kafka ${testResult.kafka_version}, ${testResult.latency_ms}ms`
                : "Connection failed"
            }
            description={testResult.error_message}
            style={{ marginTop: 12 }}
          />
        )}
      </Form>
    </Modal>
  );
}
