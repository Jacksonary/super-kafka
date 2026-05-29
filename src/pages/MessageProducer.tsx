import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Typography,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, PlusOutlined, SendOutlined } from "@ant-design/icons";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { MessageHeader, TopicSummary } from "../types";

const { Text } = Typography;
const { TextArea } = Input;

export default function MessageProducer() {
  const { currentClusterId } = useClusterStore();
  const { message } = AntdApp.useApp();

  const [topics, setTopics] = useState<TopicSummary[]>([]);
  const [topic, setTopic] = useState<string | null>(null);
  const [partition, setPartition] = useState<number | null>(null);
  const [keyText, setKeyText] = useState<string>("");
  const [valueText, setValueText] = useState<string>("");
  const [headers, setHeaders] = useState<MessageHeader[]>([]);
  const [sending, setSending] = useState(false);
  const [lastResult, setLastResult] = useState<{ partition: number; offset: number } | null>(null);

  useEffect(() => {
    if (!currentClusterId) return;
    void api
      .listTopics(currentClusterId)
      .then(setTopics)
      .catch((e) => message.error(String(e)));
  }, [currentClusterId, message]);

  const updateHeader = useCallback((idx: number, patch: Partial<MessageHeader>) => {
    setHeaders((prev) => prev.map((h, i) => (i === idx ? { ...h, ...patch } : h)));
  }, []);
  const addHeader = useCallback(() => {
    setHeaders((prev) => [...prev, { key: "", value: "" }]);
  }, []);
  const removeHeader = useCallback((idx: number) => {
    setHeaders((prev) => prev.filter((_, i) => i !== idx));
  }, []);

  async function handleSend() {
    if (!currentClusterId || !topic) {
      message.warning("Select a topic first");
      return;
    }
    if (!valueText.trim()) {
      message.warning("Value cannot be empty");
      return;
    }
    setSending(true);
    try {
      const res = await api.produceMessage({
        cluster_id: currentClusterId,
        topic,
        partition,
        key: keyText || null,
        value: valueText,
        headers: headers.filter((h) => h.key.trim() !== ""),
      });
      setLastResult(res);
      message.success(`Sent to partition ${res.partition}, offset ${res.offset}`);
    } catch (e) {
      message.error(String(e));
    } finally {
      setSending(false);
    }
  }

  if (!currentClusterId) {
    return <Alert type="info" showIcon message="No cluster selected." />;
  }

  return (
    <Card title="Produce Message" style={{ maxWidth: 900 }}>
      <Form layout="vertical">
        <Space size={12} style={{ width: "100%" }}>
          <Form.Item label="Topic" style={{ flex: 1, minWidth: 280 }}>
            <Select
              showSearch
              value={topic ?? undefined}
              onChange={setTopic}
              placeholder="Select topic"
              options={topics
                .filter((t) => !t.is_internal)
                .map((t) => ({ value: t.name, label: t.name }))}
            />
          </Form.Item>
          <Form.Item label="Partition (optional)">
            <InputNumber
              min={0}
              value={partition ?? undefined}
              onChange={(v) => setPartition(v ?? null)}
              placeholder="auto"
              style={{ width: 140 }}
            />
          </Form.Item>
        </Space>

        <Form.Item label="Key (optional)">
          <Input
            value={keyText}
            onChange={(e) => setKeyText(e.target.value)}
            placeholder="message key"
          />
        </Form.Item>

        <Form.Item label="Value">
          <TextArea
            rows={12}
            value={valueText}
            onChange={(e) => setValueText(e.target.value)}
            placeholder='{"hello": "world"}'
            style={{
              fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace',
              fontSize: 13,
            }}
          />
        </Form.Item>

        <Form.Item label="Headers">
          <Space direction="vertical" style={{ width: "100%" }}>
            {headers.map((h, idx) => (
              <Space key={idx} style={{ width: "100%" }}>
                <Input
                  placeholder="key"
                  value={h.key}
                  onChange={(e) => updateHeader(idx, { key: e.target.value })}
                  style={{ width: 200 }}
                />
                <Input
                  placeholder="value"
                  value={h.value ?? ""}
                  onChange={(e) => updateHeader(idx, { value: e.target.value })}
                  style={{ width: 380 }}
                />
                <Button danger icon={<DeleteOutlined />} onClick={() => removeHeader(idx)} />
              </Space>
            ))}
            <Button icon={<PlusOutlined />} onClick={addHeader} type="dashed">
              Add Header
            </Button>
          </Space>
        </Form.Item>

        <Form.Item>
          <Space>
            <Button
              type="primary"
              icon={<SendOutlined />}
              loading={sending}
              onClick={handleSend}
            >
              Send Message
            </Button>
            {lastResult && (
              <Text type="success">
                Last: partition {lastResult.partition}, offset {lastResult.offset}
              </Text>
            )}
          </Space>
        </Form.Item>
      </Form>
    </Card>
  );
}
