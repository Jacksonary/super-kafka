import { useCallback, useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Progress,
  Select,
  Space,
  Tag,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, PlusOutlined, SendOutlined } from "@ant-design/icons";
import { api } from "../api";
import { useClusterStore } from "../store/clusterStore";
import type { CompressionCodec, KafkaMessage, MessageHeader, TopicSummary } from "../types";

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
  const [compression, setCompression] = useState<CompressionCodec>("none");
  const [sending, setSending] = useState(false);
  const [repeatCount, setRepeatCount] = useState<number>(1);
  const [sendProgress, setSendProgress] = useState<{ done: number; total: number } | null>(null);
  const location = useLocation();

  const partitionOptions = (() => {
    const t = topics.find((x) => x.name === topic);
    if (!t) return null;
    return [
      { value: -1, label: "Auto" },
      ...Array.from({ length: t.partition_count }, (_, i) => ({ value: i, label: "Partition " + i })),
    ];
  })();

  useEffect(() => {
    if (!currentClusterId) return;
    void api
      .listTopics(currentClusterId)
      .then(setTopics)
      .catch((e) => message.error(String(e)));
  }, [currentClusterId, message]);

  useEffect(() => {
    const state = location.state as { replayMessage?: KafkaMessage; replayTopic?: string } | null;
    if (!state?.replayMessage) return;
    const msg = state.replayMessage;
    if (state.replayTopic) setTopic(state.replayTopic);
    setKeyText(msg.key_text ?? "");
    setValueText(msg.value_text ?? "");
    setHeaders(msg.headers.map((h) => ({ key: h.key, value: h.value ?? "" })));
    window.history.replaceState({}, "");
  }, [location.state]);

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
    if (!currentClusterId || !topic) { message.warning("Select a topic first"); return; }
    if (!valueText.trim()) { message.warning("Value cannot be empty"); return; }
    setSending(true);
    const req = {
      cluster_id: currentClusterId,
      topic,
      partition,
      key: keyText || null,
      value: valueText,
      headers: headers.filter((h) => h.key.trim() !== ""),
      compression,
    };
    try {
      if (repeatCount <= 1) {
        await api.produceMessage(req);
        message.success("Message sent");
      } else {
        setSendProgress({ done: 0, total: repeatCount });
        for (let i = 0; i < repeatCount; i++) {
          await api.produceMessage(req);
          setSendProgress({ done: i + 1, total: repeatCount });
        }
        message.success("Sent " + repeatCount + " messages");
        setSendProgress(null);
      }
    } catch (e) {
      message.error(String(e));
      setSendProgress(null);
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
          <Form.Item label="Partition">
            <Select
              style={{ width: 140 }}
              value={partition ?? -1}
              onChange={(v: number) => setPartition(v === -1 ? null : v)}
              options={partitionOptions ?? [{ value: -1, label: "Auto" }]}
              disabled={partitionOptions === null}
            />
          </Form.Item>
          <Form.Item label="Compression">
            <Select
              value={compression}
              onChange={setCompression}
              style={{ width: 120 }}
              options={[
                { value: "none", label: "None" },
                { value: "gzip", label: "gzip" },
                { value: "snappy", label: "snappy" },
                { value: "lz4", label: "lz4" },
                { value: "zstd", label: "zstd" },
              ]}
            />
          </Form.Item>
          <Form.Item label="Repeat">
            <InputNumber
              min={1}
              max={1000}
              value={repeatCount}
              onChange={(v) => setRepeatCount(v ?? 1)}
              style={{ width: 100 }}
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
          <Space style={{ marginBottom: 4 }} size={8}>
            <Button
              size="small"
              onClick={() => {
                try {
                  setValueText(JSON.stringify(JSON.parse(valueText), null, 2));
                } catch {
                  message.warning("Not valid JSON");
                }
              }}
            >
              Format JSON
            </Button>
            {valueText.trim() !== "" && (() => {
              try { JSON.parse(valueText); return <Tag color="green">Valid JSON</Tag>; }
              catch { return <Tag color="default">Plain text</Tag>; }
            })()}
          </Space>
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
          <Space direction="vertical" style={{ width: "100%" }}>
            <Button
              type="primary"
              icon={<SendOutlined />}
              loading={sending}
              onClick={handleSend}
            >
              Send Message
            </Button>
            {sendProgress && (
              <Progress
                percent={Math.round((sendProgress.done / sendProgress.total) * 100)}
                status="active"
                size="small"
                style={{ maxWidth: 300 }}
              />
            )}
          </Space>
        </Form.Item>
      </Form>
    </Card>
  );
}
