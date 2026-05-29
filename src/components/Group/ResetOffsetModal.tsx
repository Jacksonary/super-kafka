import { useEffect, useState } from "react";
import { DatePicker, Form, InputNumber, Modal, Select, App as AntdApp } from "antd";
import type { Dayjs } from "dayjs";
import { api } from "../../api";
import type { ResetOffsetStrategy } from "../../types";

interface Props {
  open: boolean;
  clusterId: string;
  groupId: string;
  topics: string[];
  onClose: () => void;
}

type Strategy = "earliest" | "latest" | "to_offset" | "to_timestamp";

export default function ResetOffsetModal({ open, clusterId, groupId, topics, onClose }: Props) {
  const { message } = AntdApp.useApp();
  const [strategy, setStrategy] = useState<Strategy>("earliest");
  const [topic, setTopic] = useState<string | null>(null);
  const [partition, setPartition] = useState<number>(0);
  const [offset, setOffset] = useState<number>(0);
  const [timestamp, setTimestamp] = useState<Dayjs | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    setStrategy("earliest");
    setTopic(topics[0] ?? null);
    setPartition(0);
    setOffset(0);
    setTimestamp(null);
  }, [open, topics]);

  async function handleOk() {
    if (!topic) {
      message.warning("Select a topic");
      return;
    }
    let s: ResetOffsetStrategy;
    if (strategy === "earliest") s = { type: "earliest" };
    else if (strategy === "latest") s = { type: "latest" };
    else if (strategy === "to_offset") s = { type: "to_offset", partition, offset };
    else s = { type: "to_timestamp", timestamp_ms: timestamp?.valueOf() ?? Date.now() };

    setSubmitting(true);
    try {
      await api.resetOffset({ cluster_id: clusterId, group_id: groupId, topic, strategy: s });
      message.success("Offset reset");
      onClose();
    } catch (e) {
      if (e instanceof Error) message.error(e.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Modal
      title={`Reset Offset — ${groupId}`}
      open={open}
      onOk={handleOk}
      onCancel={onClose}
      confirmLoading={submitting}
      destroyOnClose
    >
      <Form layout="vertical">
        <Form.Item label="Topic" required>
          <Select
            value={topic ?? undefined}
            onChange={setTopic}
            options={topics.map((t) => ({ value: t, label: t }))}
            placeholder="Select topic"
          />
        </Form.Item>
        <Form.Item label="Strategy" required>
          <Select
            value={strategy}
            onChange={setStrategy}
            options={[
              { value: "earliest", label: "Earliest" },
              { value: "latest", label: "Latest" },
              { value: "to_offset", label: "To specific offset" },
              { value: "to_timestamp", label: "To timestamp" },
            ]}
          />
        </Form.Item>
        {strategy === "to_offset" && (
          <>
            <Form.Item label="Partition">
              <InputNumber
                min={0}
                value={partition}
                onChange={(v) => setPartition(v ?? 0)}
                style={{ width: "100%" }}
              />
            </Form.Item>
            <Form.Item label="Offset">
              <InputNumber
                min={0}
                value={offset}
                onChange={(v) => setOffset(v ?? 0)}
                style={{ width: "100%" }}
              />
            </Form.Item>
          </>
        )}
        {strategy === "to_timestamp" && (
          <Form.Item label="Timestamp">
            <DatePicker
              showTime
              value={timestamp ?? undefined}
              onChange={setTimestamp}
              style={{ width: "100%" }}
            />
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
