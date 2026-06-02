import { useEffect, useState } from "react";
import { Form, InputNumber, Modal, Select, App as AntdApp } from "antd";
import { api } from "../../api";
import type { ResetOffsetStrategy } from "../../types";

interface Props {
  open: boolean;
  clusterId: string;
  groupId: string;
  topics: string[];
  /** Single-partition reset mode: lock to this partition (pass topics=[theTopic]). */
  fixedPartition?: number;
  onClose: () => void;
}

type Strategy = "earliest" | "latest" | "to_offset";

export default function ResetOffsetModal({
  open,
  clusterId,
  groupId,
  topics,
  fixedPartition,
  onClose,
}: Props) {
  const { message } = AntdApp.useApp();
  const [strategy, setStrategy] = useState<Strategy>("earliest");
  const [topic, setTopic] = useState<string | null>(null);
  const [partition, setPartition] = useState<number>(0);
  const [offset, setOffset] = useState<number>(0);
  const [submitting, setSubmitting] = useState(false);

  const single = fixedPartition != null;

  useEffect(() => {
    if (!open) return;
    setStrategy("earliest");
    setTopic(topics[0] ?? null);
    setPartition(fixedPartition ?? 0);
    setOffset(0);
  }, [open, topics, fixedPartition]);

  async function handleOk() {
    if (!topic) {
      message.warning("Select a topic");
      return;
    }
    const effPartition = fixedPartition ?? partition;
    let s: ResetOffsetStrategy;
    if (strategy === "earliest") s = { type: "earliest" };
    else if (strategy === "latest") s = { type: "latest" };
    else s = { type: "to_offset", partition: effPartition, offset };

    setSubmitting(true);
    try {
      await api.resetOffset({
        cluster_id: clusterId,
        group_id: groupId,
        topic,
        partition: fixedPartition,
        strategy: s,
      });
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
      title={
        single
          ? `Reset Offset — ${groupId} · partition ${fixedPartition}`
          : `Reset Offset — ${groupId}`
      }
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
            disabled={single}
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
            ]}
          />
        </Form.Item>
        {strategy === "to_offset" && (
          <>
            {!single && (
              <Form.Item label="Partition">
                <InputNumber
                  min={0}
                  value={partition}
                  onChange={(v) => setPartition(v ?? 0)}
                  style={{ width: "100%" }}
                />
              </Form.Item>
            )}
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
      </Form>
    </Modal>
  );
}
