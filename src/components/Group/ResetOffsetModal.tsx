import { useEffect, useState } from "react";
import { DatePicker, Form, InputNumber, Modal, Select, Space, Typography, App as AntdApp } from "antd";
import dayjs, { type Dayjs } from "dayjs";
import { api } from "../../api";
import type { ResetOffsetStrategy } from "../../types";

const { Text } = Typography;

interface PartitionWatermark {
  start_offset: number;
  log_end_offset: number;
}

interface Props {
  open: boolean;
  clusterId: string;
  groupId: string;
  topics: string[];
  /** Single-partition reset mode: lock to this partition (pass topics=[theTopic]). */
  fixedPartition?: number;
  /** Total partitions of the topic — shown in the confirm step for an all-partition reset. */
  partitionCount?: number;
  /** Watermarks for the fixed partition — bounds the offset input. */
  partitionWatermark?: PartitionWatermark;
  onClose: () => void;
}

/** Derived from the wire type so a strategy can't be added in only one place. */
type Strategy = ResetOffsetStrategy["type"];

const STRATEGY_OPTIONS: { value: Strategy; label: string }[] = [
  { value: "earliest", label: "Earliest" },
  { value: "latest", label: "Latest" },
  { value: "to_offset", label: "Specific offset" },
  { value: "to_timestamp", label: "By timestamp" },
];

// A single offset is meaningless across partitions with differing ranges, so it
// is only offered in single-partition mode.
const ALL_PARTITIONS_STRATEGY_OPTIONS = STRATEGY_OPTIONS.filter((o) => o.value !== "to_offset");

function strategyLabel(
  strategy: Strategy,
  topic: string,
  partition: number | undefined,
  partitionCount: number | undefined,
): string {
  const target =
    partition !== undefined
      ? `partition ${partition} of "${topic}"`
      : partitionCount != null
        ? `all ${partitionCount} partitions of "${topic}"`
        : `all partitions of "${topic}"`;
  switch (strategy) {
    case "earliest":
      return `Reset ${target} to earliest`;
    case "latest":
      return `Reset ${target} to latest`;
    case "to_offset":
      return `Reset ${target} to a specific offset`;
    case "to_timestamp":
      return `Reset ${target} by timestamp`;
  }
}

interface ResetOutcome {
  partitions: number;
  fellBack: number;
  unresolved: number;
  agedOut: number;
  clamped: number;
}

/**
 * Describe an outcome that did not land exactly where the user asked, or null
 * when it did. Each cause gets its own wording: a broker that could not answer
 * is not the same event as "there is nothing after that time", and saying
 * "every partition" reads as a lie when only one was targeted.
 *
 * Checked most-actionable first — `unresolved` is retryable, so it must not be
 * reported as a fact about the data.
 */
function resetSummary(
  strategy: Strategy,
  o: ResetOutcome,
): { title: string; body: string } | null {
  const { partitions, fellBack, unresolved, agedOut, clamped } = o;

  if (unresolved > 0) {
    return {
      title: "Could not resolve the requested position — set to latest",
      body:
        unresolved === partitions
          ? "The broker did not return a position for the requested time, so the offsets were set to latest and nothing already on the log will be consumed. This is usually transient — re-check the offsets and try again."
          : `${unresolved} of ${partitions} partitions could not be resolved and were set to their latest offsets. This is usually transient — retry to position those correctly.`,
    };
  }
  if (agedOut > 0) {
    return {
      title: "Requested time is older than the retained data",
      body:
        agedOut === partitions
          ? "Messages from that time have already been deleted by retention, so the group was set to the oldest offset still available."
          : `${agedOut} of ${partitions} partitions no longer retain messages from that time and were set to their oldest available offsets.`,
    };
  }
  if (strategy === "to_timestamp" && fellBack > 0) {
    if (fellBack === partitions) {
      return {
        title: "No data at or after that time — set to latest",
        body:
          partitions === 1
            ? "That partition has no message at or after the requested time, so it was set to its latest offset and will consume nothing already on the log."
            : `No partition has a message at or after the requested time, so all ${partitions} were set to their latest offsets. The group will skip everything already on this topic.`,
      };
    }
    return {
      title: "Offsets reset, with adjustments",
      body: `${fellBack} of ${partitions} partitions had no message at or after the requested time and were set to their latest offsets.`,
    };
  }
  if (strategy === "to_offset" && clamped > 0) {
    return {
      title: "Offset adjusted to the log range",
      body: "The offset you entered lies outside the partition's available range, so the nearest valid offset was committed instead.",
    };
  }
  return null;
}

export default function ResetOffsetModal({
  open,
  clusterId,
  groupId,
  topics,
  fixedPartition,
  partitionCount,
  partitionWatermark,
  onClose,
}: Props) {
  const { message, modal } = AntdApp.useApp();
  const [strategy, setStrategy] = useState<Strategy>("earliest");
  const [topic, setTopic] = useState<string | null>(null);
  const [offset, setOffset] = useState<number>(0);
  const [timestamp, setTimestamp] = useState<Dayjs | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const single = fixedPartition != null;
  // Depend on primitives, not the `topics` array / watermark object: the caller
  // passes fresh literals every render, which would re-run the reset effect and
  // silently discard a strategy or timestamp the user had already picked.
  const defaultTopic = topics[0] ?? null;
  // A degenerate range means the watermarks are unusable — either the partition
  // is empty or its watermark read failed and the backend substituted (0, 0).
  // Advertising and enforcing it would reject every offset the user tries, so
  // fall back to no hint and no client-side bound; `reset_offset` clamps
  // authoritatively against watermarks it fetches itself and reports any change.
  const usableRange =
    partitionWatermark != null &&
    partitionWatermark.log_end_offset > partitionWatermark.start_offset
      ? partitionWatermark
      : undefined;
  const startOffset = usableRange?.start_offset;
  const endOffset = usableRange?.log_end_offset;

  useEffect(() => {
    if (!open) return;
    setStrategy("earliest");
    setTopic(defaultTopic);
    // Start inside the InputNumber bounds.
    setOffset(startOffset ?? 0);
    setTimestamp(null);
    setSubmitting(false);
  }, [open, defaultTopic, fixedPartition, startOffset]);

  async function doReset(chosen: ResetOffsetStrategy, targetTopic: string) {
    let succeeded = false;
    setSubmitting(true);
    try {
      const { ok, warnings, ...outcome } = await api.resetOffset({
        cluster_id: clusterId,
        group_id: groupId,
        topic: targetTopic,
        partition: single ? fixedPartition : undefined,
        strategy: chosen,
      });
      if (!ok) {
        void message.error("Offset reset was rejected");
        return;
      }
      succeeded = true;

      // Counted server-side per cause; warnings.length is not a usable proxy
      // because partitions with nothing useful to report are omitted.
      const summary = resetSummary(chosen.type, outcome);
      if (!summary) {
        void message.success("Offset reset successfully");
      } else {
        modal.warning({
          title: summary.title,
          content: (
            <Space direction="vertical" size={8} style={{ marginTop: 4, width: "100%" }}>
              <Text>{summary.body}</Text>
              {warnings.length > 0 && (
                <div style={{ maxHeight: 320, overflowY: "auto" }}>
                  {warnings.map((w, i) => (
                    <div key={i}>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {w}
                      </Text>
                    </div>
                  ))}
                </div>
              )}
            </Space>
          ),
        });
      }
    } catch (e) {
      void message.error(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
    if (succeeded) {
      // Outside the try so a parent refetch failure is not reported as if the
      // reset had failed, and in its own guard so a throw here cannot reject
      // this promise and strand the confirm dialog spinning.
      try {
        onClose();
      } catch {
        /* the parent surfaces its own refresh errors */
      }
    }
  }

  function handleOk() {
    if (!topic) {
      void message.warning("Select a topic");
      return;
    }

    let chosen: ResetOffsetStrategy;
    if (strategy === "earliest") {
      chosen = { type: "earliest" };
    } else if (strategy === "latest") {
      chosen = { type: "latest" };
    } else if (strategy === "to_offset") {
      // Don't advertise a valid range and then submit outside it — the backend
      // would clamp the value and report an adjustment for a self-inflicted miss.
      if (startOffset != null && endOffset != null && (offset < startOffset || offset > endOffset)) {
        void message.warning(
          `Offset must be between ${startOffset.toLocaleString()} and ${endOffset.toLocaleString()}`,
        );
        return;
      }
      chosen = { type: "to_offset", offset };
    } else {
      if (!timestamp) {
        void message.warning("Select a timestamp");
        return;
      }
      // disabledDate only blocks whole future days, so today plus a future clock
      // time slips through. A future timestamp matches no message on any
      // partition and silently degrades into a reset-to-latest.
      if (timestamp.isAfter(dayjs())) {
        void message.warning("Timestamp cannot be in the future");
        return;
      }
      chosen = { type: "to_timestamp", timestamp_ms: timestamp.valueOf() };
    }

    modal.confirm({
      title: "Confirm offset reset",
      content: (
        <Space direction="vertical" size={4} style={{ marginTop: 4 }}>
          <Text>{strategyLabel(strategy, topic, fixedPartition, partitionCount)}</Text>
          <Text type="secondary">Group: {groupId}</Text>
          {strategy === "to_offset" && <Text type="secondary">Offset: {offset}</Text>}
          {strategy === "to_timestamp" && timestamp && (
            <Text type="secondary">Time: {timestamp.format("YYYY-MM-DD HH:mm:ss")}</Text>
          )}
          <Text type="warning" style={{ fontSize: 12 }}>
            This cannot be undone — the current committed offsets will be lost.
          </Text>
        </Space>
      ),
      okText: "Confirm Reset",
      okButtonProps: { danger: true },
      onOk: () => doReset(chosen, topic),
    });
  }

  const strategyOptions = single ? STRATEGY_OPTIONS : ALL_PARTITIONS_STRATEGY_OPTIONS;

  return (
    <Modal
      title={
        single
          ? `Reset Offset — ${groupId} · partition ${fixedPartition}`
          : `Reset All Partitions — ${groupId}`
      }
      open={open}
      onOk={handleOk}
      onCancel={onClose}
      confirmLoading={submitting}
      cancelButtonProps={{ disabled: submitting }}
      // cancelButtonProps alone still leaves ESC, the close icon and the mask as
      // ways to dismiss the dialog mid-commit and fire onClose twice.
      closable={!submitting}
      keyboard={!submitting}
      maskClosable={!submitting}
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
          <Select value={strategy} onChange={(v) => setStrategy(v)} options={strategyOptions} />
        </Form.Item>

        {strategy === "to_offset" && (
          <Form.Item
            label="Offset"
            extra={
              startOffset != null && endOffset != null ? (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Valid range: {startOffset.toLocaleString()} – {endOffset.toLocaleString()}
                </Text>
              ) : undefined
            }
          >
            <InputNumber
              min={startOffset ?? 0}
              max={endOffset}
              step={1}
              // Offsets are integers; a decimal would fail deserialization in the
              // backend with a confusing "specific requires `offset`".
              precision={0}
              value={offset}
              onChange={(v) => setOffset(v ?? 0)}
              style={{ width: "100%" }}
            />
          </Form.Item>
        )}

        {strategy === "to_timestamp" && (
          <Form.Item label="Timestamp" required>
            <DatePicker
              showTime
              value={timestamp}
              onChange={setTimestamp}
              style={{ width: "100%" }}
              disabledDate={(d) => d.isAfter(dayjs(), "day")}
              placeholder="Select date and time"
            />
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
