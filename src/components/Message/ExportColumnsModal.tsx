import { useEffect, useRef, useState } from "react";
import {
  Button,
  Form,
  Input,
  InputNumber,
  Modal,
  Progress,
  Space,
  Tooltip,
  Typography,
  App as AntdApp,
} from "antd";
import { DeleteOutlined, PlusOutlined } from "@ant-design/icons";
import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api";
import type { ExportColumn, FetchMode, KafkaMessage } from "../../types";

interface Props {
  open: boolean;
  clusterId: string;
  topic: string;
  partition: number | null;
  /** 当前预览使用的 fetch 条件，导出复用它 */
  fetchMode: FetchMode;
  /** 用于推断默认列的样本消息（当前已加载的预览数据） */
  sampleMessages: KafkaMessage[];
  onClose: () => void;
}

const { Text } = Typography;

/** 从首条可解析为 JSON 对象的消息里推断顶层字段，作为默认列 */
function inferColumns(messages: KafkaMessage[]): ExportColumn[] {
  for (const m of messages) {
    if (m.value_text == null) continue;
    try {
      const parsed = JSON.parse(m.value_text);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        const keys = Object.keys(parsed as Record<string, unknown>);
        if (keys.length > 0) return keys.map((k) => ({ name: k, path: "" }));
      }
    } catch {
      // 跳过非 JSON 消息
    }
  }
  return [{ name: "", path: "" }];
}

export default function ExportColumnsModal({
  open,
  clusterId,
  topic,
  partition,
  fetchMode,
  sampleMessages,
  onClose,
}: Props) {
  const { message } = AntdApp.useApp();
  const [columns, setColumns] = useState<ExportColumn[]>([{ name: "", path: "" }]);
  const [maxRecords, setMaxRecords] = useState<number | null>(null);
  const [exporting, setExporting] = useState(false);
  const [written, setWritten] = useState(0);
  const sessionIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (open) {
      setColumns(inferColumns(sampleMessages));
      setMaxRecords(null);
      setWritten(0);
      setExporting(false);
    }
  }, [open, sampleMessages]);

  function updateColumn(idx: number, patch: Partial<ExportColumn>) {
    setColumns((cols) => cols.map((c, i) => (i === idx ? { ...c, ...patch } : c)));
  }
  function addColumn() {
    setColumns((cols) => [...cols, { name: "", path: "" }]);
  }
  function removeColumn(idx: number) {
    setColumns((cols) => cols.filter((_, i) => i !== idx));
  }

  async function handleExport() {
    const valid = columns.filter((c) => c.name.trim());
    if (valid.length === 0) {
      message.warning("At least one column with a name is required");
      return;
    }
    if (!topic) {
      message.warning("Select a topic first");
      return;
    }

    const isoDate = new Date().toISOString().slice(0, 19).replace(/:/g, "-");
    const path = await save({
      defaultPath: `${topic}_${isoDate}.csv`,
      filters: [{ name: "CSV", extensions: ["csv"] }],
    });
    if (!path) return; // 用户取消保存框

    const sessionId = crypto.randomUUID();
    sessionIdRef.current = sessionId;
    setExporting(true);
    setWritten(0);
    try {
      await api.exportMessages(
        {
          cluster_id: clusterId,
          topic,
          partition,
          fetch_mode: fetchMode,
          max_records: maxRecords,
          columns: valid,
          out_path: path,
        },
        sessionId,
        (p) => {
          setWritten(p.written);
          if (p.done) {
            if (p.error) message.error(`Export failed: ${p.error}`);
            else if (p.cancelled) message.warning(`Export cancelled (${p.written} rows written)`);
            else message.success(`Exported ${p.written} rows`);
          }
        },
      );
      onClose();
    } catch (e) {
      message.error(`Export failed: ${String(e)}`);
    } finally {
      setExporting(false);
      sessionIdRef.current = null;
    }
  }

  async function handleCancel() {
    const sid = sessionIdRef.current;
    if (exporting && sid) {
      await api.stopExport(sid);
      return;
    }
    onClose();
  }

  return (
    <Modal
      title="Export messages to CSV"
      open={open}
      onCancel={handleCancel}
      maskClosable={!exporting}
      closable={!exporting}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          {exporting ? "Cancel export" : "Close"}
        </Button>,
        <Button key="export" type="primary" loading={exporting} onClick={handleExport}>
          Export
        </Button>,
      ]}
      width={560}
      destroyOnClose
    >
      <Text type="secondary">
        Only the message value (JSON) is exported. Define each CSV column by its name and an
        optional property path. Leave the path empty to use the column name. Nested paths use dots,
        e.g. <Text code>adress.country</Text>. Export reuses the current fetch conditions.
      </Text>
      <Form layout="vertical" style={{ marginTop: 16 }}>
        <Space style={{ display: "flex", marginBottom: 4 }}>
          <Text strong style={{ width: 220, display: "inline-block" }}>
            Column name
          </Text>
          <Text strong style={{ width: 220, display: "inline-block" }}>
            Property path (optional)
          </Text>
        </Space>
        {columns.map((col, idx) => (
          <Space key={idx} style={{ display: "flex", marginBottom: 8 }} align="baseline">
            <Input
              placeholder="e.g. name"
              value={col.name}
              disabled={exporting}
              onChange={(e) => updateColumn(idx, { name: e.target.value })}
              style={{ width: 220 }}
            />
            <Input
              placeholder="default = column name"
              value={col.path}
              disabled={exporting}
              onChange={(e) => updateColumn(idx, { path: e.target.value })}
              style={{ width: 220 }}
            />
            <Tooltip title="Remove column">
              <Button
                type="text"
                icon={<DeleteOutlined />}
                disabled={columns.length === 1 || exporting}
                onClick={() => removeColumn(idx)}
              />
            </Tooltip>
          </Space>
        ))}
        <Button type="dashed" icon={<PlusOutlined />} onClick={addColumn} block disabled={exporting}>
          Add column
        </Button>

        <Form.Item label="Max records (empty = all available)" style={{ marginTop: 16 }}>
          <InputNumber
            min={1}
            value={maxRecords ?? undefined}
            disabled={exporting}
            onChange={(v) => setMaxRecords(v ?? null)}
            placeholder="All"
            style={{ width: 200 }}
          />
        </Form.Item>

        {exporting && (
          <div>
            <Progress percent={maxRecords ? Math.min(100, Math.round((written / maxRecords) * 100)) : undefined} status="active" showInfo={!!maxRecords} />
            <Text type="secondary">{written} rows written…</Text>
          </div>
        )}
      </Form>
    </Modal>
  );
}
