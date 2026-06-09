import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { Button, Descriptions, Drawer, Empty, Input, Space, Tag, Tooltip, Typography, message as antMessage, theme } from "antd";
import { CopyOutlined, DownOutlined, SearchOutlined, SendOutlined, UpOutlined } from "@ant-design/icons";
import type { InputRef } from "antd";
import type { KafkaMessage } from "../../types";
import { formatTimestamp } from "../../utils/format";

const { Text } = Typography;

interface Props {
  open: boolean;
  message: KafkaMessage | null;
  onClose: () => void;
  onReplay?: (msg: KafkaMessage, topic: string | null) => void;
  topic?: string | null;
}

function tryFormatJson(text: string | null): string | null {
  if (!text) return null;
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function usePreStyle(): CSSProperties {
  const { token } = theme.useToken();
  return {
    background: token.colorFillQuaternary,
    border: `1px solid ${token.colorBorderSecondary}`,
    borderRadius: 4,
    padding: 12,
    margin: 0,
    whiteSpace: "pre-wrap",
    wordBreak: "break-all",
    fontSize: 12,
    maxHeight: 360,
    overflow: "auto",
    color: token.colorText,
  };
}

function CodeBlock({ children }: { children: string }) {
  return <pre style={usePreStyle()}>{children}</pre>;
}

/**
 * Value 展示区：复制 + 文本搜索高亮 + 上一个/下一个定位。
 * 搜索大小写不敏感；当前命中用不同底色区分，并自动滚动到可视区域。
 */
function ValueViewer({ text }: { text: string }) {
  const preStyle = usePreStyle();
  const { token } = theme.useToken();
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);
  const searchRef = useRef<InputRef>(null);
  const activeMarkRef = useRef<HTMLElement | null>(null);

  // 计算所有匹配区间，并据此把文本切成 高亮/非高亮 片段
  const { segments, matchCount } = useMemo(() => {
    const q = query;
    if (!q) return { segments: [{ text, match: false }], matchCount: 0 };
    const lower = text.toLowerCase();
    const ql = q.toLowerCase();
    const segs: { text: string; match: boolean }[] = [];
    let from = 0;
    let count = 0;
    for (;;) {
      const at = lower.indexOf(ql, from);
      if (at === -1) {
        segs.push({ text: text.slice(from), match: false });
        break;
      }
      if (at > from) segs.push({ text: text.slice(from, at), match: false });
      segs.push({ text: text.slice(at, at + ql.length), match: true });
      count += 1;
      from = at + ql.length;
    }
    return { segments: segs, matchCount: count };
  }, [text, query]);

  // query 变化时重置当前命中索引
  useEffect(() => {
    setActiveIdx(0);
  }, [query]);

  // 当前命中滚动到可视区域
  useEffect(() => {
    activeMarkRef.current?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [activeIdx, query]);

  function goNext() {
    if (matchCount > 0) setActiveIdx((i) => (i + 1) % matchCount);
  }
  function goPrev() {
    if (matchCount > 0) setActiveIdx((i) => (i - 1 + matchCount) % matchCount);
  }

  // 渲染片段，高亮匹配项；给第 activeIdx 个匹配挂 ref 以便滚动
  let matchSeen = -1;
  const rendered = segments.map((seg, i) => {
    if (!seg.match) return seg.text;
    matchSeen += 1;
    const isActive = matchSeen === activeIdx;
    return (
      <mark
        key={i}
        ref={isActive ? activeMarkRef : undefined}
        style={{
          background: isActive ? token.colorWarning : token.colorWarningBorder,
          color: token.colorTextLightSolid,
          borderRadius: 2,
          padding: "0 1px",
        }}
      >
        {seg.text}
      </mark>
    );
  });

  return (
    <div style={{ position: "relative" }}>
      <pre style={preStyle}>{rendered}</pre>

      <Space style={{ position: "absolute", top: 6, right: 6 }} size={2}>
        <Tooltip title="Search in value">
          <Button
            size="small"
            type="text"
            icon={<SearchOutlined />}
            onClick={() => {
              setSearchOpen((v) => !v);
              window.setTimeout(() => searchRef.current?.focus(), 0);
            }}
          />
        </Tooltip>
        <Tooltip title="Copy value">
          <Button
            size="small"
            type="text"
            icon={<CopyOutlined />}
            onClick={() => {
              navigator.clipboard.writeText(text).then(() => {
                void antMessage.success("Copied");
              });
            }}
          />
        </Tooltip>
      </Space>

      {searchOpen && (
        <Space.Compact style={{ position: "absolute", top: 36, right: 6, zIndex: 1 }}>
          <Input
            ref={searchRef}
            size="small"
            allowClear
            placeholder="Find"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                if (e.shiftKey) goPrev();
                else goNext();
              } else if (e.key === "Escape") {
                setSearchOpen(false);
              }
            }}
            style={{ width: 160 }}
            suffix={
              <Text type="secondary" style={{ fontSize: 12 }}>
                {matchCount > 0 ? `${activeIdx + 1}/${matchCount}` : query ? "0/0" : ""}
              </Text>
            }
          />
          <Tooltip title="Previous (Shift+Enter)">
            <Button size="small" icon={<UpOutlined />} disabled={matchCount === 0} onClick={goPrev} />
          </Tooltip>
          <Tooltip title="Next (Enter)">
            <Button size="small" icon={<DownOutlined />} disabled={matchCount === 0} onClick={goNext} />
          </Tooltip>
        </Space.Compact>
      )}
    </div>
  );
}

export default function MessageDetailDrawer({ open, message, onClose, onReplay, topic }: Props) {
  return (
    <Drawer
      title="Message Detail"
      placement="right"
      width={640}
      open={open}
      onClose={onClose}
      destroyOnClose
    >
      {!message ? (
        <Empty description="No message selected" />
      ) : (
        <Space direction="vertical" size={16} style={{ width: "100%" }}>
          <Descriptions column={2} size="small" bordered>
            <Descriptions.Item label="Partition">{message.partition}</Descriptions.Item>
            <Descriptions.Item label="Offset">{message.offset}</Descriptions.Item>
            <Descriptions.Item label="Timestamp" span={2}>
              {formatTimestamp(message.timestamp)}{" "}
              {message.timestamp_type && (
                <Tag style={{ marginLeft: 6 }}>{message.timestamp_type}</Tag>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Encoding">
              <Tag color="blue">{message.value_encoding}</Tag>
            </Descriptions.Item>
          </Descriptions>

          <div>
            <Text strong>Key</Text>
            {message.key_text ? (
              <CodeBlock>{tryFormatJson(message.key_text) ?? message.key_text}</CodeBlock>
            ) : (
              <Text type="secondary"> (null)</Text>
            )}
          </div>

          <div>
            <Text strong>Value</Text>
            <ValueViewer
              text={tryFormatJson(message.value_text) ?? message.value_text ?? "(binary)"}
            />
          </div>

          <div>
            <Text strong>Headers</Text>
            {message.headers.length === 0 ? (
              <div>
                <Text type="secondary"> (none)</Text>
              </div>
            ) : (
              <Descriptions
                column={1}
                size="small"
                bordered
                style={{ marginTop: 8 }}
              >
                {message.headers.map((h, idx) => (
                  <Descriptions.Item key={idx} label={h.key}>
                    <Text code>{h.value ?? "(null)"}</Text>
                  </Descriptions.Item>
                ))}
              </Descriptions>
            )}
          </div>
          {onReplay && (
            <Button
              icon={<SendOutlined />}
              disabled={message.value_text === null}
              onClick={() => onReplay(message, topic ?? null)}
            >
              Replay in Producer
            </Button>
          )}
        </Space>
      )}
    </Drawer>
  );
}
