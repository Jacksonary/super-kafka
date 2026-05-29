import { Descriptions, Drawer, Empty, Space, Tag, Typography } from "antd";
import type { KafkaMessage } from "../../types";
import { formatTimestamp } from "../../utils/format";

const { Text } = Typography;

interface Props {
  open: boolean;
  message: KafkaMessage | null;
  onClose: () => void;
}

function tryFormatJson(text: string | null): string | null {
  if (!text) return null;
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre
      style={{
        background: "#0a0e14",
        border: "1px solid #1f242c",
        borderRadius: 4,
        padding: 12,
        margin: 0,
        whiteSpace: "pre-wrap",
        wordBreak: "break-all",
        fontSize: 12,
        maxHeight: 360,
        overflow: "auto",
      }}
    >
      {children}
    </pre>
  );
}

export default function MessageDetailDrawer({ open, message, onClose }: Props) {
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
            <Descriptions.Item label="Compression">
              <Tag color={message.compression_codec === "none" ? "default" : "cyan"}>
                {message.compression_codec}
              </Tag>
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
            <CodeBlock>
              {tryFormatJson(message.value_text) ?? message.value_text ?? "(binary)"}
            </CodeBlock>
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
        </Space>
      )}
    </Drawer>
  );
}
