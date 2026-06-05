import { Button, Descriptions, Drawer, Empty, Space, Tag, Tooltip, Typography, message as antMessage, theme } from "antd";
import { CopyOutlined, SendOutlined } from "@ant-design/icons";
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

function CodeBlock({ children }: { children: string }) {
  const { token } = theme.useToken();
  return (
    <pre
      style={{
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
      }}
    >
      {children}
    </pre>
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
            <div style={{ position: "relative" }}>
              <CodeBlock>
                {tryFormatJson(message.value_text) ?? message.value_text ?? "(binary)"}
              </CodeBlock>
              <Space style={{ position: "absolute", top: 6, right: 6 }} size={2}>
                <Tooltip title="Copy full message as JSON">
                  <Button
                    size="small"
                    type="text"
                    icon={<CopyOutlined />}
                    onClick={() => {
                      const obj = {
                        partition: message.partition,
                        offset: message.offset,
                        timestamp: message.timestamp,
                        timestamp_type: message.timestamp_type,
                        key: message.key_text ?? null,
                        value: message.value_text ?? null,
                        headers: message.headers,
                      };
                      navigator.clipboard.writeText(JSON.stringify(obj, null, 2)).then(() => {
                        void antMessage.success("Copied full message");
                      });
                    }}
                  />
                </Tooltip>
                <Tooltip title="Copy value">
                  <Button
                    size="small"
                    type="text"
                    icon={<CopyOutlined />}
                    onClick={() => {
                      const text = tryFormatJson(message.value_text) ?? message.value_text ?? "";
                      navigator.clipboard.writeText(text).then(() => {
                        void antMessage.success("Copied");
                      });
                    }}
                  />
                </Tooltip>
              </Space>
            </div>
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
