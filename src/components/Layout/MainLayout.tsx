import React, { useMemo, useState } from "react";
import logoUrl from "../../assets/logo.png";
import { Layout, Menu, Select, Space, Tag, Tooltip, Typography } from "antd";
import {
  UnorderedListOutlined,
  TeamOutlined,
  DatabaseOutlined,
  ApiOutlined,
  SettingOutlined,
  SendOutlined,
} from "@ant-design/icons";
import { Routes, Route, useLocation, useNavigate, Navigate } from "react-router-dom";
import { useClusterStore } from "../../store/clusterStore";
import Topics from "../../pages/Topics";
import TopicDetail from "../../pages/TopicDetail";
import MessageBrowser from "../../pages/MessageBrowser";
import MessageProducer from "../../pages/MessageProducer";
import ConsumerGroups from "../../pages/ConsumerGroups";
import SchemaRegistry from "../../pages/SchemaRegistry";
import Connect from "../../pages/Connect";
import Settings from "../../pages/Settings";

const { Sider, Content, Header } = Layout;
const { Text } = Typography;

const NAV_ITEMS = [
  { key: "/topics", label: "Topics", icon: <UnorderedListOutlined /> },
  { key: "/groups", label: "Consumer Groups", icon: <TeamOutlined /> },
  { key: "/schemas", label: "Schema Registry", icon: <DatabaseOutlined /> },
  { key: "/connect", label: "Kafka Connect", icon: <ApiOutlined /> },
  { key: "/producer", label: "Producer", icon: <SendOutlined /> },
  { key: "/settings", label: "Settings", icon: <SettingOutlined /> },
];

function StatusDot({ status }: { status: string | undefined }) {
  let color = "#8c8c8c";
  if (status === "connected") color = "#52c41a";
  else if (status === "error") color = "#ff4d4f";
  else if (status === "connecting") color = "#faad14";
  return (
    <span
      style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        background: color,
        boxShadow: status === "connected" ? `0 0 6px ${color}` : "none",
      }}
    />
  );
}

export default function MainLayout() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { clusters, currentClusterId, setCurrentClusterId, currentSummary } = useClusterStore();

  const selectedKey = useMemo(() => {
    const match = NAV_ITEMS.find((item) => location.pathname.startsWith(item.key));
    return match?.key ?? "/topics";
  }, [location.pathname]);

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider
        width={240}
        collapsedWidth={64}
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        style={{
          borderRight: "1px solid #1f242c",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div
          style={{
            height: 56,
            display: "flex",
            alignItems: "center",
            justifyContent: collapsed ? "center" : "flex-start",
            padding: collapsed ? 0 : "0 16px",
            borderBottom: "1px solid #1f242c",
          }}
        >
          <img
            src={logoUrl}
            alt="logo"
            style={{
              width: 32,
              height: 32,
              borderRadius: 8,
              marginRight: collapsed ? 0 : 10,
              flexShrink: 0,
            }}
          />
          {!collapsed && (
            <Text strong style={{ color: "#00d4ff", fontSize: 16 }}>
              Super Kafka
            </Text>
          )}
        </div>

        {!collapsed && (
          <div style={{ padding: 12, borderBottom: "1px solid #1f242c" }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              CLUSTER
            </Text>
            <Select
              value={currentClusterId ?? undefined}
              onChange={(v) => setCurrentClusterId(v)}
              placeholder="Select a cluster"
              style={{ width: "100%", marginTop: 6 }}
              options={clusters.map((c) => ({
                value: c.id,
                label: c.name,
              }))}
              notFoundContent={
                <span style={{ color: "#8c8c8c" }}>
                  No clusters configured. Add one in Settings.
                </span>
              }
            />
          </div>
        )}

        <Menu
          mode="inline"
          theme="dark"
          selectedKeys={[selectedKey]}
          onClick={(e) => navigate(e.key)}
          style={{ borderRight: 0, flex: 1 }}
          items={NAV_ITEMS}
        />

        {!collapsed && (
          <div
            style={{
              padding: 12,
              borderTop: "1px solid #1f242c",
              fontSize: 12,
            }}
          >
            <Space size={8}>
              <StatusDot status={currentSummary?.status} />
              <Text style={{ color: "#d0d7de" }} ellipsis>
                {currentSummary?.name ?? "No cluster"}
              </Text>
            </Space>
            {currentSummary?.status === "connected" && (
              <div style={{ marginTop: 4 }}>
                <Tag color="cyan" bordered={false} style={{ fontSize: 11 }}>
                  {currentSummary.broker_count} brokers
                </Tag>
                {currentSummary.kafka_version && (
                  <Tag color="default" bordered={false} style={{ fontSize: 11 }}>
                    v{currentSummary.kafka_version}
                  </Tag>
                )}
              </div>
            )}
            {currentSummary?.status === "error" && currentSummary.error_message && (
              <Tooltip title={currentSummary.error_message}>
                <Text type="danger" style={{ fontSize: 11 }} ellipsis>
                  {currentSummary.error_message}
                </Text>
              </Tooltip>
            )}
          </div>
        )}
      </Sider>

      <Layout>
        <Header
          style={{
            background: "#0d1117",
            borderBottom: "1px solid #1f242c",
            padding: "0 24px",
            display: "flex",
            alignItems: "center",
            height: 48,
          }}
        >
          <Text strong style={{ fontSize: 14 }}>
            {NAV_ITEMS.find((i) => i.key === selectedKey)?.label}
          </Text>
        </Header>
        <Content style={{ padding: 24, background: "#0d1117", overflow: "auto" }}>
          <Routes>
            <Route index element={<Navigate to="/topics" replace />} />
            <Route path="/topics" element={<Topics />} />
            <Route path="/topics/:topicName" element={<TopicDetail />} />
            <Route path="/topics/:topicName/messages" element={<MessageBrowser />} />
            <Route path="/messages" element={<MessageBrowser />} />
            <Route path="/groups" element={<ConsumerGroups />} />
            <Route path="/schemas" element={<SchemaRegistry />} />
            <Route path="/connect" element={<Connect />} />
            <Route path="/producer" element={<MessageProducer />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="*" element={<Navigate to="/topics" replace />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  );
}
