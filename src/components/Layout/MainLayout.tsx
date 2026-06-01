import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import logoUrl from "../../assets/logo.png";
import { Layout, Menu, Progress, Select, Space, Tooltip, Typography, message as antMessage } from "antd";
import {
  UnorderedListOutlined,
  TeamOutlined,
  DatabaseOutlined,
  ApiOutlined,
  SettingOutlined,
  SendOutlined,
  GithubOutlined,
  ReloadOutlined,
} from "@ant-design/icons";
import { openUrl } from "@tauri-apps/plugin-opener";
import { relaunch } from "@tauri-apps/plugin-process";
import { useUpdateCheck } from "../../useUpdateCheck";
import { theme } from "antd";
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

const SIDEBAR_WIDTH_KEY = "super-kafka:sidebar-width";
const MIN_SIDEBAR_WIDTH = 64;
const MAX_SIDEBAR_WIDTH = 320;
const DEFAULT_SIDEBAR_WIDTH = 240;

const GITHUB_URL = "https://github.com/Jacksonary/super-kafka";
const GITEE_URL = "https://gitee.com/weiguoliu/super-kafka";

function GiteeIcon() {
  return (
    <svg viewBox="0 0 24 24" width="1em" height="1em" fill="currentColor" aria-hidden="true">
      <path d="M11.984 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.016 0zm6.09 5.333c.328 0 .593.26.593.593v1.482a.594.594 0 0 1-.593.592H9.777c-.982 0-1.778.796-1.778 1.778v5.63c0 .327.26.593.593.593h5.63c.982 0 1.778-.796 1.778-1.778v-.296a.593.593 0 0 0-.592-.593h-4.15a.592.592 0 0 1-.592-.592v-1.482a.593.593 0 0 1 .593-.592h6.815c.327 0 .593.265.593.592v3.408a4 4 0 0 1-4 4H5.926a.593.593 0 0 1-.593-.593V9.778a4.444 4.444 0 0 1 4.445-4.444h8.296Z" />
    </svg>
  );
}

export default function MainLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const { clusters, currentClusterId, setCurrentClusterId, currentSummary, connecting } = useClusterStore();
  const { token } = theme.useToken();
  const { state: updateState, setState: setUpdateState, fallback, checking, recheck } = useUpdateCheck(__APP_VERSION__);

  const [sidebarWidth, setSidebarWidth] = useState(() => {
    try {
      const stored = localStorage.getItem(SIDEBAR_WIDTH_KEY);
      if (stored) {
        const w = Number(stored);
        if (w >= MIN_SIDEBAR_WIDTH && w <= MAX_SIDEBAR_WIDTH) return w;
      }
    } catch { /* ignore */ }
    return DEFAULT_SIDEBAR_WIDTH;
  });
  const [isResizing, setIsResizing] = useState(false);

  const handleUpdate = async () => {
    if (updateState.status !== "available") return;
    const upd = updateState.update;
    let total = 0;
    let downloaded = 0;
    setUpdateState({ status: "downloading", progress: 0 });
    try {
      await upd.downloadAndInstall((evt) => {
        if (evt.event === "Started" && evt.data.contentLength) {
          total = evt.data.contentLength;
        } else if (evt.event === "Progress") {
          downloaded += evt.data.chunkLength;
          if (total > 0) setUpdateState({ status: "downloading", progress: Math.round((downloaded / total) * 100) });
        }
      });
      setUpdateState({ status: "ready" });
    } catch (e) {
      setUpdateState({ status: "error", message: String(e) });
    }
  };

  const selectedKey = useMemo(() => {
    const match = NAV_ITEMS.find((item) => location.pathname.startsWith(item.key));
    return match?.key ?? "/topics";
  }, [location.pathname]);

  // ── Resize drag ──
  const resizingRef = useRef(false);
  const startXRef = useRef(0);
  const startWidthRef = useRef(0);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    resizingRef.current = true;
    startXRef.current = e.clientX;
    startWidthRef.current = sidebarWidth;
    document.body.style.userSelect = "none";
    setIsResizing(true);
  }, [sidebarWidth]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!resizingRef.current) return;
      const delta = e.clientX - startXRef.current;
      const next = Math.min(MAX_SIDEBAR_WIDTH, Math.max(MIN_SIDEBAR_WIDTH, startWidthRef.current + delta));
      setSidebarWidth(next);
    };
    const handleMouseUp = () => {
      if (resizingRef.current) {
        resizingRef.current = false;
        document.body.style.userSelect = "";
        setIsResizing(false);
        setSidebarWidth((w) => {
          try { localStorage.setItem(SIDEBAR_WIDTH_KEY, String(w)); } catch { /* ignore */ }
          return w;
        });
      }
    };
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, []);

  // ── Status color ──
  const statusColor = useMemo(() => {
    if (!currentSummary?.status) return "#8c8c8c";
    if (currentSummary.status === "error") return "#ff4d4f";
    if (connecting) return "#faad14";
    if (currentSummary.status === "connected") return "#52c41a";
    return "#8c8c8c";
  }, [currentSummary?.status, connecting]);

  const clusterDisplayName = useMemo(() => {
    if (!currentClusterId) return "No cluster";
    return currentSummary?.name ?? currentClusterId;
  }, [currentClusterId, currentSummary?.name]);

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider
        width={sidebarWidth}
        style={{
          borderRight: "1px solid #1f242c",
          display: "flex",
          flexDirection: "column",
          position: "relative",
        }}
      >
        {/* Resize handle */}
        <div
          onMouseDown={handleResizeStart}
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            width: 4,
            height: "100%",
            cursor: "col-resize",
            zIndex: 10,
            background: isResizing ? "rgba(0,212,255,0.3)" : "transparent",
            transition: "background 0.15s",
          }}
          onMouseEnter={(e) => { e.currentTarget.style.background = "rgba(255,255,255,0.08)"; }}
          onMouseLeave={(e) => { if (!isResizing) e.currentTarget.style.background = "transparent"; }}
          aria-label="Resize sidebar"
        />

        {/* ── Header ── */}
        <div
          style={{
            height: 56,
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-start",
            padding: "0 16px",
            borderBottom: "1px solid #1f242c",
          }}
        >
          <div
            style={{
              width: 32,
              height: 32,
              borderRadius: 8,
              overflow: "hidden",
              flexShrink: 0,
              marginRight: 10,
            }}
          >
            <img src={logoUrl} alt="logo" style={{ width: 32, height: 32, display: "block" }} />
          </div>
          <Text strong style={{ color: "#00d4ff", fontSize: 16 }}>
            Super Kafka
          </Text>
        </div>

        {/* ── Cluster selector with status ── */}
        <div style={{ padding: 12, borderBottom: "1px solid #1f242c" }}>
          <Select
            value={currentClusterId ?? undefined}
            onChange={(v) => setCurrentClusterId(v)}
            placeholder="Select a cluster"
            style={{ width: "100%" }}
            options={clusters.map((c) => ({
              value: c.id,
              label: c.name,
            }))}
            notFoundContent={
              <span style={{ color: "#8c8c8c" }}>
                No clusters configured. Add one in Settings.
              </span>
            }
            labelRender={({ label }) => (
              <div style={{ display: "flex", flexDirection: "column" }}>
                <Text
                  style={{
                    fontSize: 13,
                    fontWeight: 500,
                    color: statusColor,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {label ?? clusterDisplayName}
                </Text>
                {currentSummary?.status === "connected" && (
                  <Text style={{ fontSize: 11, color: token.colorTextQuaternary }}>
                    v{currentSummary.kafka_version ?? "?"} · {currentSummary.broker_count ?? 0} broker{(currentSummary.broker_count ?? 0) !== 1 ? "s" : ""}
                  </Text>
                )}
                {currentSummary?.status === "error" && currentSummary.error_message && (
                  <Tooltip title={currentSummary.error_message}>
                    <Text
                      style={{
                        fontSize: 11,
                        color: "#ff4d4f",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {currentSummary.error_message}
                    </Text>
                  </Tooltip>
                )}
                {connecting && (
                  <Text style={{ fontSize: 11, color: "#faad14" }}>
                    Connecting...
                  </Text>
                )}
              </div>
            )}
          />
        </div>

        {/* ── Navigation ── */}
        <Menu
          mode="inline"
          theme="dark"
          selectedKeys={[selectedKey]}
          onClick={(e) => navigate(e.key)}
          style={{ borderRight: 0, flex: 1 }}
          items={NAV_ITEMS}
        />

        {/* ── Footer: version + links ── */}
        <div
          style={{
            padding: "8px 12px",
            borderTop: "1px solid #1f242c",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 4,
          }}
        >
          {updateState.status === "available" ? (
            <Tooltip title={`v${updateState.version} available — click to update`}>
              <a href="#" onClick={(e) => { e.preventDefault(); handleUpdate(); }} style={{ cursor: "pointer", textDecoration: "none" }}>
                <Text style={{ fontSize: 11, color: token.colorWarningText }}>
                  v{__APP_VERSION__} → v{updateState.version}
                </Text>
              </a>
            </Tooltip>
          ) : updateState.status === "downloading" ? (
            <div style={{ flex: 1 }}>
              <Text style={{ fontSize: 11, color: token.colorWarningText }}>
                Downloading... {updateState.progress}%
              </Text>
              <Progress percent={updateState.progress} size="small" showInfo={false} strokeColor={token.colorWarning} />
            </div>
          ) : updateState.status === "ready" ? (
            <a href="#" onClick={(e) => { e.preventDefault(); relaunch(); }} style={{ cursor: "pointer", textDecoration: "none" }}>
              <Text style={{ fontSize: 11, color: token.colorSuccessText }}>
                Update ready — restart
              </Text>
            </a>
          ) : updateState.status === "error" ? (
            <Tooltip title={updateState.message}>
              <a href="#" onClick={(e) => { e.preventDefault(); recheck(); }} style={{ cursor: "pointer", textDecoration: "none" }}>
                <Text style={{ fontSize: 11, color: token.colorErrorText }}>Update failed — retry</Text>
              </a>
            </Tooltip>
          ) : fallback ? (
            <Tooltip title={`v${fallback.latestVersion} available — click to open release`}>
              <a href={fallback.releaseUrl} onClick={(e) => { e.preventDefault(); openUrl(fallback.releaseUrl); }} style={{ cursor: "pointer", textDecoration: "none" }}>
                <Text style={{ fontSize: 11, color: token.colorWarningText }}>
                  v{__APP_VERSION__} → v{fallback.latestVersion}
                </Text>
              </a>
            </Tooltip>
          ) : (
            <Space size={4}>
              <Text style={{ fontSize: 11, color: token.colorTextQuaternary }}>
                v{__APP_VERSION__}
              </Text>
              <Tooltip title="Check for updates">
                <ReloadOutlined
                  spin={checking}
                  style={{ fontSize: 11, color: token.colorTextQuaternary, cursor: "pointer" }}
                  onClick={async () => {
                    if (checking) return;
                    const result = await recheck();
                    if (result === "up-to-date") antMessage.info("Already up to date");
                    else if (result === "error") antMessage.error("Failed to check for updates");
                  }}
                />
              </Tooltip>
            </Space>
          )}

          <Space size={6} align="center">
            <Tooltip title="GitHub">
              <a
                href={GITHUB_URL}
                onClick={(e) => { e.preventDefault(); openUrl(GITHUB_URL); }}
                style={{ color: token.colorTextQuaternary, cursor: "pointer", display: "flex" }}
                aria-label="GitHub repository"
              >
                <GithubOutlined style={{ fontSize: 14 }} />
              </a>
            </Tooltip>
            <Tooltip title="Gitee">
              <a
                href={GITEE_URL}
                onClick={(e) => { e.preventDefault(); openUrl(GITEE_URL); }}
                style={{ color: token.colorTextQuaternary, cursor: "pointer", display: "flex", fontSize: 14 }}
                aria-label="Gitee repository"
              >
                <GiteeIcon />
              </a>
            </Tooltip>
          </Space>
        </div>
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
