import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import logoUrl from "../../assets/logo.png";
import { Alert, Button, Layout, Menu, Modal, Progress, Select, Tooltip, Typography, message as antMessage } from "antd";
import {
  UnorderedListOutlined,
  TeamOutlined,
  ClusterOutlined,
  SendOutlined,
  GithubOutlined,
  ReloadOutlined,
  LeftOutlined,
  RightOutlined,
  SettingOutlined,
  PlusOutlined,
} from "@ant-design/icons";
import { openUrl } from "@tauri-apps/plugin-opener";
import { relaunch } from "@tauri-apps/plugin-process";
import { check, type Update } from "@tauri-apps/plugin-updater";
import { useUpdateCheck } from "../../useUpdateCheck";
import { useSettings } from "../../store/settingsStore";
import { theme } from "antd";
import { Routes, Route, useLocation, useNavigate, Navigate } from "react-router-dom";
import { useClusterStore } from "../../store/clusterStore";
import Topics from "../../pages/Topics";
import TopicDetail from "../../pages/TopicDetail";
import MessageBrowser from "../../pages/MessageBrowser";
import MessageProducer from "../../pages/MessageProducer";
import ConsumerGroups from "../../pages/ConsumerGroups";
import Cluster from "../../pages/Cluster";
import ClusterDetail from "../../pages/ClusterDetail";
import Settings from "../../pages/Settings";

const { Sider, Content, Header } = Layout;
const { Text } = Typography;

const NAV_ITEMS = [
  { key: "/cluster", label: "Cluster", icon: <ClusterOutlined /> },
  { key: "/topics", label: "Topics", icon: <UnorderedListOutlined /> },
  { key: "/groups", label: "Consumer Groups", icon: <TeamOutlined /> },
  { key: "/producer", label: "Producer", icon: <SendOutlined /> },
  { key: "/settings", label: "Settings", icon: <SettingOutlined /> },
];

const SIDEBAR_WIDTH = 240;
const SIDEBAR_COLLAPSED_WIDTH = 64;
const SIDEBAR_COLLAPSED_KEY = "super-kafka:sidebar-collapsed";
// Set when the user clicks "Later" on a downloaded update; on next launch the app
// silently re-downloads and installs before showing the UI.
const PENDING_UPDATE_KEY = "super-kafka:install-update-on-launch";

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
  const { clusters, currentClusterId, setCurrentClusterId, currentSummary, connecting, refreshCurrentSummary, requestAddCluster } = useClusterStore();
  const { token } = theme.useToken();
  const { config: appConfig } = useSettings();
  const isDark = appConfig.theme !== "light";
  const hoverBg = isDark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.04)";
  const { state: updateState, setState: setUpdateState, checking, recheck } = useUpdateCheck(__APP_VERSION__, appConfig.check_updates_on_startup);

  const readyVersionRef = useRef<string>("");
  const pendingUpdateRef = useRef<Update | null>(null);
  const downloadingRef = useRef(false);
  const modalOpenRef = useRef(false);

  const [collapsed, setCollapsed] = useState<boolean>(() => {
    try {
      return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === "true";
    } catch {
      return false;
    }
  });

  const toggleCollapsed = useCallback(() => {
    setCollapsed((c) => {
      const next = !c;
      try { localStorage.setItem(SIDEBAR_COLLAPSED_KEY, String(next)); } catch { /* ignore */ }
      return next;
    });
  }, []);

  function showRestartModal(version: string) {
    if (modalOpenRef.current) return;
    modalOpenRef.current = true;
    Modal.confirm({
      title: "Update ready",
      content: version
        ? `Version ${version} has been downloaded. Restart now to apply the update, or continue working and restart later.`
        : `An update has been downloaded. Restart now to apply it, or continue working and restart later.`,
      okText: "Restart now",
      cancelText: "Later",
      onOk: async () => {
        modalOpenRef.current = false;
        if (pendingUpdateRef.current) {
          try {
            await pendingUpdateRef.current.install();
          } catch (e) {
            void antMessage.error(`Install failed: ${String(e)}`);
            return;
          }
        }
        try { localStorage.removeItem(PENDING_UPDATE_KEY); } catch { /* ignore */ }
        void relaunch();
      },
      onCancel: () => {
        modalOpenRef.current = false;
        // Defer the install to next launch instead of discarding the download.
        try { localStorage.setItem(PENDING_UPDATE_KEY, "1"); } catch { /* ignore */ }
      },
    });
  }

  const handleUpdate = async () => {
    if (updateState.status !== "available" || downloadingRef.current) return;
    downloadingRef.current = true;
    const upd = updateState.update;
    const version = updateState.version;
    pendingUpdateRef.current = upd;
    let total = 0;
    let downloaded = 0;
    setUpdateState({ status: "downloading", progress: 0 });
    try {
      await upd.download((evt) => {
        if (evt.event === "Started" && evt.data.contentLength) {
          total = evt.data.contentLength;
        } else if (evt.event === "Progress") {
          downloaded += evt.data.chunkLength;
          if (total > 0) setUpdateState({ status: "downloading", progress: Math.round((downloaded / total) * 100) });
        }
      });
      readyVersionRef.current = version;
      setUpdateState({ status: "ready" });
      showRestartModal(version);
    } catch (e) {
      setUpdateState({ status: "error", message: String(e) });
    } finally {
      downloadingRef.current = false;
    }
  };

  // On launch, if the user previously chose "Later", finish the deferred install
  // now (silently) before they start working. The in-memory Update handle does not
  // survive a restart, so we re-check to obtain a fresh one, then download+install.
  const launchInstallRan = useRef(false);
  useEffect(() => {
    if (launchInstallRan.current) return;
    launchInstallRan.current = true;
    let pending = false;
    try { pending = localStorage.getItem(PENDING_UPDATE_KEY) === "1"; } catch { /* ignore */ }
    if (!pending) return;

    void (async () => {
      try {
        const upd = await check();
        if (!upd) {
          // Already up to date (e.g. installed elsewhere) — clear and move on.
          try { localStorage.removeItem(PENDING_UPDATE_KEY); } catch { /* ignore */ }
          return;
        }
        setUpdateState({ status: "downloading", progress: 0 });
        let total = 0;
        let downloaded = 0;
        await upd.download((evt) => {
          if (evt.event === "Started" && evt.data.contentLength) {
            total = evt.data.contentLength;
          } else if (evt.event === "Progress") {
            downloaded += evt.data.chunkLength;
            if (total > 0) setUpdateState({ status: "downloading", progress: Math.round((downloaded / total) * 100) });
          }
        });
        await upd.install();
        try { localStorage.removeItem(PENDING_UPDATE_KEY); } catch { /* ignore */ }
        void relaunch();
      } catch (e) {
        // Don't trap the user in a failing upgrade loop — clear the flag and
        // fall back to the normal in-app update prompt.
        try { localStorage.removeItem(PENDING_UPDATE_KEY); } catch { /* ignore */ }
        setUpdateState({ status: "error", message: String(e) });
      }
    })();
  }, [setUpdateState]);

  const selectedKey = useMemo(() => {
    const match = NAV_ITEMS.find((item) => location.pathname.startsWith(item.key));
    return match?.key ?? "/cluster";
  }, [location.pathname]);

  // ── Status color ──
  const statusColor = useMemo(() => {
    if (connecting) return "#faad14";
    if (currentSummary?.status === "error") return "#ff4d4f";
    if (currentSummary?.status === "connected") return "#52c41a";
    return "#8c8c8c";
  }, [currentSummary?.status, connecting]);

  return (
    <Layout style={{ height: "100vh", overflow: "hidden" }}>
      <Sider
        width={SIDEBAR_WIDTH}
        collapsedWidth={SIDEBAR_COLLAPSED_WIDTH}
        collapsed={collapsed}
        trigger={null}
        style={{
          height: "100vh",
          overflow: "hidden",
          borderRight: `1px solid ${token.colorBorder}`,
          flexShrink: 0,
        }}
      >
        {/* ── Header ── */}
        <div
          style={{
            height: 56,
            display: "flex",
            alignItems: "center",
            justifyContent: collapsed ? "center" : "space-between",
            padding: collapsed ? 0 : "0 12px 0 16px",
            borderBottom: `1px solid ${token.colorBorder}`,
            flexShrink: 0,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10, overflow: "hidden" }}>
            <img
              src={logoUrl}
              alt="logo"
              style={{ width: 32, height: 32, flexShrink: 0, display: "block" }}
            />
            {!collapsed && (
              <Text strong style={{ color: token.colorPrimary, fontSize: 16, whiteSpace: "nowrap" }}>
                Super Kafka
              </Text>
            )}
          </div>

          {!collapsed && (
            <Tooltip title="Collapse sidebar" placement="right">
              <div
                onClick={toggleCollapsed}
                style={{
                  width: 24,
                  height: 24,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  borderRadius: 6,
                  cursor: "pointer",
                  color: token.colorTextQuaternary,
                  flexShrink: 0,
                  transition: "color 0.15s, background 0.15s",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.color = token.colorTextSecondary;
                  e.currentTarget.style.background = hoverBg;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = token.colorTextQuaternary;
                  e.currentTarget.style.background = "transparent";
                }}
              >
                <LeftOutlined style={{ fontSize: 12 }} />
              </div>
            </Tooltip>
          )}
        </div>

        {/* ── Cluster selector ── */}
        {!collapsed && (
          <div style={{ padding: 12, borderBottom: `1px solid ${token.colorBorder}`, flexShrink: 0 }}>
            <Select
              value={currentClusterId ?? undefined}
              onChange={(v) => { setCurrentClusterId(v); navigate(selectedKey); }}
              placeholder="Select a cluster"
              style={{ width: "100%" }}
              options={clusters.map((c) => ({ value: c.id, label: c.name }))}
              notFoundContent={
                <span style={{ color: token.colorTextTertiary }}>No clusters configured.</span>
              }
              dropdownRender={(menu) => (
                <>
                  {menu}
                  <div
                    style={{
                      borderTop: `1px solid ${token.colorBorderSecondary}`,
                      padding: "6px 8px",
                    }}
                  >
                    <Button
                      type="link"
                      size="small"
                      block
                      icon={<PlusOutlined />}
                      style={{ textAlign: "left", paddingLeft: 4 }}
                      onClick={() => {
                        // 先跳到 Cluster 页（保证 Cluster 组件已 mount），再请求弹窗。
                        // requestAddCluster 通过 store 计数器变化触发 Cluster 页 useEffect 打开 Modal。
                        navigate("/cluster");
                        requestAddCluster();
                      }}
                    >
                      Add Cluster
                    </Button>
                  </div>
                </>
              )}
              labelRender={({ label }) => (
                <Text
                  style={{
                    fontSize: 13,
                    fontWeight: 500,
                    color: statusColor,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    display: "block",
                  }}
                >
                  {label}
                </Text>
              )}
            />
            {/* Error and connecting states shown below the Select, not inside it */}
            {!connecting && currentSummary?.status === "error" && currentSummary.error_message && (
              <Tooltip title={currentSummary.error_message}>
                <Text
                  style={{
                    display: "block",
                    marginTop: 4,
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
              <Text style={{ display: "block", marginTop: 4, fontSize: 11, color: "#faad14" }}>
                Connecting...
              </Text>
            )}
          </div>
        )}

        {/* ── Expand button when collapsed ── */}
        {collapsed && (
          <Tooltip title="Expand sidebar" placement="right">
            <div
              onClick={toggleCollapsed}
              style={{
                position: "relative",
                height: 40,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: "pointer",
                color: token.colorTextQuaternary,
                borderBottom: `1px solid ${token.colorBorder}`,
                transition: "color 0.15s, background 0.15s",
                flexShrink: 0,
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.color = token.colorTextSecondary;
                e.currentTarget.style.background = "rgba(255,255,255,0.06)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.color = token.colorTextQuaternary;
                e.currentTarget.style.background = "transparent";
              }}
            >
              <RightOutlined style={{ fontSize: 12 }} />
              <div
                style={{
                  position: "absolute",
                  top: 8,
                  right: 8,
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  background: statusColor,
                }}
              />
            </div>
          </Tooltip>
        )}

        {/* ── Navigation ── */}
        <Menu
          mode="inline"
          theme={isDark ? "dark" : "light"}
          selectedKeys={[selectedKey]}
          inlineCollapsed={collapsed}
          onClick={(e) => navigate(e.key)}
          style={{ borderRight: 0, flex: 1, overflow: "auto" }}
          items={NAV_ITEMS}
        />

        {/* ── Footer: version + links ── */}
        <div
          style={{
            padding: collapsed ? "8px 0" : "8px 12px",
            borderTop: `1px solid ${token.colorBorder}`,
            display: "flex",
            alignItems: "center",
            justifyContent: collapsed ? "center" : "space-between",
            gap: 4,
            flexShrink: 0,
            minWidth: 0,
          }}
        >
          {!collapsed && (
            <div style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>
              {updateState.status === "available" ? (
                <Tooltip title={`${updateState.version} available — click to update`}>
                  <a href="#" onClick={(e) => { e.preventDefault(); handleUpdate(); }} style={{ cursor: "pointer", textDecoration: "none" }}>
                    <Text style={{ fontSize: 11, color: token.colorWarningText }} ellipsis>
                      v{__APP_VERSION__} → {updateState.version}
                    </Text>
                  </a>
                </Tooltip>
              ) : updateState.status === "downloading" ? (
                <div>
                  <Text style={{ fontSize: 11, color: token.colorWarningText }}>
                    Downloading... {updateState.progress}%
                  </Text>
                  <Progress percent={updateState.progress} size="small" showInfo={false} strokeColor={token.colorWarning} />
                </div>
              ) : updateState.status === "ready" ? (
                <a href="#" onClick={(e) => { e.preventDefault(); showRestartModal(readyVersionRef.current); }} style={{ cursor: "pointer", textDecoration: "none" }}>
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
              ) : (
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
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
                </div>
              )}
            </div>
          )}

          <div style={{ display: "flex", alignItems: "center", gap: 6, flexShrink: 0 }}>
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
          </div>
        </div>
      </Sider>

      <Layout style={{ overflow: "hidden" }}>
        <Header
          style={{
            background: token.colorBgContainer,
            borderBottom: `1px solid ${token.colorBorder}`,
            padding: "0 24px",
            display: "flex",
            alignItems: "center",
            height: 48,
            flexShrink: 0,
          }}
        >
          <Text strong style={{ fontSize: 14 }}>
            {NAV_ITEMS.find((i) => i.key === selectedKey)?.label}
          </Text>
        </Header>
        {currentSummary?.status === "error" && !connecting && (
          <Alert
            banner
            type="error"
            showIcon
            message={currentSummary.error_message ?? "Cannot connect to cluster"}
            action={
              <Button size="small" type="link" onClick={() => void refreshCurrentSummary()}>
                Reconnect
              </Button>
            }
          />
        )}
        <Content style={{ padding: 24, background: token.colorBgLayout, overflow: "hidden", display: "flex", flexDirection: "column", minHeight: 0, height: "100%" }}>
          <Routes>
            <Route index element={<Navigate to="/cluster" replace />} />
            <Route path="/cluster" element={<Cluster />} />
            <Route path="/cluster/:clusterId" element={<ClusterDetail />} />
            <Route path="/topics" element={<Topics />} />
            <Route path="/topics/:topicName" element={<TopicDetail />} />
            <Route path="/topics/:topicName/messages" element={<MessageBrowser />} />
            <Route path="/groups" element={<ConsumerGroups />} />
            <Route path="/producer" element={<MessageProducer />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="*" element={<Navigate to="/cluster" replace />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  );
}
