import { useLayoutEffect, useRef } from "react";
import { ConfigProvider, App as AntdApp, theme } from "antd";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import MainLayout from "./components/Layout/MainLayout";
import { ClusterStoreProvider } from "./store/clusterStore";
import { SettingsStoreProvider, useSettings } from "./store/settingsStore";

const FONT_FAMILY =
  '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';

const DARK_TOKENS = {
  colorPrimary: "#00d4ff",
  colorBgBase: "#0d1117",
  colorBgLayout: "#0d1117",
  colorBgContainer: "#11161d",
  colorBgElevated: "#161b22",
  colorBorder: "#1f242c",
  colorBorderSecondary: "#1f242c",
  borderRadius: 6,
  fontFamily: FONT_FAMILY,
};

const LIGHT_TOKENS = {
  colorPrimary: "#0096b8",
  colorBgLayout: "#f5f7fa",
  colorBgContainer: "#ffffff",
  colorBgElevated: "#ffffff",
  colorBorder: "#e5e7eb",
  colorBorderSecondary: "#eef0f3",
  borderRadius: 6,
  fontFamily: FONT_FAMILY,
};

const MENU_DARK_OVERRIDE = {
  darkItemBg: "#0a0e14",
  darkSubMenuItemBg: "#0a0e14",
  darkItemSelectedBg: "rgba(0, 212, 255, 0.12)",
  darkItemHoverBg: "rgba(0, 212, 255, 0.06)",
};

const MENU_LIGHT_OVERRIDE = {
  itemBg: "#ffffff",
  subMenuItemBg: "#ffffff",
  itemSelectedBg: "rgba(0, 150, 184, 0.10)",
  itemHoverBg: "rgba(0, 0, 0, 0.04)",
  itemSelectedColor: "#0096b8",
};

function ThemedApp() {
  const { config } = useSettings();
  const isDark = config.theme !== "light";
  const firstRender = useRef(true);

  // 同步 html data-theme，让 index.css 能按属性切换全局样式。
  // 主题切换瞬间给 <html> 加 .theme-switching class，全站禁用 transition/animation
  // 一帧。这样可避开 cssinjs 注入新 hash class 与旧 token 残留并存的瞬间，
  // 否则 antd Radio.Button 的 ::before 等会先用"新 colorPrimary 套到旧选中态"
  // 渲染一帧，肉眼看到为蓝粗框闪现。
  useLayoutEffect(() => {
    document.documentElement.dataset.theme = isDark ? "dark" : "light";
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    const root = document.documentElement;
    root.classList.add("theme-switching");
    // antd cssinjs 异步注入新 token hash 的 <style>，加上 antd 默认 motionDurationMid=200ms
    // 的过渡，单纯 RAF 不够。用 250ms 兜底覆盖整个切换窗口。
    const timer = window.setTimeout(() => {
      root.classList.remove("theme-switching");
    }, 250);
    return () => {
      window.clearTimeout(timer);
      root.classList.remove("theme-switching");
    };
  }, [isDark]);

  const antTheme = {
    algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
    token: isDark ? DARK_TOKENS : LIGHT_TOKENS,
    components: {
      Layout: {
        siderBg: isDark ? "#0a0e14" : "#ffffff",
        headerBg: isDark ? "#0d1117" : "#ffffff",
        bodyBg: isDark ? "#0d1117" : "#f5f7fa",
      },
      Menu: isDark ? MENU_DARK_OVERRIDE : MENU_LIGHT_OVERRIDE,
    },
  };

  return (
    <ConfigProvider theme={antTheme}>
      <AntdApp>
        <ClusterStoreProvider>
          <BrowserRouter>
            <Routes>
              <Route path="/*" element={<MainLayout />} />
            </Routes>
          </BrowserRouter>
        </ClusterStoreProvider>
      </AntdApp>
    </ConfigProvider>
  );
}

export default function App() {
  return (
    <SettingsStoreProvider>
      <ThemedApp />
    </SettingsStoreProvider>
  );
}
