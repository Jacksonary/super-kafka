import { ConfigProvider, App as AntdApp, theme } from "antd";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import MainLayout from "./components/Layout/MainLayout";
import { ClusterStoreProvider } from "./store/clusterStore";

const customTheme = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: "#00d4ff",
    colorBgBase: "#0d1117",
    colorBgContainer: "#11161d",
    colorBgElevated: "#161b22",
    colorBorder: "#1f242c",
    colorBorderSecondary: "#1f242c",
    borderRadius: 6,
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
  },
  components: {
    Layout: {
      siderBg: "#0a0e14",
      headerBg: "#0d1117",
      bodyBg: "#0d1117",
    },
    Menu: {
      darkItemBg: "#0a0e14",
      darkSubMenuItemBg: "#0a0e14",
      darkItemSelectedBg: "rgba(0, 212, 255, 0.12)",
      darkItemHoverBg: "rgba(0, 212, 255, 0.06)",
    },
  },
};

export default function App() {
  return (
    <ConfigProvider theme={customTheme}>
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
