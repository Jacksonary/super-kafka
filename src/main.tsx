import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "antd/dist/reset.css";
import "./index.css";

// 在 React mount 之前同步给 <html> 设 data-theme，让 index.css 的主题背景规则
// 立即生效，避免启动瞬间的 "白底 → dark → 用户实际主题" 闪屏。
// 真实主题由 settingsStore 在加载后校正（仅在 localStorage 与后端不一致时才切一次）。
try {
  const saved = localStorage.getItem("super-kafka:theme");
  document.documentElement.dataset.theme = saved === "light" ? "light" : "dark";
} catch {
  document.documentElement.dataset.theme = "dark";
}

document.addEventListener("contextmenu", (e) => e.preventDefault());

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
