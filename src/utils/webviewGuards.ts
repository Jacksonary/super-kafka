// 禁用 webview 里来自浏览器的默认行为，让应用更像原生桌面程序。
// 复制/粘贴/剪切/全选/撤销/重做（Ctrl+C/V/X/A/Z/Y）刻意不拦截，避免破坏编辑。

function isEditable(el: EventTarget | null): boolean {
  if (!(el instanceof HTMLElement)) return false;
  const tag = el.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || el.isContentEditable;
}

export function installWebviewGuards(): void {
  const isDev = import.meta.env.DEV;

  // 右键菜单
  document.addEventListener("contextmenu", (e) => e.preventDefault());

  document.addEventListener("keydown", (e) => {
    const mod = e.ctrlKey || e.metaKey;
    const key = e.key.toLowerCase();

    // DevTools —— 仅生产禁用，开发模式保留方便调试
    if (!isDev) {
      if (e.key === "F12") return e.preventDefault();
      if (mod && e.shiftKey && (key === "i" || key === "j" || key === "c")) {
        return e.preventDefault();
      }
    }

    // 查找/打印/查看源码/保存网页/打开文件/缩放（Ctrl/Cmd + 单键）
    if (mod && !e.shiftKey && !e.altKey) {
      if (["f", "g", "p", "u", "s", "o", "=", "-", "+", "0"].includes(key)) {
        return e.preventDefault();
      }
    }
    // 查找上一个（Ctrl/Cmd + Shift + G）
    if (mod && e.shiftKey && key === "g") return e.preventDefault();

    // 历史导航：Alt+←/→
    if (e.altKey && (e.key === "ArrowLeft" || e.key === "ArrowRight")) {
      return e.preventDefault();
    }
    // 历史导航：Backspace 后退（仅当焦点不在可编辑元素时，否则会吞掉删除）
    if (e.key === "Backspace" && !isEditable(e.target)) {
      return e.preventDefault();
    }
  });

  // Ctrl/Cmd + 滚轮缩放（passive:false 才能 preventDefault）
  document.addEventListener("wheel", (e) => {
    if (e.ctrlKey || e.metaKey) e.preventDefault();
  }, { passive: false });

  // 拖放文件到窗口导致 webview 直接打开该文件
  document.addEventListener("dragover", (e) => e.preventDefault());
  document.addEventListener("drop", (e) => e.preventDefault());
}
