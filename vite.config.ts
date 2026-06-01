import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { execSync } from "child_process";

function getAppVersion(): string {
  try {
    return execSync("git describe --tags --abbrev=0 2>/dev/null").toString().trim();
  } catch {
    try {
      const hash = execSync("git rev-parse --short HEAD").toString().trim();
      return `0.0.0-dev+${hash}`;
    } catch {
      return "0.0.0-dev";
    }
  }
}

export default defineConfig({
  plugins: [react()],
  define: {
    __APP_VERSION__: JSON.stringify(getAppVersion()),
  },
  server: {
    port: 1420,
    strictPort: true,
  },
  clearScreen: false,
  envPrefix: ["VITE_", "TAURI_"],
  build: {
    outDir: "dist",
    target:
      process.env.TAURI_ENV_PLATFORM === "windows" ? "chrome105" : "safari14",
    sourcemap: !!process.env.TAURI_ENV_DEBUG,
    minify: !process.env.TAURI_ENV_DEBUG ? "esbuild" : false,
  },
});
