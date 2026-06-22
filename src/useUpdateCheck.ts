import { useEffect, useRef, useState } from "react";
import { check, type Update } from "@tauri-apps/plugin-updater";
import { api } from "./api";

export type UpdateState =
  | { status: "idle" }
  | { status: "available"; update: Update; version: string }
  | { status: "downloading"; progress: number }
  | { status: "ready" }
  | { status: "error"; message: string };

const CACHE_KEY = "super-kafka-update-check";
const CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours
// Set when the user deferred an install via "Later"; the launch-time install flow
// in MainLayout owns the update check in that case, so the auto-check must stand down.
const PENDING_UPDATE_KEY = "super-kafka:install-update-on-launch";

interface FallbackInfo {
  latestVersion: string;
  releaseUrl: string;
}

function parseVersion(v: string): number[] {
  return v.replace(/^v/i, "").split(".").map(Number);
}

function isNewer(remote: string, local: string): boolean {
  const r = parseVersion(remote);
  const l = parseVersion(local);
  for (let i = 0; i < Math.max(r.length, l.length); i++) {
    const rv = r[i] ?? 0;
    const lv = l[i] ?? 0;
    if (rv > lv) return true;
    if (rv < lv) return false;
  }
  return false;
}

export function useUpdateCheck(currentVersion: string, enabled: boolean = true) {
  const [state, setState] = useState<UpdateState>({ status: "idle" });
  const [fallback, setFallback] = useState<FallbackInfo | null>(null);
  const [checking, setChecking] = useState(false);
  const runningRef = useRef(false);

  const doCheck = async (skipCache = false): Promise<"up-to-date" | "error" | null> => {
    if (runningRef.current) return null;

    if (!skipCache) {
      const cached = sessionStorage.getItem(CACHE_KEY);
      if (cached) {
        try {
          const { ts } = JSON.parse(cached);
          if (Date.now() - ts < CACHE_TTL) return null;
        } catch { /* ignore */ }
      }
    }

    runningRef.current = true;
    setChecking(true);
    try {
      const update = await check();
      sessionStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now() }));
      if (update) {
        setState({ status: "available", update, version: update.version });
      } else if (skipCache) {
        return "up-to-date";
      }
    } catch {
      try {
        const info = await api.checkUpdate();
        sessionStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now() }));
        if (isNewer(info.latestVersion, currentVersion)) {
          setFallback(info);
        } else if (skipCache) {
          return "up-to-date";
        }
      } catch {
        if (skipCache) return "error";
      }
    } finally {
      runningRef.current = false;
      setChecking(false);
    }
    return null;
  };

  useEffect(() => {
    if (!enabled) return;
    // Defer to MainLayout's launch-time install flow when an update is pending,
    // so the two paths don't run concurrent check() calls and clobber state.
    let pending = false;
    try { pending = localStorage.getItem(PENDING_UPDATE_KEY) === "1"; } catch { /* ignore */ }
    if (!pending) doCheck();
  }, [currentVersion, enabled]); // eslint-disable-line react-hooks/exhaustive-deps

  const recheck = async (): Promise<"up-to-date" | "error" | null> => {
    sessionStorage.removeItem(CACHE_KEY);
    return doCheck(true);
  };

  return { state, setState, fallback, checking, recheck };
}
