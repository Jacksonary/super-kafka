import React, { createContext, useContext, useEffect, useState, useCallback } from "react";
import { api } from "../api";
import type { AppConfig } from "../types";

const DEFAULT_CONFIG: AppConfig = {
  theme: "dark",
  language: "en",
  fetch_limit_default: 100,
  max_message_display_bytes: 1048576,
  allow_multiple_instances: false,
  check_updates_on_startup: true,
};

interface SettingsStoreValue {
  config: AppConfig;
  loading: boolean;
  save: (patch: Partial<AppConfig>) => Promise<void>;
}

const SettingsStoreContext = createContext<SettingsStoreValue | null>(null);

export function SettingsStoreProvider({ children }: { children: React.ReactNode }) {
  const [config, setConfig] = useState<AppConfig>(DEFAULT_CONFIG);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    void (async () => {
      try {
        const cfg = await api.getAppConfig();
        setConfig(cfg);
      } catch {
        // fall back to default
      }
    })();
  }, []);

  const save = useCallback(async (patch: Partial<AppConfig>) => {
    const next = { ...config, ...patch };
    setLoading(true);
    try {
      await api.saveAppConfig(next);
      setConfig(next);
    } finally {
      setLoading(false);
    }
  }, [config]);

  return (
    <SettingsStoreContext.Provider value={{ config, loading, save }}>
      {children}
    </SettingsStoreContext.Provider>
  );
}

export function useSettings() {
  const ctx = useContext(SettingsStoreContext);
  if (!ctx) throw new Error("useSettings must be used inside SettingsStoreProvider");
  return ctx;
}
