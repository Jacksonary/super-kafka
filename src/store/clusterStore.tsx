import React, { createContext, useContext, useEffect, useMemo, useState, useCallback } from "react";
import { api } from "../api";
import type { ClusterConfig, ClusterSummary } from "../types";

interface ClusterStoreValue {
  clusters: ClusterConfig[];
  loadingClusters: boolean;
  refreshClusters: () => Promise<void>;
  currentClusterId: string | null;
  setCurrentClusterId: (id: string | null) => void;
  currentCluster: ClusterConfig | null;
  currentSummary: ClusterSummary | null;
  refreshCurrentSummary: () => Promise<void>;
}

const ClusterStoreContext = createContext<ClusterStoreValue | null>(null);

const LS_KEY = "super-kafka:current-cluster-id";

export function ClusterStoreProvider({ children }: { children: React.ReactNode }) {
  const [clusters, setClusters] = useState<ClusterConfig[]>([]);
  const [loadingClusters, setLoadingClusters] = useState<boolean>(false);
  const [currentClusterId, setCurrentClusterIdState] = useState<string | null>(() => {
    try {
      return localStorage.getItem(LS_KEY);
    } catch {
      return null;
    }
  });
  const [currentSummary, setCurrentSummary] = useState<ClusterSummary | null>(null);

  const setCurrentClusterId = useCallback((id: string | null) => {
    setCurrentClusterIdState(id);
    try {
      if (id) localStorage.setItem(LS_KEY, id);
      else localStorage.removeItem(LS_KEY);
    } catch {
      /* ignore */
    }
  }, []);

  const refreshClusters = useCallback(async () => {
    setLoadingClusters(true);
    try {
      const list = await api.listClusters();
      setClusters(list);
      // Auto-select first cluster if none selected or stored id is gone
      setCurrentClusterIdState((cur) => {
        if (cur && list.some((c) => c.id === cur)) return cur;
        const next = list[0]?.id ?? null;
        try {
          if (next) localStorage.setItem(LS_KEY, next);
          else localStorage.removeItem(LS_KEY);
        } catch { /* ignore */ }
        return next;
      });
    } finally {
      setLoadingClusters(false);
    }
  }, []);

  const refreshCurrentSummary = useCallback(async () => {
    if (!currentClusterId) {
      setCurrentSummary(null);
      return;
    }
    try {
      const s = await api.getClusterSummary(currentClusterId);
      setCurrentSummary(s);
    } catch {
      setCurrentSummary(null);
    }
  }, [currentClusterId]);

  useEffect(() => {
    void refreshClusters();
  }, [refreshClusters]);

  useEffect(() => {
    void refreshCurrentSummary();
  }, [refreshCurrentSummary]);

  const currentCluster = useMemo(
    () => clusters.find((c) => c.id === currentClusterId) ?? null,
    [clusters, currentClusterId],
  );

  const value: ClusterStoreValue = {
    clusters,
    loadingClusters,
    refreshClusters,
    currentClusterId,
    setCurrentClusterId,
    currentCluster,
    currentSummary,
    refreshCurrentSummary,
  };

  return (
    <ClusterStoreContext.Provider value={value}>
      {children}
    </ClusterStoreContext.Provider>
  );
}

export function useClusterStore(): ClusterStoreValue {
  const ctx = useContext(ClusterStoreContext);
  if (!ctx) throw new Error("useClusterStore must be used inside ClusterStoreProvider");
  return ctx;
}
