import { useCallback } from "react";
import type { ClusterConfig } from "../types";
import { api } from "../api";

export function useClusterOrder() {
  const applySortOrder = useCallback((clusters: ClusterConfig[]): ClusterConfig[] => {
    return [...clusters].sort((a, b) => {
      if (a.sort_order && b.sort_order) return a.sort_order - b.sort_order;
      if (a.sort_order) return -1;
      if (b.sort_order) return 1;
      return a.created_at - b.created_at;
    });
  }, []);

  const saveOrder = useCallback(async (ids: string[]) => {
    await api.reorderClusters(ids);
  }, []);

  return { applySortOrder, saveOrder };
}
