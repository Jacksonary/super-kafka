use crate::config;
use crate::kafka_client::{create_bundle, KafkaClientBundle};
use crate::types::ClusterConfig;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ClusterPool {
    bundles: Mutex<HashMap<String, Arc<KafkaClientBundle>>>,
    configs: Mutex<HashMap<String, ClusterConfig>>,
}

impl ClusterPool {
    pub fn new() -> Self {
        Self {
            bundles: Mutex::new(HashMap::new()),
            configs: Mutex::new(HashMap::new()),
        }
    }

    pub fn upsert_config(&self, cluster: ClusterConfig) {
        let mut configs = self.configs.lock();
        configs.insert(cluster.id.clone(), cluster);
    }

    pub fn load_configs<I: IntoIterator<Item = ClusterConfig>>(&self, iter: I) {
        let mut configs = self.configs.lock();
        configs.clear();
        for c in iter {
            configs.insert(c.id.clone(), c);
        }
    }

    pub fn get_config(&self, cluster_id: &str) -> Option<ClusterConfig> {
        self.configs.lock().get(cluster_id).cloned()
    }

    pub fn list_configs(&self) -> Vec<ClusterConfig> {
        self.configs.lock().values().cloned().collect()
    }

    pub fn remove_config(&self, cluster_id: &str) -> Option<ClusterConfig> {
        self.invalidate(cluster_id);
        self.configs.lock().remove(cluster_id)
    }

    pub fn invalidate(&self, cluster_id: &str) {
        self.bundles.lock().remove(cluster_id);
    }

    pub fn get_or_create(&self, cluster_id: &str) -> Result<Arc<KafkaClientBundle>, String> {
        if let Some(b) = self.bundles.lock().get(cluster_id).cloned() {
            return Ok(b);
        }
        let cluster = self
            .get_config(cluster_id)
            .ok_or_else(|| format!("[KAFKA-POOL] cluster `{cluster_id}` not found"))?;
        let password = config::load_sasl_password(cluster_id).ok().flatten();
        let bundle = Arc::new(create_bundle(&cluster, password.as_deref())?);
        self.bundles
            .lock()
            .insert(cluster_id.to_string(), bundle.clone());
        Ok(bundle)
    }
}

impl Default for ClusterPool {
    fn default() -> Self {
        Self::new()
    }
}
