use crate::types::{ClusterConfig, ConnectorDetail, ConnectorSummary, ConnectorTask};
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone)]
pub struct ConnectClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct StatusResp {
    name: String,
    connector: StatusConnector,
    tasks: Vec<StatusTask>,
    #[serde(rename = "type")]
    #[serde(default)]
    connector_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusConnector {
    state: String,
    #[serde(default)]
    trace: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusTask {
    id: i32,
    state: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    trace: Option<String>,
}

impl ConnectClient {
    pub fn from_cluster(cluster: &ClusterConfig) -> Result<Option<Self>, String> {
        let url = match cluster.connect_url.as_ref() {
            Some(u) if !u.trim().is_empty() => u.trim().trim_end_matches('/').to_string(),
            _ => return Ok(None),
        };
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        // Connect REST API has no built-in auth in our config; if needed reuse schema registry creds.
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_millis(cluster.request_timeout_ms as u64))
            .build()
            .map_err(|e| format!("[CONNECT] http client: {e}"))?;
        Ok(Some(Self { base_url: url, http }))
    }

    #[allow(dead_code)]
    pub fn with_basic_auth(mut self, user: &str, password: &str) -> Result<Self, String> {
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        let val = HeaderValue::from_str(&format!("Basic {token}"))
            .map_err(|e| format!("[CONNECT] invalid basic auth: {e}"))?;
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, val);
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        self.http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| format!("[CONNECT] rebuild http: {e}"))?;
        Ok(self)
    }

    pub async fn list_connectors(&self) -> Result<Vec<ConnectorSummary>, String> {
        let url = format!("{}/connectors", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("[CONNECT] GET connectors: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[CONNECT] HTTP {s}: {body}"));
        }
        let names: Vec<String> = resp
            .json()
            .await
            .map_err(|e| format!("[CONNECT] decode connectors: {e}"))?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            match self.fetch_status_and_config(&name).await {
                Ok((status, cfg)) => {
                    let connector_class = cfg
                        .get("connector.class")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());
                    let failed = status.tasks.iter().filter(|t| t.state == "FAILED").count() as i32;
                    out.push(ConnectorSummary {
                        name: status.name,
                        connector_type: status
                            .connector_type
                            .unwrap_or_else(|| "unknown".to_string()),
                        state: status.connector.state,
                        task_count: status.tasks.len() as i32,
                        failed_tasks: failed,
                        connector_class,
                    });
                }
                Err(_) => out.push(ConnectorSummary {
                    name,
                    connector_type: "unknown".to_string(),
                    state: "UNKNOWN".to_string(),
                    task_count: 0,
                    failed_tasks: 0,
                    connector_class: "unknown".to_string(),
                }),
            }
        }
        Ok(out)
    }

    async fn fetch_status_and_config(
        &self,
        name: &str,
    ) -> Result<(StatusResp, HashMap<String, String>), String> {
        let status_url = format!("{}/connectors/{}/status", self.base_url, urlencode(name));
        let cfg_url = format!("{}/connectors/{}/config", self.base_url, urlencode(name));
        let status: StatusResp = self
            .http
            .get(&status_url)
            .send()
            .await
            .map_err(|e| format!("[CONNECT] GET status: {e}"))?
            .error_for_status()
            .map_err(|e| format!("[CONNECT] status http: {e}"))?
            .json()
            .await
            .map_err(|e| format!("[CONNECT] decode status: {e}"))?;
        let cfg: HashMap<String, String> = self
            .http
            .get(&cfg_url)
            .send()
            .await
            .map_err(|e| format!("[CONNECT] GET config: {e}"))?
            .error_for_status()
            .map_err(|e| format!("[CONNECT] config http: {e}"))?
            .json()
            .await
            .map_err(|e| format!("[CONNECT] decode config: {e}"))?;
        Ok((status, cfg))
    }

    pub async fn get_connector(&self, name: &str) -> Result<ConnectorDetail, String> {
        let (status, config) = self.fetch_status_and_config(name).await?;
        let trace = status.connector.trace.clone();
        let tasks: Vec<ConnectorTask> = status
            .tasks
            .iter()
            .map(|t| ConnectorTask {
                task_id: t.id,
                state: t.state.clone(),
                worker_id: t.worker_id.clone().unwrap_or_default(),
                error_trace: t.trace.clone(),
            })
            .collect();
        Ok(ConnectorDetail {
            name: status.name,
            connector_type: status
                .connector_type
                .unwrap_or_else(|| "unknown".to_string()),
            state: status.connector.state,
            config,
            tasks,
            error_trace: trace,
        })
    }

    pub async fn pause(&self, name: &str) -> Result<(), String> {
        self.simple_action("PUT", &format!("connectors/{}/pause", urlencode(name)))
            .await
    }

    pub async fn resume(&self, name: &str) -> Result<(), String> {
        self.simple_action("PUT", &format!("connectors/{}/resume", urlencode(name)))
            .await
    }

    pub async fn restart(&self, name: &str) -> Result<(), String> {
        self.simple_action("POST", &format!("connectors/{}/restart", urlencode(name)))
            .await
    }

    pub async fn delete(&self, name: &str) -> Result<(), String> {
        self.simple_action("DELETE", &format!("connectors/{}", urlencode(name)))
            .await
    }

    async fn simple_action(&self, method: &str, path: &str) -> Result<(), String> {
        let url = format!("{}/{}", self.base_url, path);
        let req = match method {
            "PUT" => self.http.put(&url),
            "POST" => self.http.post(&url),
            "DELETE" => self.http.delete(&url),
            _ => return Err(format!("[CONNECT] unsupported method {method}")),
        };
        let resp = req
            .send()
            .await
            .map_err(|e| format!("[CONNECT] {method} {path}: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[CONNECT] HTTP {s}: {body}"));
        }
        Ok(())
    }

    pub async fn upsert(
        &self,
        name: &str,
        config: HashMap<String, String>,
    ) -> Result<Value, String> {
        let url = format!("{}/connectors/{}/config", self.base_url, urlencode(name));
        let resp = self
            .http
            .put(&url)
            .json(&config)
            .send()
            .await
            .map_err(|e| format!("[CONNECT] PUT config: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[CONNECT] HTTP {s}: {body}"));
        }
        resp.json::<Value>()
            .await
            .map_err(|e| format!("[CONNECT] decode upsert: {e}"))
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
            out.push(c);
        } else {
            for b in c.to_string().bytes() {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}
