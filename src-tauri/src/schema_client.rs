use crate::types::{ClusterConfig, SchemaSubject, SchemaVersion};
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

#[derive(Clone)]
pub struct SchemaClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct VersionResp {
    subject: String,
    version: i32,
    id: i32,
    #[serde(default = "default_schema_type")]
    #[serde(rename = "schemaType")]
    schema_type: String,
    schema: String,
}
fn default_schema_type() -> String {
    "AVRO".to_string()
}

#[derive(Debug, Serialize)]
struct RegisterBody<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType")]
    schema_type: &'a str,
}

impl SchemaClient {
    pub fn from_cluster(
        cluster: &ClusterConfig,
        password: Option<&str>,
    ) -> Result<Option<Self>, String> {
        let url = match cluster.schema_registry_url.as_ref() {
            Some(u) if !u.trim().is_empty() => u.trim().trim_end_matches('/').to_string(),
            _ => return Ok(None),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(SR_CONTENT_TYPE),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(SR_CONTENT_TYPE),
        );
        if let (Some(user), Some(pw)) = (cluster.schema_registry_username.as_deref(), password) {
            let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pw}"));
            let val = HeaderValue::from_str(&format!("Basic {token}"))
                .map_err(|e| format!("[SCHEMA-REGISTRY] invalid basic auth: {e}"))?;
            headers.insert(AUTHORIZATION, val);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_millis(cluster.request_timeout_ms as u64))
            .build()
            .map_err(|e| format!("[SCHEMA-REGISTRY] http client: {e}"))?;
        Ok(Some(Self { base_url: url, http }))
    }

    pub async fn list_subjects(&self) -> Result<Vec<SchemaSubject>, String> {
        let url = format!("{}/subjects", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] GET subjects: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[SCHEMA-REGISTRY] HTTP {s}: {body}"));
        }
        let names: Vec<String> = resp
            .json()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] decode subjects: {e}"))?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            match self.get_latest_version_meta(&name).await {
                Ok(v) => out.push(SchemaSubject {
                    name: v.subject,
                    version_count: v.version,
                    latest_version: v.version,
                    schema_type: v.schema_type,
                }),
                Err(_) => out.push(SchemaSubject {
                    name,
                    version_count: 0,
                    latest_version: 0,
                    schema_type: "AVRO".to_string(),
                }),
            }
        }
        Ok(out)
    }

    async fn get_latest_version_meta(&self, subject: &str) -> Result<VersionResp, String> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.base_url,
            urlencode(subject)
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] GET latest: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[SCHEMA-REGISTRY] HTTP {s}: {body}"));
        }
        resp.json::<VersionResp>()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] decode version: {e}"))
    }

    pub async fn get_version(&self, subject: &str, version: &str) -> Result<SchemaVersion, String> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url,
            urlencode(subject),
            version
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] GET version: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[SCHEMA-REGISTRY] HTTP {s}: {body}"));
        }
        let v: VersionResp = resp
            .json()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] decode version: {e}"))?;
        Ok(SchemaVersion {
            subject: v.subject,
            version: v.version,
            id: v.id,
            schema_type: v.schema_type,
            schema: v.schema,
        })
    }

    pub async fn delete_version(&self, subject: &str, version: &str) -> Result<(), String> {
        let url = format!(
            "{}/subjects/{}/versions/{}",
            self.base_url,
            urlencode(subject),
            version
        );
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] DELETE: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[SCHEMA-REGISTRY] HTTP {s}: {body}"));
        }
        Ok(())
    }

    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> Result<i32, String> {
        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            urlencode(subject)
        );
        let body = RegisterBody {
            schema,
            schema_type,
        };
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] POST: {e}"))?;
        if !resp.status().is_success() {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("[SCHEMA-REGISTRY] HTTP {s}: {body}"));
        }
        #[derive(Deserialize)]
        struct R {
            id: i32,
        }
        let r: R = resp
            .json()
            .await
            .map_err(|e| format!("[SCHEMA-REGISTRY] decode register: {e}"))?;
        Ok(r.id)
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
