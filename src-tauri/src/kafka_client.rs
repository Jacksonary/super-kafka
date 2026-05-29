use crate::types::ClusterConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, DefaultProducerContext};

pub struct KafkaClientBundle {
    pub admin: AdminClient<DefaultClientContext>,
    pub producer: BaseProducer<DefaultProducerContext>,
}

pub fn build_client_config(cluster: &ClusterConfig, password: Option<&str>) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &cluster.bootstrap_servers);
    cfg.set(
        "request.timeout.ms",
        cluster.request_timeout_ms.to_string(),
    );
    cfg.set(
        "socket.timeout.ms",
        (cluster.request_timeout_ms + 5_000).to_string(),
    );
    cfg.set("client.id", format!("super-kafka/{}", cluster.id));

    let protocol = cluster.security_protocol.to_uppercase();
    cfg.set("security.protocol", &protocol);

    let needs_ssl = matches!(protocol.as_str(), "SSL" | "SASL_SSL");
    if needs_ssl {
        if let Some(ca) = &cluster.ssl_ca_cert_path {
            cfg.set("ssl.ca.location", ca);
        }
        if let Some(cert) = &cluster.ssl_client_cert_path {
            cfg.set("ssl.certificate.location", cert);
        }
        if let Some(key) = &cluster.ssl_client_key_path {
            cfg.set("ssl.key.location", key);
        }
        cfg.set("enable.ssl.certificate.verification", "true");
    }

    let needs_sasl = matches!(protocol.as_str(), "SASL_PLAINTEXT" | "SASL_SSL");
    if needs_sasl {
        if let Some(mech) = &cluster.sasl_mechanism {
            cfg.set("sasl.mechanism", mech);
        }
        if let Some(user) = &cluster.sasl_username {
            cfg.set("sasl.username", user);
        }
        if let Some(pw) = password {
            cfg.set("sasl.password", pw);
        }
    }

    cfg
}

pub fn create_bundle(
    cluster: &ClusterConfig,
    password: Option<&str>,
) -> Result<KafkaClientBundle, String> {
    let cfg = build_client_config(cluster, password);
    let admin: AdminClient<DefaultClientContext> = cfg
        .create()
        .map_err(|e| format!("[KAFKA-ADMIN] failed to create admin client: {e}"))?;
    let producer: BaseProducer<DefaultProducerContext> = cfg
        .create()
        .map_err(|e| format!("[KAFKA-PRODUCER] failed to create producer: {e}"))?;
    Ok(KafkaClientBundle { admin, producer })
}
