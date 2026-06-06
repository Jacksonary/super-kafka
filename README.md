[English](README.md) | [简体中文](README.zh-CN.md)

# Super Kafka Desktop

A cross-platform desktop client for Apache Kafka — manage clusters, browse topics, inspect messages, and produce / replay records, all without launching a terminal or a separate web UI.

Built with [Tauri 2](https://v2.tauri.app/) + Rust + React. No Docker, no JVM, no server deployment — just download and run.

**Repository**: [GitHub](https://github.com/Jacksonary/super-kafka) | [Gitee](https://gitee.com/weiguoliu/super-kafka)

## Features

### Multi-Cluster Management
- Configure unlimited Kafka clusters; switch between them from the sidebar dropdown
- In-app add / edit / delete cluster — no manual YAML editing required
- SASL credentials stored in the OS keychain, never in plain-text configs
- Per-cluster health indicator (connected / connecting / error) with one-click reconnect

### Topics
- Searchable list with internal-topic tagging, sortable columns
- Create / delete topics; add partitions; edit retention (with a "Forever" toggle)
- Detail page with partition layout, replica & ISR view, full config editor
- Browse the topic's consumer groups inline, including per-partition lag

### Messages
- **Fetch modes**: Latest / Earliest / From Offset / Time Range
- **Live mode**: real-time tail with subscription-based consumption (rolling 500-message buffer)
- Per-message detail drawer: key, value, headers, encoding auto-detection (JSON pretty-print, base64 fallback for binary)
- Filter loaded messages by key / value substring
- Export the current view to CSV
- Configurable max display size to keep huge payloads from clogging the UI

### Producer
- Select topic, partition, key and arbitrary headers
- Choose compression codec (none / gzip / snappy / zstd / lz4) — codec auto-engages only when payload exceeds threshold
- Replay any fetched message back into a topic with one click

### Consumer Groups
- List all groups with state (Stable / Empty / Dead / Rebalancing) and member count
- Inspect each member's client id, host, and assigned partitions
- Delete a group (must be Empty / Dead)
- Reset offsets per-partition with multiple strategies (earliest / latest / specific offset / by timestamp)

### Theming & UX
- Light / Dark mode with instant switching, preference persisted across launches
- Hybrid scroll model — toolbar and pagination stay pinned while only the table body scrolls
- Configurable default fetch limit, max display size, multi-window mode, and startup update check

## Download

Head to [GitHub Releases](https://github.com/Jacksonary/super-kafka/releases) or [Gitee Releases](https://gitee.com/weiguoliu/super-kafka/releases) to grab the installer for your platform:

| Platform | Format |
|---|---|
| Windows 64-bit | `.exe` (NSIS) / `.msi` |
| Linux | `.deb` / `.rpm` / `.AppImage` |
| macOS | `.dmg` |

> For Linux AppImage, no installation is required — just make it executable and run:
> `chmod +x Super\ Kafka_*.AppImage && ./Super\ Kafka_*.AppImage`

## Configuration

On first launch the cluster list is empty. Click **+ Add Cluster** in the sidebar dropdown (or open the Cluster page) to add one. Cluster metadata is saved to the system application data directory:

| OS | Path |
|---|---|
| Linux | `~/.config/super-kafka/clusters.yaml` |
| macOS | `~/Library/Application Support/super-kafka/clusters.yaml` |
| Windows | `%APPDATA%\super-kafka\clusters.yaml` |

SASL passwords are stored separately in the OS keychain (Secret Service on Linux, Keychain on macOS, Credential Manager on Windows), keyed by cluster id.

App-level preferences (theme, fetch defaults, multi-window, etc.) live in `app.yaml` next to the cluster config.

Cluster config format (YAML list — one entry per cluster):

```yaml
- id: 8a2b...                            # auto-generated UUID
  name: "Local Dev"
  bootstrap_servers: "localhost:9092"
  security_protocol: PLAINTEXT           # or SASL_PLAINTEXT / SASL_SSL / SSL
  sasl_mechanism: null                   # PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
  sasl_username: null
  ssl_ca_cert_path: null
  ssl_client_cert_path: null
  ssl_client_key_path: null
  request_timeout_ms: 30000
  created_at: 1717564800
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Display name |
| `bootstrap_servers` | Yes | Comma-separated `host:port` list |
| `security_protocol` | Yes | `PLAINTEXT` / `SASL_PLAINTEXT` / `SASL_SSL` / `SSL` |
| `sasl_mechanism` | If SASL | `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| `sasl_username` | If SASL | SASL username; password is in the keychain |
| `ssl_*` | If SSL | Optional CA / client cert / client key file paths |
| `request_timeout_ms` | No | Admin request timeout, defaults to 30000 |

## Building from Source

```bash
# Prerequisites: Rust, Node.js, and Tauri system dependencies
# Linux: sudo apt install libwebkit2gtk-4.1-dev libjavascriptcoregtk-4.1-dev libsoup-3.0-dev librsvg2-dev libayatana-appindicator3-dev cmake
# Tauri CLI: cargo install tauri-cli@^2

git clone https://github.com/Jacksonary/super-kafka.git
cd super-kafka
npm install
cargo tauri build
```

Build artifacts are located at `src-tauri/target/release/bundle/`.

> The build vendors librdkafka via cmake; first build can take 5–10 min while it compiles librdkafka + OpenSSL + curl + zstd statically.

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | Tauri 2 |
| Backend | Rust + rdkafka (librdkafka) |
| Frontend | React 18 + TypeScript + Ant Design 5 |
| Build | Vite 5 + Cargo + cmake |

---

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Buy Me a Beer

If you find this project helpful, feel free to buy the author a beer 🍺

<p align="center">
  <table align="center"><tr>
    <td align="center">
      <img src="docs/images/weixinpay.png" width="240" alt="WeChat Pay"><br>WeChat
    </td>
    <td width="60"></td>
    <td align="center">
      <img src="docs/images/alipay.png" width="240" alt="Alipay"><br>Alipay
    </td>
  </tr></table>
</p>
