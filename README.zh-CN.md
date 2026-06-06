[English](README.md) | [简体中文](README.zh-CN.md)

# Super Kafka Desktop

跨平台 Apache Kafka 桌面客户端 —— 集群管理、Topic 浏览、消息查看、消息生产 / 重放，全程图形化操作，无需启动终端或额外的 Web UI。

基于 [Tauri 2](https://v2.tauri.app/) + Rust + React 构建，无需 Docker、无需 JVM、无需部署服务器，双击即用。

**仓库地址**：[GitHub](https://github.com/Jacksonary/super-kafka) | [Gitee](https://gitee.com/weiguoliu/super-kafka)

## 功能特性

### 多集群管理
- 支持配置任意数量 Kafka 集群，侧边栏下拉切换
- 应用内直接新增、编辑、删除集群，无需手动改 YAML
- SASL 密码存储在系统 Keychain，配置文件不落明文
- 集群健康状态指示（已连接 / 连接中 / 错误），一键重连

### Topic 管理
- 列表搜索 + 内置 topic 标记 + 列排序
- 创建 / 删除 topic；新增分区；修改保留时间（支持「永久」开关）
- 详情页：分区布局、副本与 ISR、配置项编辑
- 内嵌查看该 topic 的所有消费组及每分区 lag

### 消息浏览
- **Fetch 模式**：Latest / Earliest / From Offset / Time Range 四种取数策略
- **Live 模式**：基于订阅的实时尾巴，500 条滚动缓冲
- 单条消息抽屉：key / value / headers，自动识别编码（JSON 美化展示、二进制走 base64 兜底）
- 已加载消息按 key / value 子串过滤
- 当前视图一键导出 CSV
- 可配置最大显示大小，超大 payload 不阻塞 UI

### 消息生产
- 选择 topic / 分区 / key / 任意 headers
- 压缩算法可选（none / gzip / snappy / zstd / lz4），仅当 payload 超阈值时实际启用编码器
- 任意消息一键 Replay 回写到指定 topic

### 消费组管理
- 列出所有消费组，含状态（Stable / Empty / Dead / Rebalancing）与成员数
- 查看每个成员的 client id、host、被分配的分区
- 删除消费组（仅限 Empty / Dead）
- 按分区粒度重置 offset，支持多种策略（earliest / latest / 指定 offset / 按时间戳）

### 主题与体验
- 亮色 / 暗色一键切换，偏好持久化
- 混合滚动布局 —— 工具栏与分页固定，仅表格内容区参与滚动
- 可配置默认 fetch 数量、最大消息显示大小、多开模式、启动检查更新

## 下载安装

前往 [GitHub Releases](https://github.com/Jacksonary/super-kafka/releases) 或 [Gitee Releases](https://gitee.com/weiguoliu/super-kafka/releases) 页面下载对应平台的安装包：

| 平台 | 格式 |
|---|---|
| Windows 64-bit | `.exe` (NSIS) / `.msi` |
| Linux | `.deb` / `.rpm` / `.AppImage` |
| macOS | `.dmg` |

> Linux AppImage 无需安装，赋予执行权限后直接运行：
> `chmod +x Super\ Kafka_*.AppImage && ./Super\ Kafka_*.AppImage`

## 配置说明

首次启动时集群列表为空，点击侧边栏下拉的 **+ Add Cluster**（或进入 Cluster 页面）添加集群即可。集群元数据保存在系统应用数据目录：

| 系统 | 路径 |
|---|---|
| Linux | `~/.config/super-kafka/clusters.yaml` |
| macOS | `~/Library/Application Support/super-kafka/clusters.yaml` |
| Windows | `%APPDATA%\super-kafka\clusters.yaml` |

SASL 密码独立存放在系统 Keychain（Linux 走 Secret Service，macOS 走 Keychain，Windows 走 Credential Manager），按 cluster id 索引。

应用级偏好（主题、fetch 默认值、多开等）保存在 `app.yaml`，与集群配置同目录。

集群配置格式（YAML 列表，每项一个集群）：

```yaml
- id: 8a2b...                            # 自动生成 UUID
  name: "Local Dev"
  bootstrap_servers: "localhost:9092"
  security_protocol: PLAINTEXT           # 或 SASL_PLAINTEXT / SASL_SSL / SSL
  sasl_mechanism: null                   # PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
  sasl_username: null
  ssl_ca_cert_path: null
  ssl_client_cert_path: null
  ssl_client_key_path: null
  request_timeout_ms: 30000
  created_at: 1717564800
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `name` | 是 | 显示名称 |
| `bootstrap_servers` | 是 | `host:port` 列表，逗号分隔 |
| `security_protocol` | 是 | `PLAINTEXT` / `SASL_PLAINTEXT` / `SASL_SSL` / `SSL` |
| `sasl_mechanism` | SASL 时必填 | `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| `sasl_username` | SASL 时必填 | SASL 用户名；密码存在 Keychain |
| `ssl_*` | SSL 时必填 | 可选的 CA / 客户端证书 / 私钥文件路径 |
| `request_timeout_ms` | 否 | Admin 请求超时，默认 30000 |

## 从源码构建

```bash
# 前提：安装 Rust、Node.js、Tauri 系统依赖
# Linux: sudo apt install libwebkit2gtk-4.1-dev libjavascriptcoregtk-4.1-dev libsoup-3.0-dev librsvg2-dev libayatana-appindicator3-dev cmake
# Tauri CLI: cargo install tauri-cli@^2

git clone https://github.com/Jacksonary/super-kafka.git
cd super-kafka
npm install
cargo tauri build
```

产物位于 `src-tauri/target/release/bundle/`。

> 构建过程内置走 cmake 编译 librdkafka，首次构建会静态编译 librdkafka + OpenSSL + curl + zstd，约需 5–10 分钟。

## 技术栈

| 层 | 技术 |
|---|---|
| 框架 | Tauri 2 |
| 后端 | Rust + rdkafka (librdkafka) |
| 前端 | React 18 + TypeScript + Ant Design 5 |
| 构建 | Vite 5 + Cargo + cmake |

---

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 开源。

## 打赏支持

如果这个项目对你有帮助，欢迎请作者喝瓶啤酒 🍺

<p align="center">
  <table align="center"><tr>
    <td align="center">
      <img src="docs/images/weixinpay.png" width="240" alt="微信打赏"><br>微信
    </td>
    <td width="60"></td>
    <td align="center">
      <img src="docs/images/alipay.png" width="240" alt="支付宝打赏"><br>支付宝
    </td>
  </tr></table>
</p>
