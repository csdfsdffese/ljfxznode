# ljfxznode

**Xboard 专用** Xray 节点服务端（仅 Xray 内核），修改自 [V2bX](https://github.com/wyx2685/V2bX) 与 [Xboard-Node](https://github.com/cedar2025/Xboard-Node)，基于 [Xray-core](https://github.com/XTLS/Xray-core)。

仅支持 [Xboard](https://github.com/cedar2025/Xboard) 面板（V2 API + WebSocket 设备同步），**不兼容 V2board**。
支持 Vmess、Vless、Trojan、Shadowsocks 协议。

## 特点

* 永久开源且免费。
* 仅内置 Xray 内核，支持 Vmess/Vless/Trojan/Shadowsocks 协议。
* 支持 Vless XTLS、Reality、XHTTP（splithttp）等新特性。
* 支持单实例对接多节点，无需重复启动。
* 支持在线 IP 数限制（device_limit）。
* 跨节点全局 IP 数限制（面板 WebSocket 设备同步 + REST alivelist 轮询兜底；被剔除 IP 冷却占坑，冷却期内任意节点拒绝重连，到期自动解禁）。
* 支持限制 TCP 连接数。
* 支持节点级、用户级限速（上行 / 下行双向生效）。
* 自动申请 / 自动续签 TLS 证书（HTTP / DNS / 自签）。
* 支持自定义 DNS、审计路由规则。
* 配置修改自动热加载。
* 节点状态上报（CPU / 内存 / 磁盘）。

## 架构

对接 Xboard 采用 **REST + WebSocket 混合**通道：

| 通道 | 用途 |
|---|---|
| REST（V2 API） | 握手 `server/handshake`、拉取节点配置 / 用户、流量 / 在线 / 状态上报、alivelist 兜底 |
| WebSocket（`/ws`） | 设备同步：节点 10s 上报 `report.devices`，面板全局去重后推送 `sync.devices`，实现跨节点全局 IP 限制 |

WebSocket 断开时自动指数退避重连（1s → 60s，带抖动），期间保留本地连接级设备计数，并配合 REST alivelist 轮询兜底，确保限制不失效。

## 功能介绍

| 功能 | v2ray | trojan | shadowsocks | vless |
|---|---|---|---|---|
| 自动申请 TLS 证书 | ✔ | ✔ | ✔ | ✔ |
| 自动续签 TLS 证书 | ✔ | ✔ | ✔ | ✔ |
| 在线人数统计 | ✔ | ✔ | ✔ | ✔ |
| 审计规则 | ✔ | ✔ | ✔ | ✔ |
| 自定义 DNS | ✔ | ✔ | ✔ | ✔ |
| 在线 IP 数限制 | ✔ | ✔ | ✔ | ✔ |
| 连接数限制 | ✔ | ✔ | ✔ | ✔ |
| 跨节点全局 IP 数限制 | ✔ | ✔ | ✔ | ✔ |
| 用户级限速 | ✔ | ✔ | ✔ | ✔ |

## 软件安装

### 一键安装（[ljfxznode-script](https://github.com/csdfsdffese/ljfxznode-script)）

```
wget -N https://raw.githubusercontent.com/csdfsdffese/ljfxznode-script/master/install.sh && bash install.sh
```

安装完成后运行 `ljfxznode` 进入管理菜单，按提示配置面板地址、通信密钥、节点 ID 与协议。

### 面板侧要求

* Xboard 后台开启「WebSocket 通信」（`server_ws_enable`），并保证 Workerman WS 服务存活。
* 节点 `network_settings` 中如需反代取真实 IP，请配置 `socketSettings.trustedXForwardedFor`（X-Forwarded-For）；或在内核侧已默认信任 XFF 的前提下配合 Nginx `proxy_set_header X-Forwarded-For $remote_addr;`。

## 构建

依赖官方 Xray v26.7.28，仅需设置 `GOPRIVATE=github.com/csdfsdffese/*`（拉取定制内核模块，编译无需任何实验特性）：

```bash
export GOPRIVATE=github.com/csdfsdffese/*
version=xxx

go build -v -o build_assets/ljfxznode -trimpath \
  -ldflags "-X 'github.com/csdfsdffese/ljfxznode/cmd.version=$version' -s -w -buildid="
```

内核通过 `go.mod` 的 `replace` 指向 `github.com/csdfsdffese/xray-core`（在官方 v26.7.28 基础上仅含一处定制：默认信任 X-Forwarded-For 以支持反代取真实 IP，其余与官方完全一致）。

## 配置

`config.json` 核心结构示例：

```json
{
  "Cores": [
    {
      "Type": "xray",
      "AssetPath": "/etc/ljfxznode/",
      "DnsConfigPath": "/etc/ljfxznode/dns.json",
      "RouteConfigPath": "/etc/ljfxznode/route.json",
      "InboundConfigPath": "/etc/ljfxznode/custom_inbound.json",
      "OutboundConfigPath": "/etc/ljfxznode/custom_outbound.json"
    }
  ],
  "Nodes": [
    {
      "Core": "xray",
      "ApiHost": "https://panel.example.com",
      "ApiKey": "通信密钥",
      "NodeID": 1,
      "NodeType": "vless",
      "Timeout": 30,
      "ListenIP": "0.0.0.0",
      "SendIP": "0.0.0.0",
      "DeviceOnlineMinTraffic": 200,
      "ReportMinTraffic": 0,
      "EnableProxyProtocol": false,
      "EnableDNS": false,
      "DNSType": "AsIs",
      "EnableTFO": false,
      "DisableSniffing": false,
      "EnableFallback": false,
      "CertConfig": {
        "CertMode": "self",
        "RejectUnknownSni": false,
        "CertDomain": "example.com",
        "CertFile": "/etc/ljfxznode/fullchain.cer",
        "KeyFile": "/etc/ljfxznode/cert.key",
        "Provider": "cloudflare",
        "DNSEnv": {}
      }
    }
  ]
}
```

完整示例见 [example/config.json](example/config.json)。

## 命令行

```
ljfxznode server     启动节点服务
ljfxznode version    查看版本
ljfxznode x25519     生成 Reality 密钥对
ljfxznode synctime   同步系统时间
ljfxznode update     更新程序
ljfxznode uninstall  卸载
ljfxznode start|stop|restart|log   服务管理（Linux）
```

## 免责声明

* 此项目用于本人自用。

## Thanks

* [Project X](https://github.com/XTLS/)
* [XrayR](https://github.com/XrayR-project/XrayR)
* [V2bX](https://github.com/wyx2685/V2bX)
* [Xboard](https://github.com/cedar2025/Xboard)
* [Xboard-Node](https://github.com/cedar2025/Xboard-Node)
