# ljfxznode

[![](https://img.shields.io/badge/TgChat-UnOfficialV2Board%E4%BA%A4%E6%B5%81%E7%BE%A4-green)](https://t.me/unofficialV2board)
[![](https://img.shields.io/badge/TgChat-YuzukiProjects%E4%BA%A4%E6%B5%81%E7%BE%A4-blue)](https://t.me/YuzukiProjects)

A V2board/Xboard node server based on Xray core, modified from V2bX / XrayR.
一个基于 Xray 内核的 V2board / Xboard 节点服务端，修改自 V2bX / XrayR，支持 Vmess、Vless、Trojan、Shadowsocks 协议。

**注意：本项目需要搭配 [V2board](https://github.com/v2board/v2board) 或 [Xboard](https://github.com/cedar2025/Xboard) 面板使用**

## 特点

* 永久开源且免费。
* 仅内置 Xray 内核，支持 Vmess/Vless/Trojan/Shadowsocks 协议。
* 支持 Vless XTLS、Reality、XHTTP 等新特性。
* 支持单实例对接多节点，无需重复启动。
* 支持限制在线 IP。
* 支持限制 Tcp 连接数。
* 支持节点端口级别、用户级别限速。
* 配置简单明了。
* 修改配置自动重启实例。
* 完整兼容 V2board 与 Xboard 面板。

## 功能介绍

| 功能        | v2ray | trojan | shadowsocks | vless |
|-----------|-------|--------|-------------|-------|
| 自动申请tls证书 | ✔    | ✔     | ✔          | ✔    |
| 自动续签tls证书 | ✔    | ✔     | ✔          | ✔    |
| 在线人数统计    | ✔    | ✔     | ✔          | ✔    |
| 审计规则      | ✔    | ✔     | ✔          | ✔    |
| 自定义DNS    | ✔    | ✔     | ✔          | ✔    |
| 在线IP数限制  | ✔    | ✔     | ✔          | ✔    |
| 连接数限制    | ✔    | ✔     | ✔          | ✔    |
| 跨节点IP数限制 | ✔    | ✔     | ✔          | ✔    |
| 按照用户限速   | ✔    | ✔     | ✔          | ✔    |
| 动态限速(未测试) | ✔    | ✔     | ✔          | ✔    |

## TODO

- [ ] 重新实现动态限速
- [ ] 完善使用文档

## 软件安装

### 一键安装

```
wget -N https://raw.githubusercontent.com/csdfsdffese/ljfxznode-script/master/install.sh && bash install.sh
```

### 手动安装

[手动安装教程](https://ljfxznode.v-50.me/ljfxznode/ljfxznode-xia-zai-he-an-zhuang/install/manual)

## 构建
``` bash
# 仅编译 Xray 内核，无需 -tags
GOEXPERIMENT=jsonv2 go build -v -o build_assets/ljfxznode -trimpath -ldflags "-X 'github.com/csdfsdffese/ljfxznode/cmd.version=$version' -s -w -buildid="
```

## 配置文件及详细使用教程

[详细使用教程](https://ljfxznode.v-50.me/)

## 免责声明

* 此项目用于本人自用，因此本人不能保证向后兼容性。
* 由于本人能力有限，不能保证所有功能的可用性，如果出现问题请在Issues反馈。
* 本人不对任何人使用本项目造成的任何后果承担责任。
* 本人比较多变，因此本项目可能会随想法或思路的变动随性更改项目结构或大规模重构代码，若不能接受请勿使用。

## 赞助

[赞助链接](https://v-50.me/)

## Thanks

* [Project X](https://github.com/XTLS/)
* [V2Fly](https://github.com/v2fly)
* [VNet-V2ray](https://github.com/ProxyPanel/VNet-V2ray)
* [Air-Universe](https://github.com/crossfw/Air-Universe)
* [XrayR](https://github.com/XrayR/XrayR)
* [V2bX](https://github.com/wyx2685/V2bX)

## Stars 增长记录

[![Stargazers over time](https://starchart.cc/csdfsdffese/ljfxznode.svg)](https://starchart.cc/csdfsdffese/ljfxznode)
