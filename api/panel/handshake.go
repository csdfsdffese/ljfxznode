package panel

import (
	"encoding/json"
	"fmt"
)

// HandshakeResponse 是 POST /api/v2/server/handshake 的响应结构。
// WebSocket 仅在面板启用了 WS 服务（server_ws_enable=1 且 ws-server 进程存活）时返回。
type HandshakeResponse struct {
	WebSocket WSConfig `json:"websocket"`
	Settings  Settings `json:"settings"`
}

// WSConfig 是面板 WS 服务的连接参数。
type WSConfig struct {
	Enabled bool   `json:"enabled"`
	WSURL   string `json:"ws_url,omitempty"`
}

// Settings 是面板定义的推送/拉取间隔（秒）。
type Settings struct {
	PushInterval int `json:"push_interval"`
	PullInterval int `json:"pull_interval"`
}

// Handshake 探测面板是否启用 WebSocket 设备推送及 WS 地址。
// 失败或未启用时调用方应回退到 alivelist 轮询，不能视为致命错误。
func (c *Client) Handshake() (*HandshakeResponse, error) {
	const path = "/api/v2/server/handshake"
	r, err := c.client.R().
		ForceContentType("application/json").
		Post(path)
	if err = c.checkResponse(r, path, err); err != nil {
		return nil, err
	}
	hs := &HandshakeResponse{}
	if err := json.Unmarshal(r.Body(), hs); err != nil {
		return nil, fmt.Errorf("decode handshake error: %s", err)
	}
	return hs, nil
}
