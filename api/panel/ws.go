package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/websocket"
)

// WS 事件类型（与面板 NodeEventHandlers 对齐）
const (
	WSEventSyncDevices   = "sync.devices"   // 面板 → 节点：全局设备状态
	WSEventReportDevices = "report.devices" // 节点 → 面板：上报本地设备
)

// wsMessage 是所有 WS 消息的 JSON 信封。
type wsMessage struct {
	Event     string          `json:"event"`
	Data      json.RawMessage `json:"data,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"`
}

// syncDevicesPayload 承载面板推送的全局设备列表。
// users 为 userID → 在线 IP 列表（跨所有节点的去重结果）。
type syncDevicesPayload struct {
	Users     map[int][]string `json:"users"`
	Timestamp int64            `json:"timestamp"`
	NodeID    int              `json:"node_id"`
}

// WSClient 连接面板的 Workerman WebSocket 服务。
// 认证在 WS 握手时通过 query 参数（token + node_id）完成，无需单独鉴权步骤。
// 断线后由 Run 负责指数退避重连；设备状态在断线期间保留旧数据，
// 由调用方配合 alivelist 轮询兜底。
type WSClient struct {
	wsURL    string
	token    string
	nodeID   int
	onDevice func(users map[int][]string)
	onStatus func(connected bool)

	connected atomic.Bool
	writeCh   chan wsMessage
}

// NewWSClient 创建 WS 客户端。
// wsURL 为面板返回的 ws(s):// 地址；token/nodeID 复用 REST 凭据。
func NewWSClient(wsURL, token string, nodeID int, onDevice func(map[int][]string), onStatus func(bool)) *WSClient {
	return &WSClient{
		wsURL:    wsURL,
		token:    token,
		nodeID:   nodeID,
		onDevice: onDevice,
		onStatus: onStatus,
	}
}

func (w *WSClient) IsConnected() bool { return w.connected.Load() }

// Run 循环连接直到 ctx 取消，断线后指数退避重连（带抖动防羊群效应）。
func (w *WSClient) Run(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 60 * time.Second
	for {
		start := time.Now()
		err := w.connect(ctx)

		wasConnected := w.connected.Swap(false)
		if wasConnected {
			w.notifyStatus(false)
		} else if err != nil {
			w.notifyStatus(false)
		}
		if err != nil {
			log.WithField("err", err).Warn("WebSocket disconnected, reconnecting")
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		// 连接稳定运行超过 2 分钟则重置退避
		if time.Since(start) > 2*time.Minute {
			backoff = time.Second
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 5)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func (w *WSClient) notifyStatus(connected bool) {
	if w.onStatus != nil {
		w.onStatus(connected)
	}
}

func (w *WSClient) connect(ctx context.Context) error {
	u, err := url.Parse(w.wsURL)
	if err != nil {
		return fmt.Errorf("parse ws url: %w", err)
	}
	q := u.Query()
	q.Set("token", w.token)
	q.Set("node_id", strconv.Itoa(w.nodeID))
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(10 << 20) // 10MB 最大消息

	// 首帧应为 auth.success 或 error；个别面板可能直接推送数据事件
	var first wsMessage
	if err := conn.ReadJSON(&first); err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	if first.Event == "error" {
		return fmt.Errorf("ws auth failed: %s", string(first.Data))
	}
	if first.Event != "auth.success" {
		w.connected.Store(true)
		w.notifyStatus(true)
		w.handleMessage(first)
	} else {
		w.connected.Store(true)
		w.notifyStatus(true)
	}

	writeCh := make(chan wsMessage, 16)
	w.writeCh = writeCh
	defer func() { w.writeCh = nil }()

	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			w.handleMessage(msg)
			if msg.Event == "ping" {
				select {
				case writeCh <- wsMessage{Event: "pong"}:
				default:
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			<-done
			return nil

		case err := <-errCh:
			return fmt.Errorf("read: %w", err)

		case msg := <-writeCh:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(msg); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}
}

func (w *WSClient) handleMessage(msg wsMessage) {
	switch msg.Event {
	case WSEventSyncDevices:
		users, err := decodeSyncDevices(msg.Data)
		if err != nil {
			log.WithField("err", err).Warn("ws: decode sync.devices failed")
			return
		}
		if w.onDevice != nil {
			w.onDevice(users)
		}
	case "ping":
		// pong 由读循环回写
	default:
		// sync.config / sync.users / auth.success 等事件与设备限制无关，忽略
	}
}

// decodeSyncDevices 宽松解析 sync.devices 的 users 载荷。
// 面板 DeviceStateService 用 array_unique 去重后，同一用户跨节点重复 IP 时
// JSON 可能输出为对象（{"0":"ip1","2":"ip2"}）而非数组，直接解析
// map[int][]string 会整体失败。先按强类型解析，失败再走宽松路径。
func decodeSyncDevices(raw json.RawMessage) (map[int][]string, error) {
	var strict syncDevicesPayload
	if err := json.Unmarshal(raw, &strict); err == nil && strict.Users != nil {
		return strict.Users, nil
	}
	// 宽松兜底：逐 uid 解析，值可能是 []interface{} 或 string
	var lax struct {
		Users map[string]json.RawMessage `json:"users"`
	}
	if err := json.Unmarshal(raw, &lax); err != nil {
		return nil, err
	}
	out := make(map[int][]string, len(lax.Users))
	for uidStr, v := range lax.Users {
		uid, err := strconv.Atoi(uidStr)
		if err != nil {
			continue
		}
		var ips []string
		if err := json.Unmarshal(v, &ips); err != nil {
			var single string
			if err2 := json.Unmarshal(v, &single); err2 != nil {
				continue
			}
			ips = []string{single}
		}
		out[uid] = ips
	}
	return out, nil
}

// SendDeviceReport 通过 WS 上报本地设备快照（userID → IP 列表）。
// 未连接时静默丢弃（调用方应保留 REST alive 上报作为兜底）。
func (w *WSClient) SendDeviceReport(devices map[int][]string) {
	if !w.connected.Load() || len(devices) == 0 {
		return
	}
	data, err := json.Marshal(devices)
	if err != nil {
		return
	}
	msg := wsMessage{
		Event:     WSEventReportDevices,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
	select {
	case w.writeCh <- msg:
	default:
		log.Warn("ws write channel full, skipping device report")
	}
}
