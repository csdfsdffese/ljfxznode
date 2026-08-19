package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/websocket"
)

// WS 事件类型（与面板 NodeEventHandlers 对齐）
const (
	WSEventSyncDevices    = "sync.devices"    // 面板 → 节点：全局设备状态
	WSEventReportDevices  = "report.devices"  // 节点 → 面板：上报本地设备
	WSEventRequestDevices = "request.devices" // 节点 → 面板：主动请求全局设备状态
	WSEventSyncUserDelta  = "sync.user.delta" // 面板 → 节点：用户变更（流量超限移除等）
	WSEventSyncKick       = "sync.kick"       // 面板 → 节点：剔除某用户某 IP 的所有连接（其他节点转发）
	WSEventReportKick     = "report.kick"     // 节点 → 面板：上报剔除请求，由面板转发给该 IP 在线的其他节点
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

// kickPayload 是剔除请求的统一载荷（report.kick / sync.kick 共用）。
// user_id 供面板定位用户、按 IP 转发给其他节点；email/ip 供接收节点断开连接。
type kickPayload struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
	IP     string `json:"ip"`
}

// WSClient 连接面板的 Workerman WebSocket 服务。
// 认证在 WS 握手时通过 query 参数（token + node_id）完成，无需单独鉴权步骤。
// 断线后由 Run 负责指数退避重连；设备状态在断线期间保留旧数据，
// 由调用方配合 alivelist 轮询兜底。
type WSClient struct {
	wsURL         string
	token         string
	nodeID        int
	onDevice      func(users map[int][]string)
	onStatus      func(connected bool)
	onRemoveUsers func(userIDs []int)
	onAddUsers    func(users []UserInfo)
	onKick        func(email string, ip string)

	connected atomic.Bool
	// writeChMu 保护 writeCh：connect（Run goroutine）赋值/置 nil，
	// SendDeviceReport（上报任务 goroutine）读取，两者需要互斥。
	writeChMu sync.Mutex
	writeCh   chan wsMessage
}

// NewWSClient 创建 WS 客户端。
// wsURL 为面板返回的 ws(s):// 地址；token/nodeID 复用 REST 凭据。
// onRemoveUsers 接收面板 sync.user.delta 的移除用户 ID 列表（流量超限主动通知）。
// onAddUsers 接收面板 sync.user.delta 的新增/恢复用户（用户新增或套餐变更），
// 使新用户秒级生效，无需等待下一轮 REST 轮询。
// onKick 接收面板 sync.kick 的剔除请求（email+ip）：本节点断开该用户该 IP 的全部连接。
func NewWSClient(wsURL, token string, nodeID int, onDevice func(map[int][]string), onStatus func(bool), onRemoveUsers func([]int), onAddUsers func([]UserInfo), onKick func(email string, ip string)) *WSClient {
	return &WSClient{
		wsURL:         wsURL,
		token:         token,
		nodeID:        nodeID,
		onDevice:      onDevice,
		onStatus:      onStatus,
		onRemoveUsers: onRemoveUsers,
		onAddUsers:    onAddUsers,
		onKick:        onKick,
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

	// 面板 AUTH_TIMEOUT 为 10s（超时主动断连），首帧读取加 20s 读超时兜底，
	// 避免面板不响应时 connect 永久阻塞。
	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
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
	w.writeChMu.Lock()
	w.writeCh = writeCh
	w.writeChMu.Unlock()
	defer func() {
		w.writeChMu.Lock()
		w.writeCh = nil
		w.writeChMu.Unlock()
	}()

	// 主动请求全局设备状态：面板 pushFullSync 不推 sync.devices，
	// 若不主动请求，节点全局表要到下一次设备上报被回推后才就绪，
	// 存在全局限数盲区（最长约 10s+60s）。
	select {
	case writeCh <- wsMessage{Event: WSEventRequestDevices}:
	default:
	}

	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			// 面板对节点的心跳 ping 间隔为 55s；超过 2 分钟无任何下行消息
			// 即判定连接已死（半开连接/面板进程挂起），主动断开交由 Run 重连，
			// 避免 connected 长期虚真、设备上报无效堆积。
			conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
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
			// reader goroutine 可能因服务器不响应 CloseMessage 而永久阻塞，
			// 加超时避免 connect 永不返回。
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
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
	case WSEventSyncUserDelta:
		// 面板 payload: {action: "add"|"remove", users:[{id, uuid, speed_limit, device_limit}]}
		// add 为新增/恢复用户，remove 为流量超限/禁用移除。字段与 UserInfo 对齐，
		// 直接复用结构体解码，新增用户即可携带限速/限设备数，无需二次 REST 拉取。
		var payload struct {
			Action string     `json:"action"`
			Users  []UserInfo `json:"users"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			log.WithField("err", err).Warn("ws: decode sync.user.delta failed")
			return
		}
		switch payload.Action {
		case "add":
			if w.onAddUsers != nil && len(payload.Users) > 0 {
				w.onAddUsers(payload.Users)
			}
		case "remove":
			if w.onRemoveUsers == nil {
				return
			}
			ids := make([]int, 0, len(payload.Users))
			for _, u := range payload.Users {
				if u.Id > 0 {
					ids = append(ids, u.Id)
				}
			}
			if len(ids) > 0 {
				w.onRemoveUsers(ids)
			}
		}
	case WSEventSyncKick:
		// 面板转发其他节点的剔除请求：本节点断开该用户该 IP 的全部连接。
		var payload struct {
			Email string `json:"email"`
			IP    string `json:"ip"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			log.WithField("err", err).Warn("ws: decode sync.kick failed")
			return
		}
		if w.onKick != nil && payload.IP != "" {
			w.onKick(payload.Email, payload.IP)
		}
	case "ping":
		// pong 由读循环回写
	default:
		// sync.config / sync.users / auth.success 等事件与设备限制无关，忽略
	}
}

// decodeSyncDevices 宽松解析 sync.devices 的 users 载荷。
// 面板 DeviceStateService::getUsersDevices 用 array_unique 去重后，同一用户
// 跨节点重复 IP 时 PHP 数组下标不连续，JSON 可能输出为对象
// （{"0":"ip1","2":"ip2"}）而非数组，直接解析 map[int][]string 会整体失败。
// 解析顺序：强类型数组 → 宽松对象（按 key 排序还原）→ 单字符串 → 丢弃。
func decodeSyncDevices(raw json.RawMessage) (map[int][]string, error) {
	var strict syncDevicesPayload
	if err := json.Unmarshal(raw, &strict); err == nil && strict.Users != nil {
		return strict.Users, nil
	}
	// 宽松兜底：逐 uid 解析，值可能是数组、对象或单字符串
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
			// 对象格式：array_unique 后下标不连续，key 为数字字符串，
			// 按下标排序保证 IP 列表顺序确定（内容集合等价）。
			var ipMap map[string]string
			if err := json.Unmarshal(v, &ipMap); err != nil {
				var single string
				if err2 := json.Unmarshal(v, &single); err2 != nil {
					continue
				}
				ips = []string{single}
			} else {
				keys := make([]int, 0, len(ipMap))
				for k := range ipMap {
					if n, convErr := strconv.Atoi(k); convErr == nil {
						keys = append(keys, n)
					}
				}
				sort.Ints(keys)
				ips = make([]string, 0, len(keys))
				for _, k := range keys {
					ips = append(ips, ipMap[strconv.Itoa(k)])
				}
			}
		}
		out[uid] = ips
	}
	return out, nil
}

// SendDeviceReport 通过 WS 上报设备快照（userID → IP 列表）。
// 允许空表：调用方用于「设备归零」时清空面板端本节点记录（面板按差集清除）。
// 未连接或写队列满时返回 false（调用方应保留 hash 并在下一轮重试，
// 避免该次设备变化因队列丢弃而永久丢失）。
func (w *WSClient) SendDeviceReport(devices map[int][]string) bool {
	if !w.connected.Load() {
		return false
	}
	data, err := json.Marshal(devices)
	if err != nil {
		return false
	}
	msg := wsMessage{
		Event:     WSEventReportDevices,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
	w.writeChMu.Lock()
	ch := w.writeCh
	w.writeChMu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- msg:
		return true
	default:
		log.Warn("ws write channel full, skipping device report")
		return false
	}
}

// SendKickReport 通过 WS 上报剔除请求：本节点设备满员时选定 victim IP，
// 上报面板由面板转发给该 IP 在线的其他节点，完成跨节点剔除。
// 未连接或写队列满时返回 false（剔除动作仅影响其他节点，本地已由
// dispatcher 直接断开，未上报可接受，不强重试）。
func (w *WSClient) SendKickReport(uid int, email string, ip string) bool {
	if !w.connected.Load() {
		return false
	}
	data, err := json.Marshal(kickPayload{UserID: uid, Email: email, IP: ip})
	if err != nil {
		return false
	}
	msg := wsMessage{
		Event:     WSEventReportKick,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
	w.writeChMu.Lock()
	ch := w.writeCh
	w.writeChMu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- msg:
		return true
	default:
		log.Warn("ws write channel full, skipping kick report")
		return false
	}
}
