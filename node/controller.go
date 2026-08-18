package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/csdfsdffese/ljfxznode/api/panel"
	"github.com/csdfsdffese/ljfxznode/common/task"
	"github.com/csdfsdffese/ljfxznode/conf"
	vCore "github.com/csdfsdffese/ljfxznode/core"
	"github.com/csdfsdffese/ljfxznode/limiter"
	log "github.com/sirupsen/logrus"
)

type Controller struct {
	server                  vCore.Core
	apiClient               *panel.Client
	tag                     string
	limiter                 *limiter.Limiter
	userList                []panel.UserInfo
	userListMu              sync.Mutex   // 保护 userList（nodeInfoMonitor 写 / WS sync.user.delta 回调读）
	stateMu                 sync.RWMutex // 保护 info/tag/limiter（任务 goroutine 写 / WS 回调读）
	pendingTraffic          []panel.UserTraffic
	info                    *panel.NodeInfo
	nodeInfoMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	statusReportPeriodic    *task.Task
	renewCertPeriodic       *task.Task
	deviceReportPeriodic    *task.Task
	statusChecker           *statusChecker
	wsClient                *panel.WSClient
	wsCancel                context.CancelFunc
	deviceReportMu          sync.Mutex // 保护 lastReportDevicesHash（WS 重连回调与上报任务并发访问）
	lastReportDevicesHash   string
	*conf.Options
}

// NewController return a Node controller with default parameters.
func NewController(server vCore.Core, api *panel.Client, config *conf.Options) *Controller {
	controller := &Controller{
		server:        server,
		Options:       config,
		apiClient:     api,
		statusChecker: newStatusChecker(),
	}
	return controller
}

// Start implement the Start() function of the service interface
func (c *Controller) Start() error {
	// First fetch Node Info
	var err error
	node, err := c.apiClient.GetNodeInfo()
	if err != nil {
		return fmt.Errorf("get node info error: %s", err)
	}
	// Update user
	c.userList, err = c.apiClient.GetUserList()
	if err != nil {
		return fmt.Errorf("get user list error: %s", err)
	}
	if len(c.userList) == 0 {
		// Do not abort startup: the panel may be mid-restart or have all users
		// temporarily disabled. Aborting here would take the whole node down;
		// the next nodeInfoMonitor tick will pick up users as soon as the
		// panel returns them.
		log.Warn("no user received from panel, waiting for the next pull")
	}
	aliveMap, err := c.apiClient.GetUserAlive()
	if err != nil {
		// 启动时 alivelist 失败不阻断节点启动：以空表继续，alivelist 兜底
		// 会在后续 nodeInfoMonitor 轮询恢复；真正的全局限数由 WS 通道承载。
		log.WithField("tag", c.tag).Warn("failed to get alive list, continue with empty list")
		aliveMap = make(map[int]int)
	}
	if len(c.Options.Name) == 0 {
		c.tag = c.buildNodeTag(node)
	} else {
		c.tag = c.Options.Name
	}

	// add limiter
	l := limiter.AddLimiter(c.tag, &c.LimitConfig, c.userList, aliveMap)
	// add rule limiter
	if err = l.UpdateRule(&node.Rules); err != nil {
		return fmt.Errorf("update rule error: %s", err)
	}
	c.limiter = l
	if node.Security == panel.Tls {
		err = c.requestCert()
		if err != nil {
			return fmt.Errorf("request cert error: %s", err)
		}
	}
	// Add new tag
	err = c.server.AddNode(c.tag, node, c.Options)
	if err != nil {
		return fmt.Errorf("add new node error: %s", err)
	}
	added, err := c.server.AddUsers(&vCore.AddUsersParams{
		Tag:      c.tag,
		Users:    c.userList,
		NodeInfo: node,
	})
	if err != nil {
		return fmt.Errorf("add users error: %s", err)
	}
	log.WithField("tag", c.tag).Infof("Added %d new users", added)
	c.info = node
	c.startTasks(node)
	c.startWS()
	return nil
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
	if c.wsCancel != nil {
		c.wsCancel()
	}
	limiter.DeleteLimiter(c.tag)
	if c.nodeInfoMonitorPeriodic != nil {
		c.nodeInfoMonitorPeriodic.Close()
	}
	if c.userReportPeriodic != nil {
		c.userReportPeriodic.Close()
	}
	if c.statusReportPeriodic != nil {
		c.statusReportPeriodic.Close()
	}
	if c.renewCertPeriodic != nil {
		c.renewCertPeriodic.Close()
	}
	if c.deviceReportPeriodic != nil {
		c.deviceReportPeriodic.Close()
	}
	err := c.server.DelNode(c.tag)
	if err != nil {
		return fmt.Errorf("del node error: %s", err)
	}
	return nil
}

// reportStatusTask collects system load stats and reports them to the panel.
// Failures are logged only so that a transient panel outage never stops the task.
func (c *Controller) reportStatusTask() error {
	status := c.statusChecker.collect()
	c.stateMu.RLock()
	tag := c.tag
	c.stateMu.RUnlock()
	if err := c.apiClient.ReportNodeStatus(status); err != nil {
		log.WithFields(log.Fields{
			"tag": tag,
			"err": err,
		}).Debug("Report node status failed")
	}
	return nil
}

// startWS 探测面板 WS 服务并启动设备状态推送客户端。
// WS 用于下行全局设备表（sync.devices）实时更新全局限数；
// 面板未启用 WS 或探测失败时静默回退到 alivelist 轮询（不影响节点可用性）。
func (c *Controller) startWS() {
	// 防御：startWS 若被重复调用（例如未来加入面板 WS 地址变更后的重建），
	// 先取消旧连接的 context，避免旧 WS goroutine 永久挂起、连接叠加泄漏。
	if c.wsCancel != nil {
		c.wsCancel()
	}
	hs, err := c.apiClient.Handshake()
	if err != nil {
		log.WithFields(log.Fields{"tag": c.tag, "err": err}).
			Debug("WebSocket handshake failed, fallback to polling")
		return
	}
	if !hs.WebSocket.Enabled || hs.WebSocket.WSURL == "" {
		log.WithField("tag", c.tag).Debug("WebSocket not enabled by panel, fallback to polling")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.wsCancel = cancel
	c.wsClient = panel.NewWSClient(
		hs.WebSocket.WSURL,
		c.apiClient.Key,
		c.apiClient.NodeId,
		func(devices map[int][]string) {
			c.stateMu.RLock()
			c.limiter.UpdateGlobalDevices(devices)
			c.stateMu.RUnlock()
		},
		func(connected bool) {
			if connected {
				// WS 重连成功后强制重发一次设备快照：面板在 onClose 时会清空
				// 该节点的设备记录，若仍沿用断线前的 hash 将永不重发，
				// 导致全局限数丢失该节点的在线设备。
				c.deviceReportMu.Lock()
				c.lastReportDevicesHash = ""
				c.deviceReportMu.Unlock()
			}
			log.WithFields(log.Fields{"tag": c.tag, "connected": connected}).
				Info("Panel WebSocket status changed")
		},
		func(userIDs []int) {
			c.removeUsersByIDs(userIDs)
		},
		// sync.user.delta add：新用户/恢复用户秒级生效，不等下一轮 REST 轮询。
		func(users []panel.UserInfo) {
			c.addUsersByWS(users)
		},
		// 面板转发其他节点的剔除请求：断开本节点该用户该 IP 的全部连接。
		func(email string, ip string) {
			limiter.KickLocal(email, ip)
		},
	)
	// 注册跨节点剔除上报：设备满员随机剔除 victim 时，向面板上报，
	// 由面板转发给该 IP 在线的其他节点完成跨节点剔除（本地断链已由
	// dispatcher 的 kickLocal 回调直接完成）。
	limiter.RegisterKickReporter(func(uid int, email string, ip string) {
		c.wsClient.SendKickReport(uid, email, ip)
	})
	log.WithField("tag", c.tag).Infof("Panel WebSocket enabled, connecting %s", hs.WebSocket.WSURL)
	go c.wsClient.Run(ctx)
	// 周期上报本地设备快照：面板聚合后回推 sync.devices（10s 级收敛）。
	// 与 REST alive 上报（UserOnlineIP 快照）并存，两者用途不同互不冲突。
	c.deviceReportPeriodic = &task.Task{
		Name:     "reportDevices",
		Interval: 10 * time.Second,
		Execute:  c.reportDevicesTask,
	}
	_ = c.deviceReportPeriodic.Start(true)
}

// reportDevicesTask 通过 WS 上报本节点活跃设备快照（连接级 refCount）。
// 面板收到 report.devices 后聚合写 Redis 并周期回推 sync.devices，
// 节点据此更新全局设备表。WS 未连接时跳过（REST alive 上报作兜底）。
// 设备集合无变化时不重复上报（对齐官方 sha256 去重），避免面板每 10s
// 全量重写 Redis 设备表造成写放大。发送失败（队列满/未连接）时不保存
// hash，下一轮无条件重试，保证该次设备变化不因队列丢弃而永久丢失。
func (c *Controller) reportDevicesTask() error {
	if c.wsClient == nil || !c.wsClient.IsConnected() {
		return nil
	}

	c.deviceReportMu.Lock()
	prevHash := c.lastReportDevicesHash
	c.deviceReportMu.Unlock()

	// 无连接变化（设备快照未更新）且已有成功上报基线时跳过，
	// 省去每 10s 的全量快照遍历与 sha256 计算（万级设备下的 CPU 节省）。
	// prevHash==""（从未上报或 WS 重连清空后）时无条件执行，保证强制重发。
	// c.limiter 指针可能被 nodeInfoMonitor 重建（tag 变化）整体替换，
	// 所有字段访问须持 stateMu，避免 data race。
	c.stateMu.RLock()
	dirty := c.limiter.DevicesDirty()
	c.stateMu.RUnlock()
	if !dirty && prevHash != "" {
		return nil
	}

	c.stateMu.RLock()
	devices := c.limiter.LocalDeviceSnapshot()
	c.stateMu.RUnlock()

	if len(devices) == 0 {
		// 设备从有到无：补发一次空报告，让面板清除本节点设备记录。
		// 否则面板保留断线前的旧记录直到 TTL(300s) 过期，期间全局设备表
		// 虚高，其他节点会误判设备数已满而拒绝新连接，面板 online_count 也残留。
		// 补发失败（队列满）不重置 hash 与脏标记，下一轮继续重试，保证最终清空；
		// 成功或无需补发（从未上报）则复位脏标记，避免空表轮询空转。
		if prevHash != "" && !c.wsClient.SendDeviceReport(make(map[int][]string)) {
			return nil
		}
		if prevHash != "" {
			c.deviceReportMu.Lock()
			c.lastReportDevicesHash = ""
			c.deviceReportMu.Unlock()
		}
		c.stateMu.RLock()
		c.limiter.ResetDevicesDirty()
		c.stateMu.RUnlock()
		return nil
	}
	hash := devicesHash(devices)
	c.deviceReportMu.Lock()
	changed := hash != c.lastReportDevicesHash
	c.deviceReportMu.Unlock()
	if !changed {
		// 快照有变化但归一化集合不变（如连接开闭交替）：已评估，复位脏标记。
		c.stateMu.RLock()
		c.limiter.ResetDevicesDirty()
		c.stateMu.RUnlock()
		return nil
	}
	if !c.wsClient.SendDeviceReport(devices) {
		return nil
	}
	c.deviceReportMu.Lock()
	c.lastReportDevicesHash = hash
	c.deviceReportMu.Unlock()
	c.stateMu.RLock()
	c.limiter.ResetDevicesDirty()
	c.stateMu.RUnlock()
	return nil
}

// removeUsersByIDs 处理面板 sync.user.delta（流量超限等主动移除通知）。
// 只从 Xray 与 limiter 中删除命中用户，不修改 userList —— 下一轮
// nodeInfoMonitor 拉取的用户列表本身不含这些用户（面板已过滤），
// compareUserList 会自然完成列表收敛，避免与本任务并发写 userList。
// 若"面板已过滤"假设失效（例如面板尚未生效时 nodeInfoMonitor 先拿到旧列表
// 重新 AddUsers），被删用户会在下一轮 nodeInfo 对比中再次被处理。
func (c *Controller) removeUsersByIDs(ids []int) {
	if len(ids) == 0 {
		return
	}
	// 先去重待删 id，避免双重循环 O(n×m)
	want := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	c.userListMu.Lock()
	var deleted []panel.UserInfo
	for _, u := range c.userList {
		if _, ok := want[u.Id]; ok {
			deleted = append(deleted, u)
		}
	}
	c.userListMu.Unlock()
	if len(deleted) == 0 {
		return
	}
	c.stateMu.RLock()
	info := c.info
	tag := c.tag
	c.stateMu.RUnlock()
	if info == nil {
		return
	}
	if err := c.server.DelUsers(deleted, tag, info); err != nil {
		log.WithFields(log.Fields{
			"tag": tag,
			"err": err,
		}).Warn("remove exceeded users failed")
		return
	}
	c.stateMu.RLock()
	c.limiter.UpdateUser(c.tag, nil, deleted, nil)
	c.stateMu.RUnlock()
	log.WithField("tag", tag).Infof("Removed %d exceeded users by sync.user.delta", len(deleted))
}

// addUsersByWS 处理面板 sync.user.delta（add）：用户新增/恢复时秒级生效，
// 不等下一轮 REST 轮询。与 nodeInfoMonitor 的 REST 增量路径等价的本地执行：
// 跳过已存在用户（防 WS 重复推送 / REST 已同步）→ Xray 添加 → limiter 更新
// → 合并进 userList，避免下轮 REST 对比将同一用户重复判定为 added。
func (c *Controller) addUsersByWS(users []panel.UserInfo) {
	if len(users) == 0 {
		return
	}
	// 过滤：跳过无 uuid（Xray user 依赖 uuid）与已在 userList 中的用户。
	c.userListMu.Lock()
	existing := make(map[int]struct{}, len(c.userList))
	for _, u := range c.userList {
		existing[u.Id] = struct{}{}
	}
	fresh := make([]panel.UserInfo, 0, len(users))
	for _, u := range users {
		if u.Uuid == "" {
			continue
		}
		if _, ok := existing[u.Id]; !ok {
			fresh = append(fresh, u)
		}
	}
	// COW 追加：在副本上合并后整体替换引用，绝不原地 append。
	// nodeInfoMonitor 的 AddUsers / 日志对 c.userList 为无锁裸读（重操作不宜持锁），
	// 原地 append 会并发写底层数组与其构成 data race；COW 后裸读方始终拿到
	// 某个一致的旧/新列表（底层数组永不被写），仅可能在极端时序下略滞后一帧，
	// 下一轮 REST compare 会自然收敛，无正确性影响。
	if len(fresh) > 0 {
		merged := make([]panel.UserInfo, 0, len(c.userList)+len(fresh))
		merged = append(merged, c.userList...)
		merged = append(merged, fresh...)
		c.userList = merged
	}
	c.userListMu.Unlock()
	if len(fresh) == 0 {
		return
	}
	c.stateMu.RLock()
	info := c.info
	tag := c.tag
	c.stateMu.RUnlock()
	if info == nil {
		return
	}
	if _, err := c.server.AddUsers(&vCore.AddUsersParams{
		Tag:      tag,
		NodeInfo: info,
		Users:    fresh,
	}); err != nil {
		log.WithFields(log.Fields{
			"tag": tag,
			"err": err,
		}).Warn("add users by sync.user.delta failed")
		return
	}
	c.stateMu.RLock()
	c.limiter.UpdateUser(tag, fresh, nil, nil)
	c.stateMu.RUnlock()
	log.WithField("tag", tag).Infof("Added %d users by sync.user.delta", len(fresh))
}

// devicesHash 计算设备快照的确定性哈希（uid+ip 排序拼接后 sha256）。
// LocalDeviceSnapshot 的 map 遍历顺序随机，必须先排序才能得到稳定哈希。
func devicesHash(devices map[int][]string) string {
	var sb strings.Builder
	uids := make([]int, 0, len(devices))
	for uid := range devices {
		uids = append(uids, uid)
	}
	sort.Ints(uids)
	for _, uid := range uids {
		ips := devices[uid]
		sort.Strings(ips)
		sb.WriteString(strconv.Itoa(uid))
		sb.WriteByte(':')
		for _, ip := range ips {
			sb.WriteString(ip)
			sb.WriteByte(',')
		}
		sb.WriteByte(';')
	}
	h := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(h[:])
}

func (c *Controller) buildNodeTag(node *panel.NodeInfo) string {
	return fmt.Sprintf("[%s]-%s:%d", c.apiClient.APIHost, node.Type, node.Id)
}
