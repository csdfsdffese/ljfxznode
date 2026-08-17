package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	aliveMap                map[int]int
	pendingTraffic          []panel.UserTraffic
	info                    *panel.NodeInfo
	nodeInfoMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	statusReportPeriodic    *task.Task
	renewCertPeriodic       *task.Task
	onlineIpReportPeriodic  *task.Task
	deviceReportPeriodic    *task.Task
	statusChecker           *statusChecker
	wsClient                *panel.WSClient
	wsCancel                context.CancelFunc
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
	c.aliveMap, err = c.apiClient.GetUserAlive()
	if err != nil {
		return fmt.Errorf("failed to get user alive list: %s", err)
	}
	if len(c.Options.Name) == 0 {
		c.tag = c.buildNodeTag(node)
	} else {
		c.tag = c.Options.Name
	}

	// add limiter
	l := limiter.AddLimiter(c.tag, &c.LimitConfig, c.userList, c.aliveMap)
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
	if c.onlineIpReportPeriodic != nil {
		c.onlineIpReportPeriodic.Close()
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
	if err := c.apiClient.ReportNodeStatus(status); err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Debug("Report node status failed")
	}
	return nil
}

// startWS 探测面板 WS 服务并启动设备状态推送客户端。
// WS 用于下行全局设备表（sync.devices）实时更新全局限数；
// 面板未启用 WS 或探测失败时静默回退到 alivelist 轮询（不影响节点可用性）。
func (c *Controller) startWS() {
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
			c.limiter.UpdateGlobalDevices(devices)
		},
		func(connected bool) {
			log.WithFields(log.Fields{"tag": c.tag, "connected": connected}).
				Info("Panel WebSocket status changed")
		},
	)
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
// 全量重写 Redis 设备表造成写放大。
func (c *Controller) reportDevicesTask() error {
	if c.wsClient == nil || !c.wsClient.IsConnected() {
		return nil
	}
	devices := c.limiter.LocalDeviceSnapshot()
	if len(devices) == 0 {
		c.lastReportDevicesHash = ""
		return nil
	}
	hash := devicesHash(devices)
	if hash == c.lastReportDevicesHash {
		return nil
	}
	c.lastReportDevicesHash = hash
	c.wsClient.SendDeviceReport(devices)
	return nil
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
