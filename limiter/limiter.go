package limiter

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/csdfsdffese/ljfxznode/api/panel"
	"github.com/csdfsdffese/ljfxznode/common/format"
	"github.com/csdfsdffese/ljfxznode/conf"
	"github.com/juju/ratelimit"
)

// globalDevicesStaleAfter 是全局设备表的新鲜度窗口。
// 超过该窗口未收到面板 WS 推送则回退到 alivelist 轮询兜底。
const globalDevicesStaleAfter = 60 * time.Second

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	DomainRules   []*regexp.Regexp
	ProtocolRules []string
	SpeedLimit    int
	UserLimitInfo *sync.Map // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map // key: TagUUID, value: *ratelimit.Bucket
	// AliveList is guarded by aliveLock because nodeInfoMonitor replaces the
	// whole map while UpdateUser/CheckLimit read/modify it concurrently.
	aliveLock sync.RWMutex
	AliveList map[int]int

	// refCount 是连接级活跃计数（uid → ip → 活跃连接数）。
	// 连接建立时 +1（CheckLimit 放行后），连接关闭时 -1（dispatcher 回调）。
	// 它是 REST alive（GetOnlineDevice）与 WS report.devices（LocalDeviceSnapshot）
	// 的唯一数据源，保证两通道写面板 Redis 设备表的口径一致，避免计数抖动。
	refLock  sync.RWMutex
	refCount map[int]map[string]int

	// globalDevices 是面板 WS 推送的全局设备表（uid → 在线 IP 集合），
	// 跨所有节点去重，用于全局限数判断；由 globalLock 保护。
	globalLock       sync.RWMutex
	globalDevices    map[int]map[string]bool
	globalLastUpdate time.Time
}

type UserLimitInfo struct {
	mu          sync.Mutex
	UID         int
	SpeedLimit  int
	DeviceLimit int
	OverLimit   bool
}

func AddLimiter(tag string, l *conf.LimitConfig, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	info := &Limiter{
		SpeedLimit:    l.SpeedLimit,
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveList,
		refCount:      make(map[int]map[string]int),
		globalDevices: make(map[int]map[string]bool),
	}
	for i := range users {
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		userLimit.OverLimit = false
		info.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	limitLock.Lock()
	limiter[tag] = info
	limitLock.Unlock()
	return info
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

func (l *Limiter) SetAliveList(m map[int]int) {
	l.aliveLock.Lock()
	l.AliveList = m
	l.aliveLock.Unlock()
}

func (l *Limiter) GetAliveCount(uid int) int {
	l.aliveLock.RLock()
	n := l.AliveList[uid]
	l.aliveLock.RUnlock()
	return n
}

func (l *Limiter) DelAlive(uid int) {
	l.aliveLock.Lock()
	delete(l.AliveList, uid)
	l.aliveLock.Unlock()
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo, modified []panel.UserInfo) {
	for i := range deleted {
		l.UserLimitInfo.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.SpeedLimiter.Delete(format.UserTag(tag, deleted[i].Uuid))
		l.DelAlive(deleted[i].Id)
		l.refLock.Lock()
		delete(l.refCount, deleted[i].Id)
		l.refLock.Unlock()
		l.globalLock.Lock()
		delete(l.globalDevices, deleted[i].Id)
		l.globalLock.Unlock()
	}
	for i := range modified {
		// Update the user limit info with the new speed/device limits.
		if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, modified[i].Uuid)); ok {
			u := v.(*UserLimitInfo)
			u.mu.Lock()
			u.SpeedLimit = modified[i].SpeedLimit
			u.DeviceLimit = modified[i].DeviceLimit
			u.mu.Unlock()
			l.UserLimitInfo.Store(format.UserTag(tag, modified[i].Uuid), u)
		}
		// Drop the cached bucket so CheckLimit rebuilds it at the new rate.
		l.SpeedLimiter.Delete(format.UserTag(tag, modified[i].Uuid))
	}
	for i := range added {
		userLimit := &UserLimitInfo{
			UID: added[i].Id,
		}
		if added[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = added[i].SpeedLimit
		}
		if added[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = added[i].DeviceLimit
		}
		userLimit.OverLimit = false
		l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
	}
}

func (l *Limiter) CheckLimit(taguuid string, ip string, noSSUDP bool) (Bucket *ratelimit.Bucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	if nodeLimit < 0 {
		// defensive: a negative node speed limit from config must not create
		// a broken token bucket
		nodeLimit = 0
	}
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		u.mu.Lock()
		deviceLimit = u.DeviceLimit
		uid = u.UID
		userLimit = u.SpeedLimit
		u.mu.Unlock()
	} else {
		return nil, true
	}
	if userLimit < 0 {
		userLimit = 0
	}
	if deviceLimit > 0 {
		if l.checkDeviceGate(uid, ip, deviceLimit) {
			return nil, true
		}
	}
	if noSSUDP {
		// 仅 TCP 连接计数（对齐官方 Xboard-Node：UDP 会话只裁决、不占用设备数）。
		// 放行：连接级计数 +1，连接关闭时由 dispatcher 回调 ConnClosed 递减
		l.ConnOpened(taguuid, uid, ip)
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	if limit > 0 {
		Bucket = ratelimit.NewBucketWithQuantum(time.Second, limit, limit) // Byte/s
		if v, ok := l.SpeedLimiter.LoadOrStore(taguuid, Bucket); ok {
			return v.(*ratelimit.Bucket), false
		}
		return Bucket, false
	} else {
		return nil, false
	}
}

// GetOnlineDevice 返回本节点当前活跃设备快照（连接级 refCount）。
// 与 LocalDeviceSnapshot（WS report.devices）同源，保证 REST alive 与 WS
// 两通道写面板 Redis 设备表的口径一致，避免设备计数周期抖动。
func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	l.refLock.RLock()
	defer l.refLock.RUnlock()
	var onlineUser []panel.OnlineUser
	for uid, m := range l.refCount {
		for ip := range m {
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
		}
	}
	return &onlineUser, nil
}

// UpdateGlobalDevices 以面板 WS 推送的全量设备表覆盖本地全局表，并刷新新鲜度。
// users 为 userID → 在线 IP 列表（面板已按纯 IP 跨节点去重）。
func (l *Limiter) UpdateGlobalDevices(users map[int][]string) {
	if users == nil {
		users = make(map[int][]string)
	}
	devices := make(map[int]map[string]bool, len(users))
	for uid, ips := range users {
		m := make(map[string]bool, len(ips))
		for _, ip := range ips {
			m[strings.TrimPrefix(ip, "::ffff:")] = true
		}
		devices[uid] = m
	}
	l.globalLock.Lock()
	l.globalDevices = devices
	l.globalLastUpdate = time.Now()
	l.globalLock.Unlock()
}

// LocalDeviceSnapshot 返回本节点当前活跃设备快照（userID → IP 列表，连接级）。
// 供 WS report.devices 周期上报；只读，不清理任何状态。
func (l *Limiter) LocalDeviceSnapshot() map[int][]string {
	l.refLock.RLock()
	out := make(map[int][]string, len(l.refCount))
	for uid, m := range l.refCount {
		ips := make([]string, 0, len(m))
		for ip := range m {
			ips = append(ips, ip)
		}
		out[uid] = ips
	}
	l.refLock.RUnlock()
	return out
}

// ConnOpened 记录一条 TCP 连接建立（放行后调用），连接级计数 +1。
func (l *Limiter) ConnOpened(taguuid string, uid int, ip string) {
	ip = strings.TrimPrefix(ip, "::ffff:")
	l.refLock.Lock()
	m := l.refCount[uid]
	if m == nil {
		m = make(map[string]int)
		l.refCount[uid] = m
	}
	m[ip]++
	l.refLock.Unlock()
}

// ConnClosed 记录一条 TCP 连接关闭（dispatcher 连接结束时回调），连接级计数 -1。
func (l *Limiter) ConnClosed(taguuid string, ip string) {
	ip = strings.TrimPrefix(ip, "::ffff:")
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return
	}
	uid := v.(*UserLimitInfo).UID
	l.refLock.Lock()
	if m := l.refCount[uid]; m != nil {
		m[ip]--
		if m[ip] <= 0 {
			delete(m, ip)
		}
		if len(m) == 0 {
			delete(l.refCount, uid)
		}
	}
	l.refLock.Unlock()
}

// localRefIPs 返回该用户当前在本节点的活跃 IP 集合（连接级计数）。
func (l *Limiter) localRefIPs(uid int) map[string]bool {
	l.refLock.RLock()
	m := l.refCount[uid]
	out := make(map[string]bool, len(m))
	for ip := range m {
		out[ip] = true
	}
	l.refLock.RUnlock()
	return out
}

// checkDeviceGate 判断新连接（新 IP）是否应被拒绝。
// 对齐官方 Xboard-Node conntracker：全局设备表新鲜时一律 merge 全量裁决；
// 全局表过期/缺失时才走本地判断，并以 REST alivelist 兜底（断线补偿）。
func (l *Limiter) checkDeviceGate(uid int, ip string, limit int) bool {
	if limit <= 0 {
		return false
	}
	local := l.localRefIPs(uid)
	// 本节点已有该 IP 的活跃连接 → 同 IP 复连，放行
	if local[ip] {
		return false
	}

	// 全局设备表（WS sync.devices）
	l.globalLock.RLock()
	stale := time.Since(l.globalLastUpdate) > globalDevicesStaleAfter
	globalIPs := l.globalDevices[uid]
	l.globalLock.RUnlock()
	if !stale {
		// 全局表新鲜：一律以 WS 数据为准（官方主路径），即使该用户无全局条目
		// 或面板刚推过空表也不回退 alivelist，避免与 REST 旧计数互相矛盾。
		if globalIPs == nil {
			globalIPs = make(map[string]bool)
		}
		// 其他节点已知该 IP → 视为同一设备，放行
		if globalIPs[ip] {
			return false
		}
		// merge 本节点 + 全局后全量裁决（官方主路径，即使本地已满也走全局）
		merged := make(map[string]bool, len(local)+len(globalIPs))
		for k := range local {
			merged[k] = true
		}
		for k := range globalIPs {
			merged[k] = true
		}
		if len(merged) < limit {
			return false
		}
		merged[ip] = true
		return !l.ipInTopN(merged, ip, limit)
	}

	// 全局表过期或缺失（WS 断线中）：本地判断；未满时以 alivelist 兜底
	// （面板 REST 全局数据）。alivelist 仅有计数无 IP 明细，无法判断新 IP
	// 是否已在其他节点在线，此处保守拒绝（断线补偿的固有代价）。
	if len(local) < limit {
		if alive := l.GetAliveCount(uid); alive >= limit {
			return true
		}
		return false
	}
	return !l.ipInTopN(local, ip, limit)
}

// ipInTopN 判断 ip 是否位于集合（含新 ip）按字典序排序后的前 limit 个。
func (l *Limiter) ipInTopN(ipSet map[string]bool, ip string, limit int) bool {
	list := make([]string, 0, len(ipSet)+1)
	for k := range ipSet {
		list = append(list, k)
	}
	list = append(list, ip)
	sort.Strings(list)
	for i := 0; i < limit && i < len(list); i++ {
		if list[i] == ip {
			return true
		}
	}
	return false
}

// determineSpeedLimit returns the minimum non-zero rate
func determineSpeedLimit(limit1, limit2 int) (limit int) {
	if limit1 == 0 || limit2 == 0 {
		if limit1 > limit2 {
			return limit1
		} else if limit1 < limit2 {
			return limit2
		} else {
			return 0
		}
	} else {
		if limit1 > limit2 {
			return limit2
		} else if limit1 < limit2 {
			return limit1
		} else {
			return limit1
		}
	}
}
