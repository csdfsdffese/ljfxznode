package limiter

import (
	"errors"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/csdfsdffese/ljfxznode/api/panel"
	"github.com/csdfsdffese/ljfxznode/common/format"
	"github.com/csdfsdffese/ljfxznode/conf"
	"github.com/juju/ratelimit"
)

// globalDevicesStaleAfter 是全局设备表的新鲜度窗口。
// 超过该窗口未收到面板 WS 推送则回退到 alivelist 轮询兜底。
const globalDevicesStaleAfter = 60 * time.Second

// gateShards 是设备门禁分片锁的数量（须为 2 的幂，按 uid 取模）。
// TryOpenConn 只需保证「同一用户的裁决+计数」原子——不同用户互不影响，
// 因此用分片锁替代全局锁，避免全节点 TCP 建连被单一互斥锁串行化（高并发瓶颈）。
const gateShards = 256

// refShards 是连接级活跃计数（refCount）的分片锁数量（须为 2 的幂，按 uid 取模）。
// ConnOpened/ConnClosed 每连接一次计数写操作，是高并发下的热点；
// 分片后不同 uid 的建连/断连计数互不阻塞（替代旧版全局 refLock 串行化）。
const refShards = 256

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

// refShard 是 refCount 的一个分片：mu 保护该分片内所有 uid 的计数 map。
type refShard struct {
	mu sync.RWMutex
	m  map[int]map[string]int
}

type Limiter struct {
	DomainRules   []*regexp.Regexp
	ProtocolRules []string
	SpeedLimit    int
	UserLimitInfo *sync.Map // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map // key: TagUUID, value: *ratelimit.Bucket
	// ruleLock 保护 DomainRules/ProtocolRules：
	// UpdateRule 整表替换（写锁），CheckDomainRule/CheckProtocolRule 并发读（读锁）。
	ruleLock sync.RWMutex
	// AliveList is guarded by aliveLock because nodeInfoMonitor replaces the
	// whole map while UpdateUser/CheckLimit read/modify it concurrently.
	aliveLock sync.RWMutex
	AliveList map[int]int

	// refShards 是连接级活跃计数（uid → ip → 活跃连接数）的分片存储。
	// 连接建立时 +1（CheckLimit 放行后），连接关闭时 -1（dispatcher 回调）。
	// 它是 REST alive（GetOnlineDevice）与 WS report.devices（LocalDeviceSnapshot）
	// 的唯一数据源，保证两通道写面板 Redis 设备表的口径一致，避免计数抖动。
	refShards [refShards]refShard
	// deviceDirty 标记设备快照自上次评估以来是否变化（ConnOpened/ConnClosed 置位）。
	// WS 上报任务据此跳过无变化轮次的「全量遍历 refShards + sha256」，省 CPU。
	deviceDirty atomic.Bool

	// gateLocks 是设备门禁的分片锁（按 uid 取模）。
	// TryOpenConn 在「同用户」分片上完成「裁决+计数」原子化，
	// 不同用户并行建连互不阻塞（替代旧版的全局 openGateMu 串行化）。
	gateLocks [gateShards]sync.Mutex

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
}

func AddLimiter(tag string, l *conf.LimitConfig, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	// 拷贝 aliveList：调用方（nodeInfoMonitor）的 map 会整体替换，且 AddLimiter
	// 之后 limiter 内会原地增删（DelAlive），避免引用外部可变 map。
	aliveCopy := make(map[int]int, len(aliveList))
	for k, v := range aliveList {
		aliveCopy[k] = v
	}
	info := &Limiter{
		SpeedLimit:    l.SpeedLimit,
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
		AliveList:     aliveCopy,
		globalDevices: make(map[int]map[string]bool),
	}
	for i := range info.refShards {
		info.refShards[i].m = make(map[int]map[string]int)
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

// GetAliveList 返回当前 alivelist 的拷贝。
// 供 limiter 整体重建（tag 变化）时保留断线兜底数据，避免新 limiter 以空表启动。
func (l *Limiter) GetAliveList() map[int]int {
	l.aliveLock.RLock()
	out := make(map[int]int, len(l.AliveList))
	for k, v := range l.AliveList {
		out[k] = v
	}
	l.aliveLock.RUnlock()
	return out
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
		sh := &l.refShards[uint(deleted[i].Id)&(refShards-1)]
		sh.mu.Lock()
		delete(sh.m, deleted[i].Id)
		sh.mu.Unlock()
		l.globalLock.Lock()
		delete(l.globalDevices, deleted[i].Id)
		l.globalLock.Unlock()
		l.deviceDirty.Store(true)
	}
	for i := range modified {
		// Update the user limit info with the new speed/device limits.
		if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, modified[i].Uuid)); ok {
			u := v.(*UserLimitInfo)
			u.mu.Lock()
			u.SpeedLimit = modified[i].SpeedLimit
			u.DeviceLimit = modified[i].DeviceLimit
			u.mu.Unlock()
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
	if noSSUDP {
		// 仅 TCP 连接计数（对齐官方 Xboard-Node：UDP 会话只裁决、不占用设备数）。
		// 裁决 + 计数原子化（TryOpenConn），避免并发新连接同时通过门禁导致超限；
		// 连接关闭时由 dispatcher 回调 ConnClosed 递减。
		if !l.TryOpenConn(uid, ip, deviceLimit) {
			return nil, true
		}
	} else if deviceLimit > 0 {
		// UDP：只裁决不计数（只读无副作用、无竞态）。checkDeviceGate 内部
		// 优先全局表（WS sync.devices），过期时回退本地 + alivelist 兜底。
		if l.checkDeviceGate(uid, ip, deviceLimit) {
			return nil, true
		}
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
func (l *Limiter) GetOnlineDevice() *[]panel.OnlineUser {
	var onlineUser []panel.OnlineUser
	for i := range l.refShards {
		sh := &l.refShards[i]
		sh.mu.RLock()
		for uid, m := range sh.m {
			for ip := range m {
				onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			}
		}
		sh.mu.RUnlock()
	}
	return &onlineUser
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
	out := make(map[int][]string)
	for i := range l.refShards {
		sh := &l.refShards[i]
		sh.mu.RLock()
		for uid, m := range sh.m {
			ips := make([]string, 0, len(m))
			for ip := range m {
				ips = append(ips, ip)
			}
			out[uid] = ips
		}
		sh.mu.RUnlock()
	}
	return out
}

// DevicesDirty 报告设备快照自上次评估以来是否发生变化。
func (l *Limiter) DevicesDirty() bool { return l.deviceDirty.Load() }

// ResetDevicesDirty 复位脏标记。WS 上报任务在成功评估（发送或确认无变化）后调用；
// 发送失败时不复位，保证下一轮重试不丢失设备变化。
func (l *Limiter) ResetDevicesDirty() { l.deviceDirty.Store(false) }

// TryOpenConn 原子化执行「设备门禁裁决 + 连接计数」。
// 无限制用户（deviceLimit<=0）走快速路径直接计数，不占用任何门禁锁；
// 有限制用户以 uid 分片锁保证「同用户裁决+计数」原子（不同用户并行），
// 避免并发新连接在彼此计数前同时通过裁决而超出 deviceLimit。
// 返回 false 表示被门禁拒绝（不计数）；true 表示放行并已计数。
func (l *Limiter) TryOpenConn(uid int, ip string, deviceLimit int) bool {
	ip = strings.TrimPrefix(ip, "::ffff:")
	if deviceLimit <= 0 {
		l.ConnOpened(uid, ip)
		return true
	}
	lk := l.gateLock(uid)
	lk.Lock()
	defer lk.Unlock()
	if l.checkDeviceGate(uid, ip, deviceLimit) {
		return false
	}
	l.ConnOpened(uid, ip)
	return true
}

// gateLock 返回 uid 对应的分片锁（取模 2 的幂，位运算取余）。
func (l *Limiter) gateLock(uid int) *sync.Mutex {
	return &l.gateLocks[uint(uid)&(gateShards-1)]
}

// ConnOpened 记录一条 TCP 连接建立（放行后调用），连接级计数 +1。
func (l *Limiter) ConnOpened(uid int, ip string) {
	ip = strings.TrimPrefix(ip, "::ffff:")
	sh := &l.refShards[uint(uid)&(refShards-1)]
	sh.mu.Lock()
	m := sh.m[uid]
	if m == nil {
		m = make(map[string]int)
		sh.m[uid] = m
	}
	m[ip]++
	sh.mu.Unlock()
	l.deviceDirty.Store(true)
}

// ConnClosed 记录一条 TCP 连接关闭（dispatcher 连接结束时回调），连接级计数 -1。
func (l *Limiter) ConnClosed(taguuid string, ip string) {
	ip = strings.TrimPrefix(ip, "::ffff:")
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		return
	}
	uid := v.(*UserLimitInfo).UID
	sh := &l.refShards[uint(uid)&(refShards-1)]
	sh.mu.Lock()
	if m := sh.m[uid]; m != nil {
		m[ip]--
		if m[ip] <= 0 {
			delete(m, ip)
		}
		if len(m) == 0 {
			delete(sh.m, uid)
		}
	}
	sh.mu.Unlock()
	l.deviceDirty.Store(true)
}

// checkDeviceGate 判断新连接（新 IP）是否应被拒绝。
// 对齐官方 Xboard-Node conntracker：全局设备表新鲜时一律 merge 全量裁决；
// 全局表过期/缺失时才走本地判断，并以 REST alivelist 兜底（断线补偿）。
// 全程无 map 分配：以线性扫描统计替代旧版的 merged 合并 + 字典序排序。
func (l *Limiter) checkDeviceGate(uid int, ip string, limit int) bool {
	if limit <= 0 {
		return false
	}
	l.globalLock.RLock()
	stale := time.Since(l.globalLastUpdate) > globalDevicesStaleAfter
	// globalIPs 引用由 UpdateGlobalDevices 整体替换、从不原地修改，
	// 释放锁后仍可安全遍历（持有的始终是不可变的旧表）。
	globalIPs := l.globalDevices[uid]
	l.globalLock.RUnlock()

	sh := &l.refShards[uint(uid)&(refShards-1)]
	sh.mu.RLock()
	local := sh.m[uid]
	// 本节点已有该 IP 的活跃连接 → 同 IP 复连，放行
	if local[ip] > 0 {
		sh.mu.RUnlock()
		return false
	}
	if !stale {
		// 全局表新鲜：一律以 WS 数据为准（官方主路径），即使该用户无全局条目
		// 或面板刚推过空表也不回退 alivelist，避免与 REST 旧计数互相矛盾。
		if globalIPs[ip] {
			sh.mu.RUnlock()
			return false // 其他节点已知该 IP → 视为同一设备，放行
		}
		// merge 本节点 + 全局后全量裁决：拒绝 ⟺ 两表并集（去重）中字典序
		// 小于 ip 的条目数 ≥ limit（等价于官方排序后 ip 不在前 limit 位）。
		reject := countLessThan(local, globalIPs, ip, limit)
		sh.mu.RUnlock()
		return reject
	}
	// 全局表过期或缺失（WS 断线中）：本地判断；未满时以 alivelist 兜底
	// （面板 REST 全局数据）。alivelist 仅有计数无 IP 明细，无法判断新 IP
	// 是否已在其他节点在线，此处保守拒绝（断线补偿的固有代价）。
	if len(local) < limit {
		sh.mu.RUnlock()
		if alive := l.GetAliveCount(uid); alive >= limit {
			return true
		}
		return false
	}
	reject := countLessThan(local, nil, ip, limit)
	sh.mu.RUnlock()
	return reject
}

// countLessThan 统计 local∪global（去重）中字典序小于 ip 的条目数是否 ≥ limit。
// local 为 refShards 分片值类型（map[string]int，连接数），global 为设备集合（map[string]bool）；
// 无分配：先扫 local，再扫 global 并跳过 local 中已计过的键，达到 limit 提前停止。
// 调用方须持有对应 refShard 的读锁以保证 local 稳定。
func countLessThan(local map[string]int, global map[string]bool, ip string, limit int) bool {
	less := 0
	for k := range local {
		if k < ip {
			less++
			if less >= limit {
				return true
			}
		}
	}
	for k := range global {
		if k < ip {
			// 显式存在性去重：local 中已计过的键（无论连接数）不再重复计数。
			// 不用 local[k]==0 判断，避免未来计数归零残留键时双重计数导致误拒。
			if _, dup := local[k]; dup {
				continue
			}
			less++
			if less >= limit {
				return true
			}
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
