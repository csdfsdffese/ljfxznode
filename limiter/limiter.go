package limiter

import (
	"errors"
	"maps"
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

// kickLocal 由 dispatcher 注册：断开本进程内指定用户（taguuid）指定 IP 的所有连接。
// 剔除动作不依赖 WS，任何时刻都可用。
var kickLocal atomic.Pointer[func(taguuid string, ip string)]

// kickReporter 由 node.Controller 注册：通过 WS 向面板上报剔除请求，
// 由面板转发给该 IP 在线的其他节点，实现跨节点剔除。
var kickReporter atomic.Pointer[func(uid int, taguuid string, ip string)]

// RegisterKickLocal 注册本地断连执行器（dispatcher 在初始化时调用）。
func RegisterKickLocal(fn func(taguuid string, ip string)) {
	if fn != nil {
		kickLocal.Store(&fn)
	}
}

// RegisterKickReporter 注册跨节点剔除上报执行器（node.Controller 调用）。
func RegisterKickReporter(fn func(uid int, taguuid string, ip string)) {
	if fn != nil {
		kickReporter.Store(&fn)
	}
}

// KickLocal 断开本进程内指定用户（taguuid）指定 IP 的所有连接。
// 供 controller 处理面板 sync.kick 事件时调用，与满员剔除（kickDevice）
// 共用 dispatcher 注册的同一断链执行器。
func KickLocal(taguuid string, ip string) {
	if fn := kickLocal.Load(); fn != nil {
		(*fn)(taguuid, ip)
	}
}

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

	// kickThrottle 记录各分片最近一次剔除时间（纳秒），下标与 gateLocks 一致，
	// 在对应 gateLock 持有下读写（同分片互斥，无 data race）。
	// 超限风暴时合并剔除频率：每分片每秒至多触发一次随机剔除，
	// 避免并发新连接各自踢一个 victim 造成「剔除风暴」同时断开多个
	// 在线用户（一次剔除腾出的名额远比单个新连接大，节流不损收敛速度）。
	kickThrottle [gateShards]int64

	// globalDevices 是面板 WS 推送的全局设备表（uid → 在线 IP 集合），
	// 跨所有节点去重，用于全局限数判断；由 globalLock 保护。
	// globalCount 是同一快照的 per-uid 去重 IP 数（UpdateGlobalDevices 时
	// 一次性算出），供 checkDeviceGate 的 O(1) 快速上/下界判断，避免每个
	// 新连接都遍历全局表。
	globalLock       sync.RWMutex
	globalDevices    map[int]map[string]bool
	globalCount      map[int]int
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
		globalCount:   make(map[int]int),
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
	maps.Copy(out, l.AliveList)
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
		delete(l.globalCount, deleted[i].Id)
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
	// 防御：面板下发负限速值不得产生损坏的令牌桶
	nodeLimit := max(l.SpeedLimit, 0)
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
	userLimit = max(userLimit, 0)
	if noSSUDP {
		// 仅 TCP 连接计数（对齐官方 Xboard-Node：UDP 会话只裁决、不占用设备数）。
		// 裁决 + 计数原子化（TryOpenConn），避免并发新连接同时通过门禁导致超限；
		// 连接关闭时由 dispatcher 回调 ConnClosed 递减。
		// 满员时 TryOpenConn 会拒绝并随机剔除一个已在线 IP（断其连接）腾位。
		if !l.TryOpenConn(uid, taguuid, ip, deviceLimit) {
			return nil, true
		}
	} else if deviceLimit > 0 {
		// UDP：只裁决不计数、不触发剔除（只读无副作用、无竞态）。
		// checkDeviceGate 内部优先全局表（WS sync.devices），过期时回退本地 + alivelist 兜底。
		if reject, _ := l.checkDeviceGate(uid, ip, deviceLimit); reject {
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
	// 同步缓存 per-uid 去重 IP 数：供 checkDeviceGate 快速上/下界判断。
	// 与 globalDevices 同一快照、同一锁，保证二者永远一致。
	globalCount := make(map[int]int, len(devices))
	for uid, m := range devices {
		globalCount[uid] = len(m)
	}
	l.globalCount = globalCount
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
// 满员时（新 IP 且在线设备数已到上限）拒绝连接，并异步随机剔除一个
// 已在线 IP（断开其所有连接），为后续新设备腾出接入名额。
// 返回 false 表示被门禁拒绝（不计数）；true 表示放行并已计数。
// taguuid 为 format.UserTag(tag, uuid)，与 Xray user.Email 及 LinkManagers 的 key 一致，
// 剔除回调据此在 dispatcher 中定位该用户的连接表。
func (l *Limiter) TryOpenConn(uid int, taguuid string, ip string, deviceLimit int) bool {
	ip = strings.TrimPrefix(ip, "::ffff:")
	if deviceLimit <= 0 {
		l.ConnOpened(uid, ip)
		return true
	}
	lk := l.gateLock(uid)
	lk.Lock()
	defer lk.Unlock()
	reject, victim := l.checkDeviceGate(uid, ip, deviceLimit)
	if reject {
		if victim != "" {
			// 剔除节流：同分片每秒至多剔除一次（gateLock 持有时读写，原子）。
			// 超限风暴时多个新连接并发被拒，若各自踢一个 victim 会同时断开
			// 多个在线用户；一次剔除已能断开 victim 的全部连接腾出大量名额，
			// 节流合并后收敛速度不变、误伤面显著减小。
			shard := uint(uid) & (gateShards - 1)
			if now := time.Now().UnixNano(); now-l.kickThrottle[shard] > int64(time.Second) {
				l.kickThrottle[shard] = now
				// 异步剔除：不阻塞当前连接路径。victim 选择基于已拷贝的
				// local/global 快照，goroutine 内不再读共享状态。
				go l.kickDevice(uid, taguuid, victim)
			}
		}
		return false
	}
	l.ConnOpened(uid, ip)
	return true
}

// kickDevice 执行剔除动作：本地断开 victim 的所有连接（dispatcher 回调），
// 并向面板上报剔除请求（controller 回调），由面板转发给该 IP 在线的
// 其他节点完成跨节点剔除。两者任一未注册则静默跳过对应环节。
func (l *Limiter) kickDevice(uid int, taguuid string, ip string) {
	if fn := kickLocal.Load(); fn != nil {
		(*fn)(taguuid, ip)
	}
	if fn := kickReporter.Load(); fn != nil {
		(*fn)(uid, taguuid, ip)
	}
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

// checkDeviceGate 判断新连接（新 IP）是否应被拒绝，并在拒绝时选出被剔除的
// 已在线 IP（victim，空串表示无需剔除）。
// 策略为「绝对上限」：以 local∪global 纯 IP 去重后的设备数为准，
// 达到 deviceLimit 即拒绝一切新 IP（不同于官方的字典序排序淘汰）。
// 全局设备表新鲜时（WS 正常）以全局数据为准；过期/缺失时回退本地判断，
// 并以 REST alivelist 兜底（断线补偿）。
// 被拒绝的新 IP 不占用任何名额；victim 断开后在线数回落，后续新设备可接入。
func (l *Limiter) checkDeviceGate(uid int, ip string, limit int) (reject bool, victim string) {
	if limit <= 0 {
		return false, ""
	}
	l.globalLock.RLock()
	stale := time.Since(l.globalLastUpdate) > globalDevicesStaleAfter
	// globalIPs/globalCount 引用由 UpdateGlobalDevices 整体替换、从不原地修改，
	// 释放锁后仍可安全遍历（持有的始终是不可变的旧表）。
	globalIPs := l.globalDevices[uid]
	globalCount := l.globalCount[uid]
	l.globalLock.RUnlock()

	sh := &l.refShards[uint(uid)&(refShards-1)]
	sh.mu.RLock()
	local := sh.m[uid]
	// 本节点已有该 IP 的活跃连接 → 同 IP 复连，放行
	if local[ip] > 0 {
		sh.mu.RUnlock()
		return false, ""
	}
	if !stale {
		// 全局表新鲜：一律以 WS 数据为准（官方主路径），即使该用户无全局条目
		// 或面板刚推过空表也不回退 alivelist，避免与 REST 旧计数互相矛盾。
		if globalIPs[ip] {
			sh.mu.RUnlock()
			return false, "" // 其他节点已知该 IP → 视为同一设备，放行
		}
		// 上界快速拒绝：union ≥ |local| 且 union ≥ |global|，任一达到 limit
		// 即必然超限，免于遍历全局表（满员高并发热点路径）。
		if len(local) >= limit || globalCount >= limit {
			victim = pickVictim(local, globalIPs)
			sh.mu.RUnlock()
			return true, victim
		}
		// 下界快速放行：union ≤ |local| + |global|（并集最大为两集合大小之和），
		// 之和仍小于 limit 则必然未满，免于遍历全局表（低水位常见路径）。
		if len(local)+globalCount < limit {
			sh.mu.RUnlock()
			return false, ""
		}
		// 中间区间：精确计算并集（去重）设备数（range nil map 安全）
		count := len(local)
		for gip := range globalIPs {
			if _, dup := local[gip]; !dup {
				count++
			}
		}
		// victim 必须在持有 local 读锁时选出（遍历共享 map）
		if count >= limit {
			victim = pickVictim(local, globalIPs)
		}
		sh.mu.RUnlock()
		return count >= limit, victim
	}
	// 全局表过期或缺失（WS 断线中）：本地判断；未满时以 alivelist 兜底
	// （面板 REST 全局数据）。alivelist 仅有计数无 IP 明细，无法判断新 IP
	// 是否已在其他节点在线，此处保守拒绝（断线补偿的固有代价）。
	if len(local) < limit {
		victim = pickVictim(local, nil)
		sh.mu.RUnlock()
		if alive := l.GetAliveCount(uid); alive >= limit {
			// alivelist 显示已满：拒绝，并尽力剔除一个本地 IP 腾位
			return true, victim
		}
		return false, ""
	}
	victim = pickVictim(local, nil)
	sh.mu.RUnlock()
	return true, victim
}

// pickVictim 从在线集合中随机选一个待剔除 IP：优先本节点本地在线的
// （断开立即可见、可控），否则从全局表选（由面板协调其他节点断开）。
// Go map 遍历顺序随机，天然实现「随机剔除」。返回空串表示无候选。
// 调用方须已持有相应 refShard 读锁（local 稳定）或持有不可变全局表引用。
func pickVictim(local map[string]int, global map[string]bool) string {
	for ip := range local {
		return ip
	}
	for ip := range global {
		return ip
	}
	return ""
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
