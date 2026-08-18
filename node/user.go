package node

import (
	"github.com/csdfsdffese/ljfxznode/api/panel"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) reportUserTrafficTask() (err error) {
	// tag/limiter 可能被 nodeInfoMonitor（tag 变化）整体替换，读取须持 stateMu，
	// 否则与重建 goroutine 构成 data race。GetOnlineDevice 返回的 slice 底层数组
	// 在方法内新建，锁外遍历安全。
	c.stateMu.RLock()
	tag := c.tag
	lim := c.limiter
	c.stateMu.RUnlock()

	userTraffic, _ := c.server.GetUserTrafficSlice(tag, true)
	// Merge the traffic that failed to report in the previous round so that
	// it is retried instead of being permanently lost.
	if c.pendingTraffic != nil {
		userTraffic = append(userTraffic, c.pendingTraffic...)
		c.pendingTraffic = nil
	}
	if len(userTraffic) > 0 {
		err = c.apiClient.ReportUserTraffic(userTraffic)
		if err != nil {
			c.pendingTraffic = userTraffic
			log.WithFields(log.Fields{
				"tag": tag,
				"err": err,
			}).Info("Report user traffic failed")
		} else {
			log.WithField("tag", tag).Infof("Report %d users traffic", len(userTraffic))
			log.WithField("tag", tag).Debugf("User traffic: %+v", userTraffic)
		}
	}

	// WS 正常时设备表由 report.devices（WS 通道）实时驱动并触发面板聚合，
	// REST alive 上报（ReportNodeOnlineUsers）纯冗余，还会触发面板逐用户
	// Redis 写入与 DB 更新（online_count/last_online_at），增加面板负载。
	// 仅 WS 未启用/未连接时走 REST 兜底，保证断线期间设备表不丢失。
	if c.wsClient == nil || !c.wsClient.IsConnected() {
		if onlineDevice := lim.GetOnlineDevice(); len(*onlineDevice) > 0 {
			// 仅当配置了 DeviceOnlineMinTraffic(>0) 时才过滤低流量用户，
			// 默认全部上报，避免挂机 IP 漏报导致全局设备计数偏小。
			var result []panel.OnlineUser
			var nocountUID = make(map[int]struct{})
			if c.Options.DeviceOnlineMinTraffic > 0 {
				for _, traffic := range userTraffic {
					total := traffic.Upload + traffic.Download
					if total < int64(c.Options.DeviceOnlineMinTraffic*1000) {
						nocountUID[traffic.UID] = struct{}{}
					}
				}
			}
			for _, online := range *onlineDevice {
				if _, ok := nocountUID[online.UID]; !ok {
					result = append(result, online)
				}
			}
			data := make(map[int][]string)
			for _, onlineuser := range result {
				// json structure: { UID1:["ip1","ip2"],UID2:["ip3","ip4"] }
				data[onlineuser.UID] = append(data[onlineuser.UID], onlineuser.IP)
			}
			if err = c.apiClient.ReportNodeOnlineUsers(&data); err != nil {
				log.WithFields(log.Fields{
					"tag": tag,
					"err": err,
				}).Info("Report online users failed")
			} else {
				log.WithField("tag", tag).Infof("Total %d online users, %d Reported", len(*onlineDevice), len(result))
				log.WithField("tag", tag).Debugf("Online users: %+v", data)
			}
		}
	}

	return nil
}

func compareUserList(old, new []panel.UserInfo) (deleted, added, modified []panel.UserInfo) {
	oldMap := make(map[string]panel.UserInfo, len(old))
	for _, u := range old {
		oldMap[u.Uuid] = u
	}

	for _, u := range new {
		if o, ok := oldMap[u.Uuid]; !ok {
			added = append(added, u)
		} else {
			if o.SpeedLimit != u.SpeedLimit || o.DeviceLimit != u.DeviceLimit {
				modified = append(modified, u)
			}
			delete(oldMap, u.Uuid)
		}
	}

	for _, o := range oldMap {
		deleted = append(deleted, o)
	}

	return deleted, added, modified
}
