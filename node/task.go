package node

import (
	"time"

	"github.com/csdfsdffese/ljfxznode/api/panel"
	"github.com/csdfsdffese/ljfxznode/common/task"
	vCore "github.com/csdfsdffese/ljfxznode/core"
	"github.com/csdfsdffese/ljfxznode/limiter"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch node info task
	c.nodeInfoMonitorPeriodic = &task.Task{
		Name:     "nodeInfoMonitor",
		Interval: node.PullInterval,
		Execute:  c.nodeInfoMonitor,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTraffic",
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
	}
	// report node load status task
	c.statusReportPeriodic = &task.Task{
		Name:     "reportNodeStatus",
		Interval: node.PushInterval,
		Execute:  c.reportStatusTask,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	// delay to start nodeInfoMonitor
	_ = c.nodeInfoMonitorPeriodic.Start(false)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(false)
	_ = c.statusReportPeriodic.Start(false)
	if node.Security == panel.Tls {
		switch c.CertConfig.CertMode {
		case "none", "", "file", "self":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCert",
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

func (c *Controller) nodeInfoMonitor() (err error) {
	// get node info
	newN, err := c.apiClient.GetNodeInfo()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}
	// get user info
	newU, err := c.apiClient.GetUserList()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get user list failed")
		return nil
	}
	// get user alive
	newA, err := c.apiClient.GetUserAlive()
	if err != nil {
		// alivelist 失败只降级跳过 alive 更新（保留旧表），不阻塞本轮
		// 用户列表同步——否则用户增删改会被 alivelist 故障拖住一整轮。
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Warn("Get alive list failed, keep old alive list")
		newA = nil
	}
	if newN != nil {
		c.stateMu.Lock()
		c.info = newN
		c.stateMu.Unlock()
		// 先基于旧 userList 计算增量，供 limiter 增量同步
		deletedU, addedU, modifiedU := func() ([]panel.UserInfo, []panel.UserInfo, []panel.UserInfo) {
			if newU == nil {
				return nil, nil, nil
			}
			c.userListMu.Lock()
			defer c.userListMu.Unlock()
			return compareUserList(c.userList, newU)
		}()
		// Remove old node
		log.WithField("tag", c.tag).Info("Node changed, reload")
		err = c.server.DelNode(c.tag)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete node failed")
			return nil
		}

		// 同步 limiter 用户表：tag 变化时整体重建（用户以 newU 全量进入）；
		// tag 不变时增量 UpdateUser。自定义 Options.Name 场景同样覆盖，
		// 避免 limiter 用户表与 userList 失步（新用户被拒、删用户残留）。
		if len(c.Options.Name) == 0 {
			newTag := c.buildNodeTag(newN)
			if newTag != c.tag {
				// Remove Old limiter (delete the old tag, not the new one,
				// otherwise the previous limiter leaks in the global map)
				limiter.DeleteLimiter(c.tag)
				c.stateMu.Lock()
				c.tag = newTag
				c.stateMu.Unlock()
				userData := newU
				if userData == nil {
					userData = c.userList
				}
				l := limiter.AddLimiter(c.tag, &c.LimitConfig, userData, newA)
				c.stateMu.Lock()
				c.limiter = l
				c.stateMu.Unlock()
				// 用户与 alive 已由 AddLimiter 写入，跳过后续增量同步
				deletedU, addedU, modifiedU = nil, nil, nil
				newA = nil
			}
		}
		if len(deletedU)+len(addedU)+len(modifiedU) > 0 {
			c.stateMu.RLock()
			c.limiter.UpdateUser(c.tag, addedU, deletedU, modifiedU)
			c.stateMu.RUnlock()
		}
		if newU != nil {
			c.userListMu.Lock()
			c.userList = newU
			c.userListMu.Unlock()
		}
		// update alive list
		if newA != nil {
			c.stateMu.RLock()
			c.limiter.SetAliveList(newA)
			c.stateMu.RUnlock()
		}
		// Update rule
		c.stateMu.RLock()
		err = c.limiter.UpdateRule(&newN.Rules)
		c.stateMu.RUnlock()
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Update Rule failed")
			return nil
		}

		// check cert
		if newN.Security == panel.Tls {
			err = c.requestCert()
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Request cert failed")
				return nil
			}
		}
		// add new node
		err = c.server.AddNode(c.tag, newN, c.Options)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add node failed")
			return nil
		}
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			Users:    c.userList,
			NodeInfo: newN,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			return nil
		}
		// Check interval
		if c.nodeInfoMonitorPeriodic.Interval != newN.PullInterval &&
			newN.PullInterval != 0 {
			c.nodeInfoMonitorPeriodic.Interval = newN.PullInterval
			c.nodeInfoMonitorPeriodic.Close()
			_ = c.nodeInfoMonitorPeriodic.Start(false)
		}
		if c.userReportPeriodic.Interval != newN.PushInterval &&
			newN.PushInterval != 0 {
			c.userReportPeriodic.Interval = newN.PushInterval
			c.userReportPeriodic.Close()
			_ = c.userReportPeriodic.Start(false)
			// status report follows the same push interval
			c.statusReportPeriodic.Interval = newN.PushInterval
			c.statusReportPeriodic.Close()
			_ = c.statusReportPeriodic.Start(false)
		}
		log.WithField("tag", c.tag).Infof("Added %d new users", len(c.userList))
		// exit
		return nil
	}
	// update alive list
	if newA != nil {
		c.stateMu.RLock()
		c.limiter.SetAliveList(newA)
		c.stateMu.RUnlock()
	}
	// node no changed, check users
	// newU == nil 表示面板返回 304（用户列表未变化），此时跳过是正确语义；
	// 若面板返回 200 + 空列表（用户被全部清空/禁用），按"全部删除"处理，
	// 避免旧用户永久残留。
	if newU == nil {
		return nil
	}
	deleted, added, modified := func() ([]panel.UserInfo, []panel.UserInfo, []panel.UserInfo) {
		c.userListMu.Lock()
		defer c.userListMu.Unlock()
		return compareUserList(c.userList, newU)
	}()
	if len(deleted) > 0 {
		// have deleted users
		c.stateMu.RLock()
		err = c.server.DelUsers(deleted, c.tag, c.info)
		c.stateMu.RUnlock()
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Delete users failed")
			return nil
		}
	}
	if len(added) > 0 {
		// have added users
		c.stateMu.RLock()
		_, err = c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    added,
		})
		c.stateMu.RUnlock()
		if err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Add users failed")
			return nil
		}
	}
	if len(added) > 0 || len(deleted) > 0 || len(modified) > 0 {
		// update Limiter
		c.stateMu.RLock()
		c.limiter.UpdateUser(c.tag, added, deleted, modified)
		c.stateMu.RUnlock()
	}
	c.userListMu.Lock()
	c.userList = newU
	c.userListMu.Unlock()
	if len(added)+len(deleted)+len(modified) != 0 {
		log.WithField("tag", c.tag).
			Infof("%d user deleted, %d user added, %d user modified", len(deleted), len(added), len(modified))
	}
	return nil
}
