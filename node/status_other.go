//go:build !linux

package node

import "github.com/csdfsdffese/ljfxznode/api/panel"

type statusChecker struct{}

func newStatusChecker() *statusChecker { return &statusChecker{} }

func (s *statusChecker) collect() *panel.NodeStatus {
	return &panel.NodeStatus{
		CPU:  0,
		Mem:  &panel.SystemSegment{},
		Swap: &panel.SystemSegment{},
		Disk: &panel.SystemSegment{},
	}
}
