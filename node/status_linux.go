//go:build linux

package node

import (
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/csdfsdffese/ljfxznode/api/panel"
)

// statusChecker samples CPU/mem/swap/disk usage from /proc and /sys.
// CPU usage is computed as a differential sample between two consecutive calls.
type statusChecker struct {
	lastCPUIdle  uint64
	lastCPUTotal uint64
}

func newStatusChecker() *statusChecker {
	return &statusChecker{}
}

// checkCPU returns the CPU usage percentage (0-100) since the previous call.
func (s *statusChecker) checkCPU() float64 {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(l, "cpu ") {
			continue
		}
		fields := strings.Fields(l)
		if len(fields) < 8 {
			return 0
		}
		var idle, total uint64
		for i, f := range fields[1:] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return 0
			}
			total += v
			if i == 3 || i == 4 { // idle + iowait
				idle += v
			}
		}
		if s.lastCPUTotal == 0 {
			s.lastCPUIdle, s.lastCPUTotal = idle, total
			return 0
		}
		// /proc/stat counters can wrap or reset (VM migration / boot);
		// in that case re-baseline and report 0 instead of a negative CPU.
		if total < s.lastCPUTotal {
			s.lastCPUIdle, s.lastCPUTotal = idle, total
			return 0
		}
		dTotal := total - s.lastCPUTotal
		dIdle := idle - s.lastCPUIdle
		s.lastCPUIdle, s.lastCPUTotal = idle, total
		if dTotal == 0 {
			return 0
		}
		usage := float64(dTotal-dIdle) * 100 / float64(dTotal)
		if usage < 0 {
			usage = 0
		}
		if usage > 100 {
			usage = 100
		}
		return usage
	}
	return 0
}

// readMeminfo parses a keyed u64 value from /proc/meminfo.
func readMeminfo(key string) uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(l, key+":") {
			fields := strings.Fields(l)
			if len(fields) < 2 {
				return 0
			}
			v, _ := strconv.ParseUint(fields[1], 10, 64)
			return v * 1024 // kB -> bytes
		}
	}
	return 0
}

// checkMem returns total and used memory in bytes.
func (s *statusChecker) checkMem() (total, used uint64) {
	total = readMeminfo("MemTotal")
	available := readMeminfo("MemAvailable")
	if total > available {
		used = total - available
	}
	return
}

// checkSwap returns total and used swap in bytes.
func (s *statusChecker) checkSwap() (total, used uint64) {
	total = readMeminfo("SwapTotal")
	free := readMeminfo("SwapFree")
	if total > free {
		used = total - free
	}
	return
}

// checkDisk returns total and used disk space of the root filesystem in bytes.
func (s *statusChecker) checkDisk() (total, used uint64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return 0, 0
	}
	bsize := uint64(stat.Bsize)
	total = stat.Blocks * bsize
	used = (stat.Blocks - stat.Bfree) * bsize
	return
}

// collect builds a NodeStatus snapshot.
func (s *statusChecker) collect() *panel.NodeStatus {
	memTotal, memUsed := s.checkMem()
	swapTotal, swapUsed := s.checkSwap()
	diskTotal, diskUsed := s.checkDisk()
	return &panel.NodeStatus{
		CPU:  s.checkCPU(),
		Mem:  &panel.SystemSegment{Total: memTotal, Used: memUsed},
		Swap: &panel.SystemSegment{Total: swapTotal, Used: swapUsed},
		Disk: &panel.SystemSegment{Total: diskTotal, Used: diskUsed},
	}
}
