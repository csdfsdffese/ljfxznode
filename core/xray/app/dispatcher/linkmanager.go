package dispatcher

import (
	sync "sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	// ip 是产生该连接的来源 IP（剔除时按 IP 匹配断开）。
	ip string
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.writer.WriteMultiBuffer(mb)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	return common.Close(w.writer)
}

type LinkManager struct {
	links map[*ManagedWriter]buf.Reader
	mu    sync.Mutex
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.links[writer] = reader
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.links, writer)
}

func (m *LinkManager) CloseAll() {
	// snapshot under lock, then close outside the lock to avoid deadlock
	// (Close triggers RemoveWriter which takes the same lock)
	m.mu.Lock()
	links := make(map[*ManagedWriter]buf.Reader, len(m.links))
	for w, r := range m.links {
		links[w] = r
	}
	m.mu.Unlock()
	for w, r := range links {
		common.Close(w)
		common.Interrupt(r)
	}
}

// CloseByIP 关闭指定来源 IP 的所有连接（含 TCP 与 UDP 会话），
// 返回被关闭的连接数。IP 为空或没有匹配连接时不执行任何操作。
// 采用与 CloseAll 相同的「快照后锁外关闭」模式，避免死锁。
func (m *LinkManager) CloseByIP(ip string) int {
	if ip == "" {
		return 0
	}
	m.mu.Lock()
	matched := make([]struct {
		w *ManagedWriter
		r buf.Reader
	}, 0, 4)
	for w, r := range m.links {
		if w.ip == ip {
			matched = append(matched, struct {
				w *ManagedWriter
				r buf.Reader
			}{w, r})
		}
	}
	m.mu.Unlock()
	for _, e := range matched {
		common.Close(e.w)
		common.Interrupt(e.r)
	}
	return len(matched)
}
