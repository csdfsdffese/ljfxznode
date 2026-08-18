package task

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Name     string
	Interval time.Duration
	Execute  func() error
	access   sync.Mutex
	running  bool
	stop     chan struct{}

	// execMu/inFlight 防止任务超时后旧 goroutine 未退出、下一 tick 并发执行
	// 同一任务（会对共享状态造成数据竞争）。超时期间 inFlight 保持 true，
	// 后续 tick 跳过；旧 goroutine 完成后自动复位恢复执行。
	execMu   sync.Mutex
	inFlight bool
}

func (t *Task) Start(first bool) error {
	t.access.Lock()
	if t.running {
		t.access.Unlock()
		return nil
	}
	t.running = true
	t.stop = make(chan struct{})
	t.access.Unlock()

	go func() {
		if first {
			if err := t.executeWithTimeout(); err != nil {
				t.access.Lock()
				t.running = false
				close(t.stop)
				t.access.Unlock()
				return
			}
		}

		for {
			select {
			case <-time.After(t.Interval):
			case <-t.stop:
				return
			}

			if err := t.executeWithTimeout(); err != nil {
				t.access.Lock()
				t.running = false
				close(t.stop)
				t.access.Unlock()
				return
			}
		}
	}()

	return nil
}

// executeWithTimeout runs Execute with a bounded timeout so that a hung task
// can never silently kill the node. On timeout it only logs the problem and
// lets the next tick retry — it never triggers a full reload (upstream
// has no such mechanism, and a reload would rebuild every node inbound and
// drop all client connections).
func (t *Task) executeWithTimeout() error {
	// 上一个执行实例仍未结束（超时未返回）时跳过本轮，避免并发执行。
	t.execMu.Lock()
	if t.inFlight {
		t.execMu.Unlock()
		log.WithField("task", t.Name).Warn("Task still in flight, skipping this tick")
		return nil
	}
	t.inFlight = true
	t.execMu.Unlock()

	done := make(chan error, 1)
	go func() {
		defer func() {
			// a panic inside a task goroutine must not take down the whole
			// process (e.g. an unexpected nil from the panel response)
			if r := recover(); r != nil {
				log.WithFields(log.Fields{
					"task":  t.Name,
					"panic": r,
				}).Error("Task panicked, recovered")
				done <- errors.New("task panicked")
			}
			// 无论正常返回还是 panic，都复位 inFlight，让后续 tick 恢复执行。
			t.execMu.Lock()
			t.inFlight = false
			t.execMu.Unlock()
		}()
		done <- t.Execute()
	}()
	timeout := 5 * t.Interval
	if timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}
	select {
	case <-time.After(timeout):
		log.WithFields(log.Fields{
			"task":    t.Name,
			"timeout": timeout,
		}).Error("Task execution timed out, will retry on next tick")
		return nil
	case err := <-done:
		return err
	}
}

func (t *Task) Close() {
	t.access.Lock()
	if t.running {
		t.running = false
		close(t.stop)
	}
	t.access.Unlock()
}
