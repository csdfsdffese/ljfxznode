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
