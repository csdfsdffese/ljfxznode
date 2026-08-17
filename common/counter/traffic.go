package counter

import (
	"sync"
	"sync/atomic"
)

type TrafficCounter struct {
	Counters sync.Map
}

type TrafficStorage struct {
	UpCounter   atomic.Int64
	DownCounter atomic.Int64
}

func NewTrafficCounter() *TrafficCounter {
	return &TrafficCounter{}
}

func (c *TrafficCounter) GetCounter(uuid string) *TrafficStorage {
	if cts, ok := c.Counters.Load(uuid); ok {
		return cts.(*TrafficStorage)
	}
	newStorage := &TrafficStorage{}
	if cts, loaded := c.Counters.LoadOrStore(uuid, newStorage); loaded {
		return cts.(*TrafficStorage)
	}
	return newStorage
}

func (c *TrafficCounter) Delete(uuid string) {
	c.Counters.Delete(uuid)
}
