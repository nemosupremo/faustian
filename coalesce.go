package faustian

import (
	"sync/atomic"
	"time"
)

type CoalesceChan struct {
	C     <-chan time.Time
	c     chan time.Time
	wake  chan time.Time
	buff  chan time.Time
	close chan struct{}
	b     int32
	delay time.Duration
}

func NewCoalesceChan(delay time.Duration) *CoalesceChan {
	ch := make(chan time.Time)
	c := &CoalesceChan{
		C:     ch,
		c:     ch,
		close: make(chan struct{}),
		buff:  make(chan time.Time),
		wake:  make(chan time.Time),
	}

	go func(delay time.Duration) {
		defer close(c.c)
		for {
			select {
			case t := <-c.buff:
				if delay > 0 {
					time.Sleep(delay)
				}
				c.c <- t
				atomic.StoreInt32(&c.b, 0)
			case <-c.close:
				return
			}
		}
	}(delay)
	go func() {
		defer close(c.buff)
		for {
			select {
			case t := <-c.wake:
				if atomic.CompareAndSwapInt32(&c.b, 0, 1) {
					c.buff <- t
				}
			case <-c.close:
				return
			}
		}
	}()
	return c
}

func (c *CoalesceChan) Wake() {
	c.wake <- time.Now()
}

func (c *CoalesceChan) Close() {
	close(c.close)
}
