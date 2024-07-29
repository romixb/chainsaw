package utils

import (
	"sync"
	"sync/atomic"
)

type WaitGroupWrap struct {
	wg      sync.WaitGroup
	counter atomic.Int32
}

func (wg *WaitGroupWrap) Add(delta int) {
	wg.counter.Add(int32(delta))
	wg.wg.Add(delta)
}
func (wg *WaitGroupWrap) Done() {
	wg.counter.Add(int32(-1))
	wg.wg.Done()
}
func (wg *WaitGroupWrap) Wait() {
	wg.wg.Wait()
}
func (wg *WaitGroupWrap) Counter() int32 {
	return wg.counter.Load()
}
