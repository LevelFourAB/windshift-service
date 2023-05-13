package events

import "sync"

type Limiter struct {
	value int
	max   int
	mu    *sync.Mutex
	cond  *sync.Cond
}

func NewLimiter(max int) *Limiter {
	sl := &Limiter{
		max: max,
		mu:  &sync.Mutex{},
	}
	sl.cond = sync.NewCond(sl.mu)
	return sl
}

func (s *Limiter) Add() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.value++
}

func (s *Limiter) Remove() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.value > 0 {
		s.value--
		s.cond.Broadcast()
	}
}

func (s *Limiter) Wait() <-chan int {
	ch := make(chan int, 1)

	go func() {
		s.cond.L.Lock()
		for s.value >= s.max {
			s.cond.Wait()
		}
		ch <- s.value
		s.cond.L.Unlock()
	}()

	return ch
}

func (s *Limiter) GetAvailable() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.max - s.value
}
