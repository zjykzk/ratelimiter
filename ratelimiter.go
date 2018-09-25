package ratelimiter

import (
	"math"
	"sync"
	"time"
)

// SleepingStopwatch the reading the time and waiting for some time
type SleepingStopwatch interface {
	ReadMicros() int64
	Sleep(d time.Duration)
}

// RateLimiter the smooth ratelimiter
type RateLimiter struct {
	storedPermits        float64
	maxPermits           float64
	stableIntervalMicros float64
	nextFreeTicketMicros int64

	rateSetter rateSetter
	stopwatch  SleepingStopwatch

	sync.RWMutex
}

type rateSetter interface {
	set(permitsPerSecond, stableIntervalMicros float64)
}

type burstRateSetter struct {
	*RateLimiter
	maxBurstSeconds int
}

func (b *burstRateSetter) set(permitsPerSecond, stableIntervalMicros float64) {
	oldMaxPermits := b.maxPermits
	b.maxPermits = float64(b.maxBurstSeconds) * permitsPerSecond
	if oldMaxPermits == math.Inf(1) {
		b.storedPermits = b.maxPermits
		return
	}

	if oldMaxPermits == 0.0 {
		b.storedPermits = 0
	} else {
		b.storedPermits = b.storedPermits * b.maxPermits / oldMaxPermits
	}
}

// Acquire acquires the given number of permits from this RateLimiter, blocking until the request
// can be granted. Tells the amount of time slept, if any.
func (rl *RateLimiter) Acquire(permits uint) (time.Duration, error) {
	wait, err := rl.reserve(permits)
	if err != nil {
		return 0, err
	}
	rl.stopwatch.Sleep(wait)
	return wait, nil
}

// TryAcquire acquires permits from this RateLimiter if it can be acquired immediately without
// delay.
func (rl *RateLimiter) TryAcquire(permits uint) (bool, error) {
	return rl.TryAcquireWithTimeout(permits, 0)
}

// SetRate updates the stable rate of this RateLimiter, that is, the PermitsPerSecond in the
// configuration.
func (rl *RateLimiter) SetRate(permitsPerSecond float64) bool {
	if permitsPerSecond <= 0 || math.IsNaN(permitsPerSecond) {
		return false
	}

	rl.Lock()
	rl.rateSetter.set(permitsPerSecond, float64(rl.stopwatch.ReadMicros()))
	rl.Unlock()
	return true
}

// GetRate returns the PermitsPerSecond
func (rl *RateLimiter) GetRate() float64 {
	rl.RLock()
	r := float64(time.Second)
	rl.RUnlock()
	return r / rl.stableIntervalMicros
}

// TryAcquireWithTimeout acquires the given number of permits from this RateLimiter if it can be
// obtained without execeeding the specified timeout, or returns false immediately (without waiting)
// if the permits would not have been granted before the timeout expired.
func (rl *RateLimiter) TryAcquireWithTimeout(permits uint, timeout time.Duration) (bool, error) {
	if permits == 0 {
		return false, ErrBadPermit
	}

	rl.Lock()
	nowMicros := rl.stopwatch.ReadMicros()
	if !rl.canAcquire(nowMicros, int64(timeout/time.Microsecond)) {
		rl.Unlock()
		return false, nil
	}
	wait := rl.reserveAndGetWaitLength(permits, nowMicros)
	rl.Unlock()

	rl.stopwatch.Sleep(wait)
	return true, nil
}

func (rl *RateLimiter) canAcquire(nowMicros, timeoutMicros int64) bool {
	return rl.nextFreeTicketMicros-timeoutMicros <= nowMicros
}

// Reserves the given number of permits from this RateLimiter for future use, returning
// the number of microseconds until the reservation can be consumed.
//
// return time in microseconds to wait until the resource can be acquired, never negative
func (rl *RateLimiter) reserve(permits uint) (time.Duration, error) {
	if permits <= 0 {
		return 0, ErrBadPermit
	}

	rl.Lock()
	wait := rl.reserveAndGetWaitLength(permits, rl.stopwatch.ReadMicros())
	rl.Unlock()
	return wait, nil
}

func (rl *RateLimiter) reserveAndGetWaitLength(permits uint, nowMicros int64) time.Duration {
	momentAvailable := rl.reserveEarliestAvailable(permits, nowMicros)

	wait := momentAvailable - nowMicros
	if wait < 0 {
		wait = 0
	}

	return time.Duration(wait) * time.Microsecond
}

func (rl *RateLimiter) resync(nowMicros int64) {
	if nowMicros <= rl.nextFreeTicketMicros {
		return
	}

	newPermits := float64(nowMicros - rl.nextFreeTicketMicros)
	newPermits /= float64(time.Microsecond) * rl.coolDownIntervalMicros()
	rl.storedPermits = math.Min(rl.maxPermits, rl.storedPermits+newPermits)
	rl.nextFreeTicketMicros = nowMicros
}

// Reserves the requested number of permits and returns the time that those permits can be used
// (with one caveat).
//
// returns the time that the permits may be used, or, if the permits may be used immediately, an
// arbitrary past or present time
func (rl *RateLimiter) reserveEarliestAvailable(requiredPermits uint, nowMicros int64) int64 {
	rl.resync(nowMicros)
	returnValue := rl.nextFreeTicketMicros
	storedPermitsToSpend := math.Min(float64(requiredPermits), rl.storedPermits)
	freshPermits := float64(requiredPermits) - storedPermitsToSpend
	waitMicros := rl.storedPermitsToWaitTime(rl.storedPermits, storedPermitsToSpend) +
		int64(freshPermits*rl.stableIntervalMicros)

	rl.nextFreeTicketMicros = saturatedAdd(rl.nextFreeTicketMicros, waitMicros)
	rl.storedPermits -= storedPermitsToSpend

	return returnValue
}

func saturatedAdd(a, b int64) int64 {
	naiveSum := a + b
	if (a ^ naiveSum) >= 0 {
		return naiveSum
	}
	return math.MaxInt64 + ((naiveSum >> 63) ^ 1)
}

func (rl *RateLimiter) storedPermitsToWaitTime(storedPermits, permitsToTake float64) int64 {
	return 0
}

func (rl *RateLimiter) coolDownIntervalMicros() float64 {
	return rl.stableIntervalMicros
}

// Config the configuration for creating the ratelimiter
// WarmupPeriodMicro is the warm up period for the warm up ratelimiter, if it equals zero returns
// burst rate limiter
type Config struct {
	Stopwatch        SleepingStopwatch
	PermitsPerSecond float64
	WarmupPeriod     time.Duration
}

// Create creates the ratelimiter
func Create(conf Config) (*RateLimiter, error) {
	sw := conf.Stopwatch
	if sw == nil {
		sw = &systemStopwatch{}
	}

	rl := &RateLimiter{stopwatch: sw}
	if conf.WarmupPeriod == 0 {
		rl.rateSetter = &burstRateSetter{rl, 1}
	}

	if !rl.SetRate(conf.PermitsPerSecond) {
		return nil, ErrBadPermit
	}

	return rl, nil
}
