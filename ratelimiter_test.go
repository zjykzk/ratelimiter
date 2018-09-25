package ratelimiter

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeStopwatch struct {
	instant int64
	events  []string
}

func (fs *fakeStopwatch) ReadMicros() int64 {
	return fs.instant / int64(time.Microsecond)
}

func (fs *fakeStopwatch) sleepWithCaption(caption string, d time.Duration) {
	fs.instant += int64(d)
	fs.events = append(fs.events, caption+fmt.Sprintf("%3.2f", float64(d)/float64(time.Nanosecond)))
}

func (fs *fakeStopwatch) Sleep(d time.Duration) {
	fs.sleepWithCaption("R", d)
}

func (fs *fakeStopwatch) sleepMillis(m int64) {
	fs.sleepWithCaption("U", time.Duration(m))
}

func (fs *fakeStopwatch) String() string {
	return strings.Join(fs.events, ",")
}

func (fs *fakeStopwatch) readEventsAndClear() string {
	ret := strings.Join(fs.events, ",")
	fs.events = fs.events[:0]
	return ret
}

func assertEvents(t *testing.T, sw *fakeStopwatch, events ...string) {
	assert.Equal(t, strings.Join(events, ","), sw.readEventsAndClear())
}

func TestSimple(t *testing.T) {
	sw := &fakeStopwatch{}

	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5,
	})
	rl.Acquire(1) // R0.00, since it's the first request
	rl.Acquire(1) // R0.20
	rl.Acquire(1) // R0.20
	assertEvents(t, sw, "R0.00", "R0.20", "R0.20")
}

func TestImmediateTryAcruire(t *testing.T) {
	rl, _ := Create(Config{
		PermitsPerSecond: 1,
	})
	b, _ := rl.TryAcquire(1)
	assert.True(t, b, "Unable to acquire initial permit")
	b, _ = rl.TryAcquire(1)
	assert.False(t, b, "Capable of acquiring secondary permit")
}

func TestDoubleMinValueCanAcquireExactlyOnce(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: math.SmallestNonzeroFloat64,
	})
	b, _ := rl.TryAcquire(1)
	assert.True(t, b, "Unable to acquire initial permit")
	assert.False(t, b, "Capable of acquiring an additional permit")

	sw.sleepMillis(math.MaxInt32)
	b, _ = rl.TryAcquire(1)
	assert.False(t, b, "Capable of acquiring an additional permit after sleeping")
}

func testSimpleRateUpdate(t *testing.T) {
	rl, _ := Create(Config{
		PermitsPerSecond: 5,
		WarmupPeriod:     5,
	})
	assert.Equal(t, 5, rl.GetRate())
	rl.SetRate(10.0)
	assert.Equal(t, 10, rl.GetRate())

	assert.NotNil(t, rl.SetRate(0.0))
	assert.NotNil(t, rl.SetRate(-10.0))
}

func TestAcquireParameterValidation(t *testing.T) {
	rl, _ := Create(Config{
		PermitsPerSecond: 999,
	})
	_, err := rl.Acquire(0)
	assert.NotNil(t, err)
	_, err = rl.TryAcquire(0)
	assert.NotNil(t, err)
	_, err = rl.TryAcquireWithTimeout(0, time.Second)
	assert.NotNil(t, err)
}

func TestSimpleWithWait(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
	})

	rl.Acquire(1)       // R0.00
	sw.sleepMillis(200) // U0.20, we are ready for the next request...
	rl.Acquire(1)       // R0.00, ...which is granted immediately
	rl.Acquire(1)       // R0.20
	assertEvents(t, sw, "R0.00", "U0.20", "R0.00", "R0.20")
}

func TestSimpleAcquireReturnValues(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
	})
	st, _ := rl.Acquire(1)
	assert.Equal(t, 0, st) // R0.00
	sw.sleepMillis(200)    // U0.20, we are ready for the next request...
	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, st) // R0.00
	st, _ = rl.Acquire(1)
	assert.Equal(t, 2*time.Millisecond/10, st) // R0.20
	assertEvents(t, sw, "R0.00", "U0.20", "R0.00", "R0.20")
}

func TestSimpleAcquireEarliestAvailableIsInPast(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
	})
	st, _ := rl.Acquire(1)
	assert.Equal(t, 0, st)
	sw.sleepMillis(400)
	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, st)
	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, st)
	st, _ = rl.Acquire(1)
	assert.Equal(t, 2*time.Millisecond/10, st)
}

func TestOneSecondBurst(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
	})

	sw.sleepMillis(1000) // max capacity reached
	sw.sleepMillis(1000) // this makes no difference
	rl.Acquire(1)        // R0.00, since it's the first request

	rl.Acquire(1) // R0.00, from capacity
	rl.Acquire(3) // R0.00, from capacity
	rl.Acquire(1) // R0.00, concluding a burst of 5 permits

	rl.Acquire(1) // R0.20, capacity exhausted
	assertEvents(t, sw,
		"U1.00", "U1.00", "R0.00", "R0.00", "R0.00", "R0.00", // first request and burst
		"R0.20")
}

func testCreateWarmupParameterValidation(t *testing.T) {
	_, err := Create(Config{
		PermitsPerSecond: 5.0,
		WarmupPeriod:     time.Nanosecond,
	})
	assert.Nil(t, err)

	_, err = Create(Config{
		PermitsPerSecond: 1.0,
	})
	assert.Nil(t, err)

	_, err = Create(Config{
		WarmupPeriod: time.Nanosecond,
	})
	assert.NotNil(t, err)

	_, err = Create(Config{
		PermitsPerSecond: 1.0,
		WarmupPeriod:     -time.Nanosecond,
	})
	assert.NotNil(t, err)
}
