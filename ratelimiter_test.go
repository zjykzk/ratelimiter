package ratelimiter

import (
	"fmt"
	"math"
	"math/rand"
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
	fs.events = append(fs.events, caption+fmt.Sprintf("%3.2f", float64(d)/float64(time.Second)))
}

func (fs *fakeStopwatch) Sleep(d time.Duration) {
	fs.sleepWithCaption("R", d)
}

func (fs *fakeStopwatch) sleepMillis(m int64) {
	fs.sleepWithCaption("U", time.Duration(m)*time.Millisecond)
}

func (fs *fakeStopwatch) String() string {
	return strings.Join(fs.events, ",")
}

func (fs *fakeStopwatch) readEventsAndClear() string {
	ret := strings.Join(fs.events, ",")
	fs.events = fs.events[:0]
	return ret
}

func assertEqual(t *testing.T, a, b time.Duration) {
	d := float64(a-b) / float64(time.Second/time.Microsecond)
	assert.True(t, math.Abs(d) < 1e-8, fmt.Sprintf("a:%d, b:%d, a-b:%f", a, b, d))
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
	b, _ = rl.TryAcquire(1)
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
	assert.Equal(t, 0, int(st)) // R0.00
	sw.sleepMillis(200)         // U0.20, we are ready for the next request...
	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, int(st)) // R0.00
	st, _ = rl.Acquire(1)
	assert.Equal(t, time.Duration(rl.stableIntervalMicros)*time.Microsecond, st) // R0.20
	assertEvents(t, sw, "R0.00", "U0.20", "R0.00", "R0.20")
}

func TestSimpleAcquireEarliestAvailableIsInPast(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
	})
	st, _ := rl.Acquire(1)
	assert.Equal(t, 0, int(st))
	sw.sleepMillis(400)

	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, int(st))

	st, _ = rl.Acquire(1)
	assert.Equal(t, 0, int(st))

	st, _ = rl.Acquire(1)
	assertEqual(t, time.Duration(rl.stableIntervalMicros)*time.Microsecond, st)
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

func TestCreateWarmupParameterValidation(t *testing.T) {
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

func TestWarmUp(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, err := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 2.0,
		WarmupPeriod:     time.Millisecond * 4000,
		ColdFactor:       3.0,
	})
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #1
	}
	sw.sleepMillis(500)  // #2
	sw.sleepMillis(4000) // #3
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #4
	}
	sw.sleepMillis(500)  // #5
	sw.sleepMillis(2000) // #6
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #7
	}
	assertEvents(t, sw,
		"R0.00", "R1.38", "R1.12", "R0.88", "R0.62", "R0.50", "R0.50", "R0.50", // #1
		"U0.50",                                                                // #2
		"U4.00",                                                                // #3
		"R0.00", "R1.38", "R1.12", "R0.88", "R0.62", "R0.50", "R0.50", "R0.50", // #4
		"U0.50",                                                                // #5
		"U2.00",                                                                // #6
		"R0.00", "R0.50", "R0.50", "R0.50", "R0.50", "R0.50", "R0.50", "R0.50", // #7
	)
}

func TestWarmUpWithColdFactor(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, err := Create(Config{
		Stopwatch:        sw,
		PermitsPerSecond: 5.0,
		WarmupPeriod:     time.Millisecond * 4000,
		ColdFactor:       10.0,
	})
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #1
	}
	sw.sleepMillis(200)  // #2: to repy for the last acquire
	sw.sleepMillis(4000) // #3: becomes cold again
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #4
	}
	sw.sleepMillis(200)  // #5: to repy for the last acquire
	sw.sleepMillis(1000) // #6: still warm! It would take another 3 seconds to go cold
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #7
	}
	assertEvents(t, sw,
		"R0.00", "R1.75", "R1.26", "R0.76", "R0.30", "R0.20", "R0.20", "R0.20", // #1
		"U0.20",                                                                // #2
		"U4.00",                                                                // #3
		"R0.00", "R1.75", "R1.26", "R0.76", "R0.30", "R0.20", "R0.20", "R0.20", // #4
		"U0.20",                                                                // #5
		"U1.00",                                                                // #6
		"R0.00", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", // #7
	)
}

func TestWarmUpWithColdFactor1(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, err := Create(Config{
		PermitsPerSecond: 5.0,
		WarmupPeriod:     time.Millisecond * 4000,
		ColdFactor:       1.0,
		Stopwatch:        sw,
	})
	assert.Nil(t, err)
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #1
	}
	sw.sleepMillis(340) // #2
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #3
	}
	assertEvents(t, sw,
		"R0.00", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", // #1
		"U0.34",                                                                // #2
		"R0.00", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", "R0.20", // #3
	)
}

func TestWarmUpAndUpdate(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 2.0,
		WarmupPeriod:     time.Millisecond * 4000,
		ColdFactor:       3.0,
		Stopwatch:        sw,
	})
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #1
	}
	sw.sleepMillis(4500)     // #2: back to cold state (warmup period + repay last acquire)
	for i := 0; i < 3; i++ { // only three steps, we're somewhere in the warmup period
		rl.Acquire(1) // #3
	}

	rl.SetRate(4.0) // double the rate!
	rl.Acquire(1)   // #4, we repay the debt of the last acquire (imposed by the old rate)
	for i := 0; i < 4; i++ {
		rl.Acquire(1) // #5
	}
	sw.sleepMillis(4250) // #6, back to cold state (warmup period + repay last acquire)
	for i := 0; i < 11; i++ {
		rl.Acquire(1) // #7, showing off the warmup starting from totally cold
	}

	// make sure the areas (times) remain the same, while permits are different
	assertEvents(t, sw,
		"R0.00", "R1.38", "R1.12", "R0.88", "R0.62", "R0.50", "R0.50", "R0.50", // #1
		"U4.50",                   // #2
		"R0.00", "R1.38", "R1.12", // #3, after that the rate changes
		"R0.88",                            // #4, this is what the throttling would be with the old rate
		"R0.34", "R0.28", "R0.25", "R0.25", // #5
		"U4.25",                                                       // #6
		"R0.00", "R0.72", "R0.66", "R0.59", "R0.53", "R0.47", "R0.41", // #7
		"R0.34", "R0.28", "R0.25", "R0.25", // #7 (cont.), note this matches #5
	)
}

func TestWarmUpAndUpdateWithColdFactor(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 5.0,
		WarmupPeriod:     time.Millisecond * 4000,
		ColdFactor:       10.0,
		Stopwatch:        sw,
	})
	for i := 0; i < 8; i++ {
		rl.Acquire(1) // #1
	}
	sw.sleepMillis(4200)     // #2: back to cold state (warmup period + repay last acquire)
	for i := 0; i < 3; i++ { // only three steps, we're somewhere in the warmup period
		rl.Acquire(1) // #3
	}

	rl.SetRate(10.0) // double the rate!
	rl.Acquire(1)    // #4, we repay the debt of the last acquire (imposed by the old rate)
	for i := 0; i < 4; i++ {
		rl.Acquire(1) // #5
	}
	sw.sleepMillis(4100) // #6, back to cold state (warmup period + repay last acquire)
	for i := 0; i < 11; i++ {
		rl.Acquire(1) // #7, showing off the warmup starting from totally cold
	}

	// make sure the areas (times) remain the same, while permits are different
	assertEvents(t, sw,
		"R0.00", "R1.75", "R1.26", "R0.76", "R0.30", "R0.20", "R0.20", "R0.20", // #1
		"U4.20",                   // #2
		"R0.00", "R1.75", "R1.26", // #3, after that the rate changes
		"R0.76",                            // #4, this is what the throttling would be with the old rate
		"R0.20", "R0.10", "R0.10", "R0.10", // #5
		"U4.10",                                                       // #6
		"R0.00", "R0.94", "R0.81", "R0.69", "R0.57", "R0.44", "R0.32", // #7
		"R0.20", "R0.10", "R0.10", "R0.10", // #7 (cont.), note, this matches #5
	)
}

func TestBurstyAndUpdate(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 1.0,
		Stopwatch:        sw,
	})
	rl.Acquire(1) // no wait
	rl.Acquire(1) // R1.00, to repay previous

	rl.SetRate(2.0) // update the rate!

	rl.Acquire(1) // R1.00, to repay previous (the previous was under the old rate!)
	rl.Acquire(2) // R0.50, to repay previous (now the rate takes effect)
	rl.Acquire(4) // R1.00, to repay previous
	rl.Acquire(1) // R2.00, to repay previous
	assertEvents(t, sw, "R0.00", "R1.00", "R1.00", "R0.50", "R1.00", "R2.00")
}

func TestTryAcquireNoWaitAllowed(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 5.0,
		Stopwatch:        sw,
	})
	b, _ := rl.TryAcquireWithTimeout(1, 0)
	assert.True(t, b)
	b, _ = rl.TryAcquireWithTimeout(1, 0)
	assert.False(t, b)
	b, _ = rl.TryAcquireWithTimeout(1, 0)
	assert.False(t, b)
	sw.sleepMillis(100)
	b, _ = rl.TryAcquireWithTimeout(1, 0)
	assert.False(t, b)
}

func TestTryAcquireSomeWaitAllowed(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 5.0,
		Stopwatch:        sw,
	})

	b, _ := rl.TryAcquireWithTimeout(1, 0)
	assert.True(t, b)
	b, _ = rl.TryAcquireWithTimeout(1, 200*time.Millisecond)
	assert.True(t, b)
	b, _ = rl.TryAcquireWithTimeout(1, 100*time.Millisecond)
	assert.False(t, b)
	sw.sleepMillis(100)
	b, _ = rl.TryAcquireWithTimeout(1, 100*time.Millisecond)
	assert.True(t, b)
}

func TestTryAcquireOverflow(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 5.0,
		Stopwatch:        sw,
	})
	b, _ := rl.TryAcquireWithTimeout(1, 0)
	assert.True(t, b)
	sw.sleepMillis(100)
	b, _ = rl.TryAcquireWithTimeout(1, math.MaxInt64)
	assert.True(t, b)
}

func TestTryAcquireNegative(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 5.0,
		Stopwatch:        sw,
	})
	b, _ := rl.TryAcquireWithTimeout(5, 0)
	assert.True(t, b)
	sw.sleepMillis(900)
	b, _ = rl.TryAcquireWithTimeout(1, math.MinInt64)
	assert.False(t, b)
	sw.sleepMillis(100)
	b, _ = rl.TryAcquireWithTimeout(1, -1)
	assert.True(t, b)
}

func TestSimpleWeights(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 1.0,
		Stopwatch:        sw,
	})
	rl.Acquire(1) // no wait
	rl.Acquire(1) // R1.00, to repay previous
	rl.Acquire(2) // R1.00, to repay previous
	rl.Acquire(4) // R2.00, to repay previous
	rl.Acquire(8) // R4.00, to repay previous
	rl.Acquire(1) // R8.00, to repay previous
	assertEvents(t, sw, "R0.00", "R1.00", "R1.00", "R2.00", "R4.00", "R8.00")
}

func TestInfinityBursty(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: math.Inf(1),
		Stopwatch:        sw,
	})
	rl.Acquire(math.MaxInt32 / 4)
	rl.Acquire(math.MaxInt32 / 2)
	rl.Acquire(math.MaxInt32)
	assertEvents(t, sw, "R0.00", "R0.00", "R0.00") // no wait, infinite rate!

	rl.SetRate(2.0)
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	assertEvents(t, sw,
		"R0.00",          // First comes the saved-up burst, which defaults to a 1-second burst (2 requests).
		"R0.00", "R0.00", // Now comes the free request.
		"R0.50", // Now it's 0.5 seconds per request.
		"R0.50")

	rl.SetRate(math.MaxFloat64)
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	assertEvents(t, sw, "R0.50", "R0.00", "R0.00") // we repay the last request (.5sec), then back to +oo
}
func TestInfinityBustyTimeElapsed(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: math.Inf(1),
		Stopwatch:        sw,
	})
	sw.instant += 1000000
	rl.SetRate(2.0)
	for i := 0; i < 5; i++ {
		rl.Acquire(1)
	}
	assertEvents(t, sw,
		"R0.00",          // First comes the saved-up burst, which defaults to a 1-second burst (2 requests).
		"R0.00", "R0.00", // Now comes the free request.
		"R0.50", // Now it's 0.5 seconds per request.
		"R0.50")
}

func TestInfinityWarmUp(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: math.Inf(1),
		WarmupPeriod:     10 * time.Second,
		ColdFactor:       3,
		Stopwatch:        sw,
	})
	rl.Acquire(math.MaxInt32 / 4)
	rl.Acquire(math.MaxInt32 / 2)
	rl.Acquire(math.MaxInt32)
	assertEvents(t, sw, "R0.00", "R0.00", "R0.00")

	rl.SetRate(1.0)
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	assertEvents(t, sw, "R0.00", "R1.00", "R1.00")

	rl.SetRate(math.Inf(1))
	rl.Acquire(1)
	rl.Acquire(1)
	rl.Acquire(1)
	assertEvents(t, sw, "R1.00", "R0.00", "R0.00")
}

func TestInfinityWarmUpTimeElapsed(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: math.Inf(1),
		WarmupPeriod:     10 * time.Second,
		ColdFactor:       3,
		Stopwatch:        sw,
	})
	sw.instant += 1000000
	rl.SetRate(1.0)
	for i := 0; i < 5; i++ {
		rl.Acquire(1)
	}
	assertEvents(t, sw, "R0.00", "R1.00", "R1.00", "R1.00", "R1.00")
}

/**
 * Make sure that bursts can never go above 1-second-worth-of-work for the current rate, even when
 * we change the rate.
 */
func TestWeNeverGetABurstMoreThanOneSec(t *testing.T) {
	sw := &fakeStopwatch{}
	rl, _ := Create(Config{
		PermitsPerSecond: 1,
		Stopwatch:        sw,
	})
	rates := []float64{1000, 1, 10, 1000000, 10, 1}
	for _, rate := range rates {
		oneSecWorthOfWork := int(rate)
		sw.sleepMillis(int64(rate) * 1000)
		rl.SetRate(rate)
		burst := measureTotalTimeMillis(sw, rl, oneSecWorthOfWork)
		// we allow one second worth of work to go in a burst (i.e. take less than a second)
		assert.True(t, burst <= 1000)
		afterBurst := measureTotalTimeMillis(sw, rl, oneSecWorthOfWork)
		// but work beyond that must take at least one second
		assert.True(t, afterBurst >= 1000)
	}
}

/**
 * This neat test shows that no matter what weights we use in our requests, if we push X amount of
 * permits in a cool state, where X = rate * timeToCoolDown, and we have specified a
 * timeToWarmUp() period, it will cost as the prescribed amount of time. E.g., calling
 * [acquire(5), acquire(1)] takes exactly the same time as [acquire(2), acquire(3), acquire(1)].
 */
func TestTimeToWarmUpIsHonouredEvenWithWeights(t *testing.T) {
	sw := &fakeStopwatch{}
	warmupPermits := 10
	coldFactorsToTest := []float64{2.0, 3.0, 10.0}
	qpsToTest := []float64{4.0, 2.0, 1.0, 0.5, 0.1}
	for trial := 0; trial < 100; trial++ {
		for _, coldFactor := range coldFactorsToTest {
			for _, qps := range qpsToTest {
				// If warmupPermits = maxPermits - thresholdPermits then
				// warmupPeriod = (1 + coldFactor) * warmupPermits * stableInterval / 2
				warmupMillis := int64((1 + coldFactor) * float64(warmupPermits) / (2.0 * qps) * 1000.0)
				rl, _ := Create(Config{
					PermitsPerSecond: qps,
					WarmupPeriod:     time.Duration(warmupMillis) * time.Millisecond,
					ColdFactor:       coldFactor,

					Stopwatch: sw,
				})
				assert.Equal(t, warmupMillis, measureTotalTimeMillis(sw, rl, warmupPermits))
			}
		}
	}
}

func measureTotalTimeMillis(stopwatch *fakeStopwatch, rateLimiter *RateLimiter, permits int) int64 {
	startTime := stopwatch.instant
	for permits > 0 {
		nextPermitsToAcquire := rand.Intn(permits)
		if nextPermitsToAcquire < 1 {
			nextPermitsToAcquire = 1
		}
		permits -= nextPermitsToAcquire
		rateLimiter.Acquire(uint(nextPermitsToAcquire))
	}
	rateLimiter.Acquire(1) // to repay for any pending debt
	return (stopwatch.instant - startTime) / int64(time.Millisecond)
}
