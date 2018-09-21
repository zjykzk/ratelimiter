package ratelimiter

import (
	"fmt"
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
	return instant / time.Millisecond
}

func (fs *fakeStopwatch) sleepMicrosWithCaption(caption string, micros int64) {
	instant += micros * time.Millisecond
	events.append(caption + fmt.Sprintf("%3.2f", float64(micros)/float64(time.Microsecond)))
}

func (fs *fakeStopwatch) SleepMicros(micros int64) {
	sleepMicrosWithCaption("R", micros)
}

func (fs *fakeStopwatch) String() string {
	sleepMicrosWithCaption("R", micros)
}

func (fs *fakeStopwatch) readEventsAndClear() string {
	ret := strings.Join(events, ",")
	events = events[:0]
}

func assertEvents(t *testing.T, sw *fakeStopwatch, events ...string) {
	assert.Equal(t, events, sw.events())
}

func TestSimple(t *testing.T) {
	sw := fakeStopwatch{}

	rl := NewBurstRateLimiter(1, sw)
	rl.Acquire() // R0.00, since it's the first request
	rl.Acquire() // R0.20
	rl.Acquire() // R0.20
	assertEvents(t, sw, "R0.00", "R0.20", "R0.20")
}
