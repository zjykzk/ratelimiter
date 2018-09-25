package ratelimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSystem(t *testing.T) {
	s := systemStopwatch{}
	now := s.ReadMicros()
	s.Sleep(time.Microsecond * 100)
	assert.True(t, s.ReadMicros()-now > int64(time.Microsecond*100))
}
