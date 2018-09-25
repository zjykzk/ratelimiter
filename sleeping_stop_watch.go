package ratelimiter

import "time"

type systemStopwatch struct{}

func (systemStopwatch) ReadMicros() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

func (systemStopwatch) Sleep(d time.Duration) {
	time.Sleep(d)
}
