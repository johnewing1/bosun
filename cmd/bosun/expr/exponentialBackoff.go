// Taken guidance from https://github.com/cenkalti/backoff/blob/master/exponential.go

package expr

import (
	"bosun.org/opentsdb"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type exponentialBackoff struct {
	maxNumberOfRetries uint
	backoffFactor      float64
	maxDelayMillis     float64
	attemptNumber      uint
}

const (
	defaultMaxNumberOfRetry = 3
	defaultBackoffFactor    = 2
	defaultMaxDelayMillis   = 30000
	defaultAttemptNumber    = 0
)

func newExponentialBackoff() *exponentialBackoff {
	return &exponentialBackoff{
		maxNumberOfRetries: defaultMaxNumberOfRetry,
		backoffFactor:      defaultBackoffFactor,
		maxDelayMillis:     defaultMaxDelayMillis,
		attemptNumber:      defaultAttemptNumber,
	}
}

func shouldRetry(e error) bool {
	// Retry if the status code is in 500s or 429
	if e != nil {
		if e1, ok := e.(*opentsdb.RequestError); ok {
			if (e1.Err.Code >= 500 && e1.Err.Code != 501) || e1.Err.Code == 429 {
				return true
			}
		}
	}
	return false
}

func (b *exponentialBackoff) jitterBackoff() (time.Duration, error) {
	b.attemptNumber++

	if b.attemptNumber <= b.maxNumberOfRetries {
		maxJitter := int(b.backoffFactor) * (2<<b.attemptNumber - 1)
		jitter := rand.Intn(maxJitter)
		delay := math.Min(b.maxDelayMillis, float64(jitter)*1000)

		return time.Duration(delay), nil
	}
	return 0, fmt.Errorf("retries exhausted")
}
