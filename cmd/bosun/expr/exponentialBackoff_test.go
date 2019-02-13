package expr

import (
	"bosun.org/opentsdb"
	"testing"
	"time"
)

func TestShouldRetry_True(t *testing.T) {
	tests := []struct {
		code int
	}{
		{429},
		{500},
		{503},
	}
	for i, test := range tests {
		var err error
		err = &opentsdb.RequestError{Request: ""}
		err.(*opentsdb.RequestError).Err.Code = test.code
		retry := shouldRetry(err)
		if retry != true {
			t.Errorf("Test %d: Request with status code %d should be retried", i, test.code)
		}
	}
}

func TestShouldRetry_False(t *testing.T) {
	tests := []struct {
		code int
	}{
		{0},
		{200},
		{403},
	}
	for i, test := range tests {
		var err error
		err = &opentsdb.RequestError{Request: ""}
		err.(*opentsdb.RequestError).Err.Code = test.code
		var retry bool
		retry = shouldRetry(err)
		if retry != false {
			t.Errorf("Test %d: Request with status code %d should not be retried", i, test.code)
		}
	}
}

func TestJitterBackoff(t *testing.T) {
	exponentialBackoff := newExponentialBackoff()

	for i := 0; i < int(exponentialBackoff.maxNumberOfRetries); i++ {
		_, err := exponentialBackoff.jitterBackoff()
		if err != nil {
			t.Error("Retries exhausted before reaching max retry limit")
		}
	}
}

func TestJitterBackoff_RetriesExhausted(t *testing.T) {
	exponentialBackoff := &exponentialBackoff{2, 1, 10000, 2}
	_, err := exponentialBackoff.jitterBackoff()
	if err == nil {
		t.Error("Retries not exhausted after reaching maximum limit")
	}
}

func TestJitterBackoff_InRange(t *testing.T) {
	exponentialBackoff := newExponentialBackoff()

	for i := 0; i < int(exponentialBackoff.maxNumberOfRetries); i++ {
		sleepTime, _ := exponentialBackoff.jitterBackoff()
		if sleepTime > time.Duration(exponentialBackoff.maxDelayMillis) ||
			sleepTime > time.Duration(int(exponentialBackoff.backoffFactor)*(2<<exponentialBackoff.attemptNumber-1)*1000) ||
			sleepTime <= time.Duration(0) {
			t.Error("Back off time not in range")
		}
	}
}
