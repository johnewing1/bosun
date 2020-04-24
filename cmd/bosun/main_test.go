package main

import (
	version "bosun.org/_version"
	"github.com/facebookgo/httpcontrol"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// test to ensure that we modify the headers in an idempotent way.
// not doing so can lead to errors with retries of signed requests such as
// cloudwatch queries

func TestAddHeaders(t *testing.T) {

	hostname := "localhost"
	useragent := "Bosun/" + version.ShortVersion()
	expected := make(http.Header)
	expected.Set("User-Agent", useragent)
	expected.Set("X-Bosun-Server", hostname)

	initHostManager(hostname)

	transport := bosunHttpTransport{
		useragent,
		&httpcontrol.Transport{
			Proxy:          http.ProxyFromEnvironment,
			RequestTimeout: time.Minute,
			MaxTries:       3,
		},
	}

	h := make(http.Header)
	request := http.Request{
		Header: h,
	}

	// this action should be idempotent
	transport.RoundTrip(&request)
	transport.RoundTrip(&request)

	got := request.Header
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("Host headers wrong, expected %v, got %v \n", expected, got)
	}
}
