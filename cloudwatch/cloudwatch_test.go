package cloudwatch

import (
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"sync"
	"testing"
	"time"
)

type slowProfileProvider struct {
	callCount int
}

func (s *slowProfileProvider) NewProfile(name, region string) cloudwatchiface.CloudWatchAPI {
	s.callCount += 1
	time.Sleep(3 * time.Second)
	return &cloudwatch.CloudWatch{}
}

func TestGetProfilOnlyCalledOnce(t *testing.T) {
	wg := sync.WaitGroup{}
	provider := &slowProfileProvider{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, _ :=  GetContextWithProvider(provider).(*cloudWatchContext)
			ctx.getProfile("fake-profile", "fake-region")
		}()
	}

	wg.Wait()

	if provider.callCount != 1 {
		t.Errorf("Expected one call to NewProfile, got %d", provider.callCount)
	}
}
