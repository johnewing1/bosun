// Package cloudwatch defines structures for interacting with Cloudwatch Metrics.
package cloudwatch // import "bosun.org/cloudwatch"

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"bosun.org/slog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	cwi "github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

const requestErrFmt = "cloudwatch RequestError (%s): %s"

var (
	once    sync.Once
	context Context
)

// Request holds query objects. Currently only absolute times are supported.
type Request struct {
	Start      *time.Time
	End        *time.Time
	Region     string
	Namespace  string
	Metric     string
	Period     string
	Statistic  string
	Dimensions []Dimension
	Profile    string
}

type Response struct {
	Raw cw.GetMetricStatisticsOutput
}

type Series struct {
	Datapoints []DataPoint
	Label      string
}

type DataPoint struct {
	Aggregator string
	Timestamp  string
	Unit       string
}

type Dimension struct {
	Name  string
	Value string
}

func (d Dimension) String() string {
	return fmt.Sprintf("%s:%s", d.Name, d.Value)
}

func (r *Request) CacheKey() string {
	return fmt.Sprintf("cloudwatch-%d-%d-%s-%s-%s-%s-%s-%s-%s",
		r.Start.Unix(),
		r.End.Unix(),
		r.Region,
		r.Namespace,
		r.Metric,
		r.Period,
		r.Statistic,
		r.Dimensions,
		r.Profile,
	)
}

// Context is the interface for querying CloudWatch.
type Context interface {
	Query(*Request) (Response, error)
}

type cloudWatchContext struct {
	profileProvider ProfileProvider
	profiles        map[string]cwi.CloudWatchAPI
	profilesLock    sync.RWMutex
}

type ProfileProvider interface {
	NewProfile(name, region string) cwi.CloudWatchAPI
}

type profileProvider struct{}

func (p profileProvider) NewProfile(name, region string) cwi.CloudWatchAPI {
	enableVerboseLogging := true
	conf := aws.Config{
		CredentialsChainVerboseErrors: &enableVerboseLogging,
		Region:                        aws.String(region),
	}

	if name != "default" {
		conf.Credentials = credentials.NewSharedCredentials("", name)
	}

	return cw.New(session.New(&conf))
}

// getProfile returns a previously created profile or creates a new one for the given profile name and region
func (c *cloudWatchContext) getProfile(awsProfileName, region string) cwi.CloudWatchAPI {
	var fullProfileName string

	if awsProfileName == "default" {
		fullProfileName = "bosun-default"
	} else {
		fullProfileName = fmt.Sprintf("user-%s", awsProfileName)
	}

	fullProfileName = fmt.Sprintf("%s-%s", fullProfileName, region)

	// We don't want to concurrently modify the c.profiles map
	c.profilesLock.Lock()
	defer c.profilesLock.Unlock()

	if cwAPI, ok := c.profiles[fullProfileName]; ok {
		return cwAPI
	}

	cwAPI := c.profileProvider.NewProfile(awsProfileName, region)
	c.profiles[fullProfileName] = cwAPI

	return cwAPI
}

func GetContext() Context {
	return GetContextWithProvider(profileProvider{})
}

func GetContextWithProvider(p ProfileProvider) Context {
	once.Do(func() {
		context = &cloudWatchContext{
			profileProvider: p,
			profiles:        make(map[string]cwi.CloudWatchAPI),
		}
	})
	return context
}

// Query performs a CloudWatch request to aws.
func (c cloudWatchContext) Query(r *Request) (Response, error) {
	api := c.getProfile(r.Profile, r.Region)

	var response Response
	awsPeriod, _ := strconv.ParseInt(r.Period, 10, 64)

	dimensions := make([]*cw.Dimension, 0)
	for _, i := range r.Dimensions {
		dimensions = append(dimensions, &cw.Dimension{
			Name:  aws.String(i.Name),
			Value: aws.String(i.Value),
		})
	}

	search := &cw.GetMetricStatisticsInput{
		StartTime:  aws.Time(*r.Start),
		EndTime:    aws.Time(*r.End),
		MetricName: aws.String(r.Metric),
		Period:     &awsPeriod,
		Statistics: []*string{aws.String(r.Statistic)},
		Namespace:  aws.String(r.Namespace),
		Dimensions: dimensions,
	}
	resp, err := api.GetMetricStatistics(search)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		slog.Error(err.Error())
		return response, err
	}
	response.Raw = *resp
	return response, nil
}
