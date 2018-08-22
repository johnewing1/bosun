// Package cloudwatch defines structures for interacting with Cloudwatch Metrics.
package cloudwatch // import "bosun.org/cloudwatch"

import (
	"fmt"
	"strconv"
	"time"

	"bosun.org/slog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	cwi "github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

const requestErrFmt = "cloudwatch RequestError (%s): %s"

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

// Perform a query to cloudwatch
func (r *Request) Query(svc cwi.CloudWatchAPI) (Response, error) {

	var response Response
	aws_period, _ := strconv.ParseInt(r.Period, 10, 64)

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
		Period:     &aws_period,
		Statistics: []*string{aws.String(r.Statistic)},
		Namespace:  aws.String(r.Namespace),
		Dimensions: dimensions,
	}
	resp, err := svc.GetMetricStatistics(search)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		slog.Error(err.Error())
		return response, err
	}
	response.Raw = *resp
	return response, nil
}

// Context is the interface for querying cloudwatch.
type Context interface {
	Query(*Request) (Response, error)
}

type Config struct {
	Profiles map[string]cwi.CloudWatchAPI
}

func NewConfig() *Config {
	c := new(Config)
	c.Profiles = make(map[string]cwi.CloudWatchAPI)
	return c
}

// Query performs a cloudwatch request to aws.
func (c Config) Query(r *Request) (Response, error) {
	var profile string
	var conf aws.Config
	if r.Profile == "default" {
		profile = "bosun-default"
	} else {
		profile = "user-" + r.Profile
	}
	// if the session hasn't already been initalised for this profile create a new one
	if c.Profiles[profile] == nil {
		conf.Credentials = credentials.NewSharedCredentials("", r.Profile)
		conf.Region = aws.String(r.Region)
		c.Profiles[profile] = cw.New(session.New(&conf))
	}

	return r.Query(c.Profiles[profile])
}
