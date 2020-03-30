package expr

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"bosun.org/cloudwatch"
	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
)

// cloudwatch defines functions for use with amazon cloudwatch api
var CloudWatch = map[string]parse.Func{

	"cw": {
		Args: []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString,
			models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet,
		Tags:          cloudwatchTagQuery,
		F:             CloudWatchQuery,
		PrefixEnabled: true,
	},
}

func parseCloudWatchResponse(req *cloudwatch.Request, s *cloudwatch.Response) ([]*Result, error) {
	const parseErrFmt = "cloudwatch ParseError (%s): %s"
	var dps Series
	if s == nil {
		return nil, fmt.Errorf(parseErrFmt, req.Metric, "empty response")
	}
	results := make([]*Result, 0)

	for _, result := range s.Raw.MetricDataResults {
		if len(result.Timestamps) == 0 {
			continue
		}

		dps = make(Series)

		for x, t := range result.Timestamps {
			dps[*t] = *result.Values[x]

		}
		r := Result{
			Value: dps,
			Group: s.TagSet[*result.Id],
		}
		results = append(results, &r)
	}

	return results, nil
}

func hasWildcardDimension(dimensions string) bool {
	return strings.Contains(dimensions, "*")
}

func parseDimensions(dimensions string) [][]cloudwatch.Dimension {
	dl := make([][]cloudwatch.Dimension, 0)
	if len(strings.TrimSpace(dimensions)) == 0 {
		return dl
	}
	dims := strings.Split(dimensions, ",")

	l := make([]cloudwatch.Dimension, 0)
	for _, row := range dims {
		dim := strings.Split(row, ":")
		l = append(l, cloudwatch.Dimension{Name: dim[0], Value: dim[1]})
	}
	dl = append(dl, l)

	return dl
}

func CloudWatchQuery(prefix string, e *State, region, namespace, metric, period, statistic, dimensions, sduration, eduration string) (*Results, error) {
	var d [][]cloudwatch.Dimension
	sd, err := opentsdb.ParseDuration(sduration)
	if err != nil {
		return nil, err
	}
	ed := opentsdb.Duration(0)
	if eduration != "" {
		ed, err = opentsdb.ParseDuration(eduration)
		if err != nil {
			return nil, err
		}
	}
	d = parseDimensions(dimensions)
	if hasWildcardDimension(dimensions) {
		lr := cloudwatch.LookupRequest{
			Region:     region,
			Namespace:  namespace,
			Metric:     metric,
			Dimensions: d,
			Profile:    prefix,
		}
		d, err = e.CloudWatchContext.LookupDimensions(&lr)
		if err != nil {
			return nil, err
		}
		if len(d) == 0 {
			return nil, fmt.Errorf("Wildcard dimension did not match any cloudwatch metrics")
		}
	}

	st := e.now.Add(-time.Duration(sd))
	et := e.now.Add(-time.Duration(ed))

	req := &cloudwatch.Request{
		Start:      &st,
		End:        &et,
		Region:     region,
		Namespace:  namespace,
		Metric:     metric,
		Period:     period,
		Statistic:  statistic,
		Dimensions: d,
		Profile:    prefix,
	}
	s, err := timeCloudwatchRequest(e, req)
	if err != nil {
		return nil, err
	}
	r := new(Results)
	results, err := parseCloudWatchResponse(req, &s)
	if err != nil {
		return nil, err
	}
	r.Results = results
	return r, nil
}

func timeCloudwatchRequest(e *State, req *cloudwatch.Request) (resp cloudwatch.Response, err error) {
	e.cloudwatchQueries = append(e.cloudwatchQueries, *req)
	b, _ := json.MarshalIndent(req, "", "  ")
	e.Timer.StepCustomTiming("cloudwatch", "query", string(b), func() {
		key := req.CacheKey()

		getFn := func() (interface{}, error) {
			return e.CloudWatchContext.Query(req)
		}
		var val interface{}
		var hit bool
		val, err, hit = e.Cache.Get(key, getFn)
		collectCacheHit(e.Cache, "cloudwatch", hit)
		resp = val.(cloudwatch.Response)

	})
	return
}

func cloudwatchTagQuery(args []parse.Node) (parse.Tags, error) {
	t := make(parse.Tags)
	n := args[5].(*parse.StringNode)
	for _, s := range strings.Split(n.Text, ",") {
		if s != "" {
			g := strings.Split(s, ":")
			if g[0] != "" {
				t[g[0]] = struct{}{}
			}
		}
	}
	return t, nil
}
