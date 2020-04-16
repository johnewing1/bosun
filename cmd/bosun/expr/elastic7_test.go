package expr

import (
	"bosun.org/opentsdb"
	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_CrossClusterSearch(t *testing.T) {

	e := State{
		now: time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC),
		Backends: &Backends{
			ElasticHosts: ElasticHosts{},
		},
		BosunProviders: &BosunProviders{
			Squelched: func(tags opentsdb.TagSet) bool {
				return false
			},
		},
		Timer: new(miniprofiler.Profile),
	}

	var tests = []struct {
		start    time.Time
		end      time.Time
		expected []string
	}{
		{time.Date(2018, 12, 30, 17, 0, 0, 0, time.UTC),
			time.Date(2019, 1, 20, 17, 0, 0, 0, time.UTC),
			[]string{"*:logstash*"},
		},
		{time.Date(2020, 2, 17, 0, 0, 0, 0, time.UTC),
			time.Date(2020, 3, 2, 0, 0, 0, 0, time.UTC),
			[]string{"*:logstash*"},
		},
	}
	/* get an instance of the weekly generator */
	results := ESCCS(&e, "@timestamp", "logstash")

	f, ok := results.Results[0].Value.(ESIndexer)
	if !ok {
		t.Errorf("Failed to get generator, wrong type")
	} else {
		for _, x := range tests {
			index_list := f.Generate(&x.start, &x.end)
			assert.Equal(t, x.expected, index_list)
		}
	}
}