package expr

import (
	"bosun.org/opentsdb"
	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_Weeklies(t *testing.T) {

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
		/* turn of year */
		{time.Date(2018, 12, 30, 17, 0, 0, 0, time.UTC),
			time.Date(2019, 1, 20, 17, 0, 0, 0, time.UTC),
			[]string{"logstash-2018.52", "logstash-2019.01", "logstash-2019.02", "logstash-2019.03"},
		},
		/* sunday to monday */
		{time.Date(2019, 4, 7, 0, 0, 0, 0, time.UTC),
			time.Date(2019, 4, 13, 0, 0, 0, 0, time.UTC),
			[]string{"logstash-2019.14", "logstash-2019.15"},
		},
		/* monday to sunday */
		{time.Date(2019, 4, 8, 0, 0, 0, 0, time.UTC),
			time.Date(2019, 4, 14, 0, 0, 0, 0, time.UTC),
			[]string{"logstash-2019.15"},
		},
		// week padding
		{time.Date(2020, 2, 17, 0, 0, 0, 0, time.UTC),
			time.Date(2020, 3, 2, 0, 0, 0, 0, time.UTC),
			[]string{"logstash-2020.08", "logstash-2020.09", "logstash-2020.10"},
		},
	}
	/* get an instance of the weekly generator */
	results, err := ESWeekly(&e, "@timestamp", "logstash")
	if err != nil {
		t.Errorf("Failed to get generator: %s ", err)
	} else if results.Results[0].Value != nil {

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
}
