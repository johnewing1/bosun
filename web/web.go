package web

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"

	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/gorilla/mux"

	"github.com/StackExchange/scollector/opentsdb"
	"github.com/StackExchange/tsaf/sched"
)

var (
	tsdbHost  opentsdb.Host
	templates *template.Template
	router    = mux.NewRouter()
	schedule  = sched.DefaultSched
)

func init() {
	miniprofiler.Position = "bottomleft"
}

func Listen(addr, dir, host string) error {
	tsdbHost = opentsdb.Host(host)
	var err error
	templates, err = template.New("").ParseFiles(
		dir + "/templates/index.html",
	)
	if err != nil {
		log.Fatal(err)
	}
	router.Handle("/api/alerts", miniprofiler.NewHandler(Alerts))
	router.Handle("/api/expr", miniprofiler.NewHandler(Expr))
	router.Handle("/api/metric", miniprofiler.NewHandler(UniqueMetrics))
	router.Handle("/api/metric/{tagk}/{tagv}", miniprofiler.NewHandler(MetricsByTagPair))
	router.Handle("/api/query", miniprofiler.NewHandler(Query))
	router.Handle("/api/tagk/{metric}", miniprofiler.NewHandler(TagKeysByMetric))
	router.Handle("/api/tagv/{tagk}", miniprofiler.NewHandler(TagValuesByTagKey))
	router.Handle("/api/tagv/{tagk}/{metric}", miniprofiler.NewHandler(TagValuesByMetricTagKey))
	http.Handle("/", miniprofiler.NewHandler(Index))
	http.Handle("/api/", router)
	http.Handle("/partials/", http.FileServer(http.Dir(dir)))
	http.Handle("/static/", http.FileServer(http.Dir(dir)))
	log.Println("TSAF web listening on:", addr)
	log.Println("TSAF web directory:", dir)
	return http.ListenAndServe(addr, nil)
}

func Index(t miniprofiler.Timer, w http.ResponseWriter, r *http.Request) {
	err := templates.ExecuteTemplate(w, "index.html", struct {
		Includes template.HTML
	}{
		t.Includes(),
	})
	if err != nil {
		serveError(w, err)
	}
}

func serveError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func Alerts(t miniprofiler.Timer, w http.ResponseWriter, r *http.Request) {
	b, err := json.Marshal(schedule)
	if err != nil {
		serveError(w, err)
		return
	}
	w.Write(b)
}
