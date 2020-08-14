package zerostor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/gateway/zerostor/config"
	"github.com/minio/minio/cmd/gateway/zerostor/repair"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/threefoldtech/0-stor/client/datastor/pipeline/storage"
	"gopkg.in/yaml.v2"
)

var (
	newLine    = []byte{'\n'}
	activeJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "minio",
		Subsystem: "healer",
		Name:      "running_jobs",
		Help:      "number of running jobs",
	})
)

// HealerAPI type
type HealerAPI struct {
	listen     string
	cfg        ConfigManager
	jobs       map[string]HealJob
	m          sync.RWMutex
	isReadOnly func() bool
}

// NewHealerAPI creates a new healer api that listens on thie address
func NewHealerAPI(listen string, cfg ConfigManager, isReadOnly func() bool) *HealerAPI {
	return &HealerAPI{
		listen:     listen,
		cfg:        cfg,
		jobs:       make(map[string]HealJob),
		isReadOnly: isReadOnly,
	}
}

func (a *HealerAPI) cancelJob(writer http.ResponseWriter, request *http.Request) {
	a.m.RLock()
	defer a.m.RUnlock()

	id := mux.Vars(request)["id"]
	job, ok := a.jobs[id]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	job.Cancel()
	delete(a.jobs, id)
	writer.WriteHeader(http.StatusOK)
}

func (a *HealerAPI) getJobs(writer http.ResponseWriter, request *http.Request) {
	a.m.RLock()
	defer a.m.RUnlock()

	writer.Header().Add("application/content-type", "json")
	writer.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(writer)
	enc.Encode(a.jobs)
}

func (a *HealerAPI) repairObject(writer http.ResponseWriter, request *http.Request) {
	bucket := mux.Vars(request)["bucket"]
	// TODO: find another way to get the unmatched part of the url.
	object := strings.TrimPrefix(request.URL.Path, fmt.Sprintf("/repair/%s/", bucket))

	id, err := uuid.NewRandom()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("failed to generate a job id"))
		return
	}

	jobType := ForegroundJob
	if request.FormValue("bg") != "" {
		jobType = BackgroundJob
	}

	location := fmt.Sprintf("%s/%s", bucket, object)

	job := newJob(jobType, id.String(), location, writer, request)

	a.m.Lock()
	a.jobs[id.String()] = job
	a.m.Unlock()

	client := a.cfg.GetClient()
	defer client.Close()
	metaMgr := a.cfg.GetMeta()
	defer metaMgr.Close()

	healer := repair.NewHealer(metaMgr, client.Inner())

	checker := healer.CheckAndRepair
	if request.FormValue("dry-run") != "" {
		checker = healer.Dryrun
	}

	status := healer.CheckObject(job.Context(), checker, bucket, object)
	writer.WriteHeader(200)
	job.Process(status)
}

func (a *HealerAPI) repairBucket(writer http.ResponseWriter, request *http.Request) {
	// if the bucket is empty this will cause the scan function
	// to go over all buckets, hence scanning entire instance
	bucket := mux.Vars(request)["bucket"]

	id, err := uuid.NewRandom()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("failed to generate a job id"))
		return
	}

	jobType := ForegroundJob
	if request.FormValue("bg") != "" {
		jobType = BackgroundJob
	}

	job := newJob(jobType, id.String(), bucket, writer, request)

	a.m.Lock()
	a.jobs[id.String()] = job
	a.m.Unlock()

	client := a.cfg.GetClient()
	defer client.Close()
	metaMgr := a.cfg.GetMeta()
	defer metaMgr.Close()

	healer := repair.NewHealer(metaMgr, client.Inner())

	checker := healer.CheckAndRepair
	if request.FormValue("dry-run") != "" {
		checker = healer.Dryrun
	}

	status := healer.CheckBucket(job.Context(), checker, bucket)
	writer.WriteHeader(200)
	job.Process(status)
}

func (a *HealerAPI) updateConfig(writer http.ResponseWriter, request *http.Request) {
	cfg := config.Config{}

	defer request.Body.Close()
	if err := json.NewDecoder(request.Body).Decode(&cfg); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("bad format for configuration"))
		return
	}

	// persist the config on disk
	p := "/data/minio.yaml" //from entrypoint.go:161
	f, err := os.Create(p)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte(fmt.Sprintf("failed to create config file: %v", err)))
		return
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte(fmt.Sprintf("failed to create config file: %v", err)))
	}

	// then hot reload
	if err := a.cfg.Reload(cfg); err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(fmt.Sprintf("failed to reload configuration: %v", err)))
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (a *HealerAPI) setup() *mux.Router {
	router := mux.NewRouter()

	// middleware function to disable healer API if minio is running in slave mode
	mw := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if a.isReadOnly() {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			h.ServeHTTP(w, r)
		})
	}

	router.Use(mw)
	router.HandleFunc("/repair", a.repairBucket).Methods(http.MethodPost)
	router.HandleFunc("/repair/{bucket}", a.repairBucket).Methods(http.MethodPost)
	router.PathPrefix("/repair/{bucket}/").HandlerFunc(a.repairObject).Methods(http.MethodPost)
	router.HandleFunc("/jobs", a.getJobs).Methods(http.MethodGet)
	router.HandleFunc("/jobs/{id}", a.cancelJob).Methods(http.MethodDelete)
	router.HandleFunc("/config", a.updateConfig).Methods(http.MethodPost)
	return router
}

// Start healer api
func (a *HealerAPI) Start() {
	listen := a.listen
	if listen == "" {
		listen = "0.0.0.0:9010"
	}

	server := http.Server{
		Addr:    listen,
		Handler: a.setup(),
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal("failed to start healer API")
	}
}

// JobType type of the healer job
type JobType string

const (
	//ForegroundJob type
	ForegroundJob JobType = "foreground"
	//BackgroundJob types
	BackgroundJob JobType = "background"
)

// HealJob interface
type HealJob interface {
	Cancel()
	Context() context.Context
	Process(status <-chan repair.Status)
}

func newJob(t JobType, id, location string, writer http.ResponseWriter, request *http.Request) HealJob {
	switch t {
	case BackgroundJob:
		return newBgJob(id, location, writer, request)
	case ForegroundJob:
		return newFgJob(id, location, writer, request)
	default:
		panic("unknown job type")
	}
}

type fgHealJob struct {
	ctx context.Context
	w   http.ResponseWriter
	id  string

	Location string  `json:"location"`
	Type     JobType `json:"type"`
	Running  bool    `json:"running"`
}

func newFgJob(id, location string, w http.ResponseWriter, r *http.Request) HealJob {
	j := &fgHealJob{
		ctx: r.Context(),
		w:   w,
		id:  id,

		Location: location,
		Type:     ForegroundJob,
	}

	return j
}

func (j *fgHealJob) Cancel() {
	// do nothing. it cancels by closing the connection
}

func (j *fgHealJob) Context() context.Context {
	return j.ctx
}

func (j *fgHealJob) Process(ch <-chan repair.Status) {
	activeJobs.Inc()
	defer activeJobs.Dec()
	j.Running = true
	j.w.Write([]byte(fmt.Sprintf("job: %s\n", j.id)))
	for status := range ch {
		j.w.Write([]byte(status.String()))
		j.w.Write(newLine)
	}
	j.Running = false
}

type bgHealJob struct {
	id     string
	cancel func()
	ctx    context.Context
	w      http.ResponseWriter

	Location string  `json:"location"`
	Type     JobType `json:"type"`
	Blobs    struct {
		Checked  uint64 `json:"checked"`
		Optimal  uint64 `json:"optimal"`
		Valid    uint64 `json:"valid"`
		Invalid  uint64 `json:"invalid"`
		Repaired uint64 `json:"repaired"`
		Errors   uint64 `json:"errors"`
	} `json:"blobs"`

	Objects struct {
		Checked uint64 `json:"checked"`
	} `json:"objects"`

	Running bool `json:"running"`
}

func newBgJob(id, location string, w http.ResponseWriter, r *http.Request) HealJob {

	ctx, cancel := context.WithCancel(context.Background())
	return &bgHealJob{
		id:     id,
		cancel: cancel,
		ctx:    ctx,
		w:      w,

		Location: location,
		Type:     BackgroundJob,
	}
}

func (j *bgHealJob) Context() context.Context {
	return j.ctx
}

// Cancel job
func (j *bgHealJob) Cancel() {
	if j.cancel != nil {
		j.cancel()
	}
}

func (j *bgHealJob) Process(ch <-chan repair.Status) {
	j.w.Write([]byte(j.id))

	go func() {
		activeJobs.Inc()
		defer activeJobs.Dec()

		for status := range ch {
			switch result := status.(type) {
			case *repair.BlobStatus:
				j.Blobs.Checked++
				if result.Repaired {
					j.Blobs.Repaired++
				}
				if result.Error() != nil {
					j.Blobs.Errors++
				}
				switch result.Status {
				case storage.CheckStatusInvalid:
					j.Blobs.Invalid++
				case storage.CheckStatusValid:
					j.Blobs.Valid++
				case storage.CheckStatusOptimal:
					j.Blobs.Optimal++
				}
			case *repair.ObjectStatus:
				j.Objects.Checked++
			}
		}
	}()
}
