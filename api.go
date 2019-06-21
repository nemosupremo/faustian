package faustian

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-chi/chi"
	"github.com/go-chi/cors"
	"github.com/google/go-cmp/cmp"
	"github.com/nemosupremo/faustian/storage"
	"github.com/segmentio/ksuid"
	log "github.com/sirupsen/logrus"

	_ "net/http/pprof"
)

var pipelineIDRegex = regexp.MustCompile("^[a-zA-Z0-9-_]+$")

func (c *Controller) Serve() {

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	routes := c.Routes()
	log.Infof("Starting HTTP Server on %v", c.Listen)
	http.ListenAndServe(c.Listen, routes)
}

func (c *Controller) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(NewLogger(nil))
	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "Origin", "Content-Length", "sessionId"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}).Handler)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			next.ServeHTTP(w, r)
		})
	})

	r.Get("/ping", c.StatusPing)

	r.Get("/pipelines", c.Pipelines)
	r.Post("/pipelines", c.CreatePipeline)

	r.Get("/pipelines/{pipelineID}", c.Pipeline)
	r.Patch("/pipelines/{pipelineID}", c.UpdatePipeline)
	r.Delete("/pipelines/{pipelineID}", c.DeletePipeline)

	// r.Get("/tasks", c.Tasks)
	// r.Get("/pipelines/{pipelineID}/tasks", c.Tasks)
	return r
}

func (c *Controller) Error(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{code, err.Error()})
}

func (c *Controller) StatusPing(w http.ResponseWriter, r *http.Request) {
	pong := c.Ping()
	json.NewEncoder(w).Encode(pong)
}

func (c *Controller) Pipelines(w http.ResponseWriter, r *http.Request) {
	if pipelines, _, err := c.Storage.Pipelines(false); err == nil {
		var r []storage.Pipeline
		if tasks, err := c.Storage.TasksInfo(); err == nil {
			for _, task := range tasks {
				if p, ok := pipelines[task.PipelineID]; ok {
					t := *task
					t.Ok = len(t.Processes) > 0
					t.Failing = false
					for _, proc := range t.Processes {
						switch proc.Status {
						case "TASK_RUNNING", "TASK_KILLING":

						case "", "TASK_KILLED", "TASK_STAGING", "TASK_STARTING":
							t.Ok = false
						default:
							t.Ok = false
							t.Failing = true
						}
					}
					p.Tasks = append(p.Tasks, t)
					pipelines[task.PipelineID] = p
				}
			}
			for _, pipe := range pipelines {
				r = append(r, pipe)
			}
			sort.Slice(r, func(i, j int) bool { return r[i].Created.Before(r[j].Created) })
			json.NewEncoder(w).Encode(r)
		} else {
			c.Error(w, err, http.StatusInternalServerError)
		}
	} else {
		c.Error(w, err, http.StatusInternalServerError)
	}
}

func validatePipeline(pipeline *storage.Pipeline) error {
	if !pipelineIDRegex.MatchString(pipeline.ID) {
		return fmt.Errorf("Invalid ID '%v'", pipeline.ID)
	}
	if len(pipeline.Processors) == 0 {
		return fmt.Errorf("There must be at least one processor specified.")
	}
	type portDef struct {
		Number   int
		Protocol string
	}
	usedPorts := make(map[portDef]struct{})
	for id, proc := range pipeline.Processors {
		if !pipelineIDRegex.MatchString(id) {
			return fmt.Errorf("Invalid Processor ID '%v'", proc.ID)

		}
		if id != proc.ID {
			if proc.ID == "" {
				proc.ID = id
				pipeline.Processors[id] = proc
			} else {
				return fmt.Errorf("Specified ID for proc '%v' doesn't match `id` field.", id)
			}
		}
		for _, port := range proc.PortMapping {
			p := portDef{port.HostPort, port.Protocol}
			switch port.Protocol {
			case "tcp", "udp":

			default:
				return fmt.Errorf("Specified port protocol '%v' is not valid.", port.Protocol)

			}
			if p.Number != 0 {
				if _, ok := usedPorts[p]; ok {
					return fmt.Errorf("Port %d Protocol %s is defined multiple times on the host.", p.Number, p.Protocol)

				} else {
					usedPorts[p] = struct{}{}
				}
			}
		}
		if proc.KillGracePeriod == 0 {
			proc.KillGracePeriod = 30
			pipeline.Processors[id] = proc
		}
	}
	if pipeline.Container.Image == "" {
		return errors.New("Container image must be specified.")
	}
	return nil
}

func (c *Controller) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var pipeline storage.Pipeline
	pipeline.ExecutorResources.CPU = 0.01
	pipeline.ExecutorResources.Mem = 32
	pipeline.ExecutorResources.Disk = 1

	if err := json.NewDecoder(r.Body).Decode(&pipeline); err == nil {
		if err := validatePipeline(&pipeline); err != nil {
			c.Error(w, err, http.StatusBadRequest)
			return
		}

		pipeline.Key = ksuid.New().String()
		pipeline.Created = time.Now()
		pipeline.Updated = pipeline.Created
		operation := func() error {
			err := c.Storage.SavePipeline(pipeline, false)
			if err != storage.ErrConflict {
				err = backoff.Permanent(err)
			}
			return err
		}
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = 60 * time.Second
		if err := backoff.Retry(operation, b); err == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			c.Error(w, err, http.StatusInternalServerError)
		}
	} else {
		c.Error(w, err, http.StatusUnprocessableEntity)
	}
}

func (c *Controller) Pipeline(w http.ResponseWriter, r *http.Request) {
	if pipelines, _, err := c.Storage.Pipelines(false); err == nil {
		pipelineID := chi.URLParam(r, "pipelineID")
		if p, ok := pipelines[pipelineID]; ok {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(p)
		} else {
			c.Error(w, fmt.Errorf("not found"), http.StatusNotFound)
		}
	} else {
		c.Error(w, err, http.StatusInternalServerError)
	}
}

func (c *Controller) UpdatePipeline(w http.ResponseWriter, r *http.Request) {
	if pipelines, _, err := c.Storage.Pipelines(false); err == nil {
		pipelineID := chi.URLParam(r, "pipelineID")
		p, ok := pipelines[pipelineID]
		creatingPipeline := !ok
		if !ok {
			p.ExecutorResources.CPU = 0.01
			p.ExecutorResources.Mem = 32
			p.ExecutorResources.Disk = 1
			p.Created = time.Now()
		}
		created := p.Created
		currentEnv := p.Environment
		{
			// this block "deep" clones the pipeline, so that when we
			// unmarshal into p, we don't merge our pointer values

			// set environment to nil so we can detect if this field is
			// overwritten
			p.Environment = nil
			if b, err := json.Marshal(p); err == nil {
				p = storage.Pipeline{}
				if err := json.Unmarshal(b, &p); err != nil {
					c.Error(w, err, http.StatusInternalServerError)
					return
				}
			} else {
				c.Error(w, err, http.StatusInternalServerError)
				return
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err == nil {
			if p.Environment == nil {
				// if the caller didn't provide an environment variable
				// use the previous environment
				p.Environment = currentEnv
			}
			op := pipelines[pipelineID]
			newInstances := p.Instances
			if p.Instances = op.Instances; cmp.Equal(p, op) && !creatingPipeline {
				log.Debugf("[Pipeline: %s] Pipelines were equal when given same instances.\n%+v, %+v", pipelineID, op, p)
				p.Instances = newInstances

				operation := func() error {
					err := c.Storage.ResizePipeline(p.ID, newInstances)
					if err != storage.ErrConflict {
						err = backoff.Permanent(err)
					}
					return err
				}
				b := backoff.NewExponentialBackOff()
				b.MaxElapsedTime = 60 * time.Second
				if err := backoff.Retry(operation, b); err == nil {
					w.WriteHeader(http.StatusNoContent)
				} else {
					c.Error(w, err, http.StatusInternalServerError)
				}
			} else {
				p.ID = pipelineID
				p.Key = ksuid.New().String()
				p.Created = created
				p.Updated = time.Now()

				if err := validatePipeline(&p); err != nil {
					c.Error(w, err, http.StatusBadRequest)
					return
				}

				operation := func() error {
					err := c.Storage.SavePipeline(p, !creatingPipeline)
					if err != storage.ErrConflict {
						err = backoff.Permanent(err)
					}
					return err
				}
				b := backoff.NewExponentialBackOff()
				b.MaxElapsedTime = 60 * time.Second
				if err := backoff.Retry(operation, b); err == nil {
					w.WriteHeader(http.StatusNoContent)
				} else {
					c.Error(w, err, http.StatusInternalServerError)
				}
			}
		} else {
			c.Error(w, err, http.StatusUnprocessableEntity)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(p)
	} else {
		c.Error(w, err, http.StatusInternalServerError)
	}
}

func (c *Controller) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	operation := func() error {
		err := c.Storage.DeletePipeline(pipelineID)
		if err != storage.ErrConflict {
			err = backoff.Permanent(err)
		}
		return err
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 60 * time.Second
	if err := backoff.Retry(operation, b); err == nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		c.Error(w, err, http.StatusInternalServerError)
	}
}
