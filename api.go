package faustian

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/nemosupremo/faustian/storage"
	"github.com/segmentio/ksuid"
	log "github.com/sirupsen/logrus"

	_ "net/http/pprof"
)

var pipelineIDRegex = regexp.MustCompile("^[a-zA-Z0-9-_]+$")

func (c *Controller) Serve() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	routes := c.Routes()
	log.Infof("Starting HTTP Server on %v", c.Listen)
	http.ListenAndServe(c.Listen, routes)
}

func (c *Controller) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(NewLogger(nil))
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
	// r.Post("/pipelines/{pipelineID}/tasks", c.CreateTask)

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
		json.NewEncoder(w).Encode(pipelines)
	} else {
		c.Error(w, err, http.StatusInternalServerError)
	}
}

func (c *Controller) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var pipeline storage.Pipeline
	pipeline.ExecutorResources.CPU = 0.01
	pipeline.ExecutorResources.Mem = 32
	pipeline.ExecutorResources.Disk = 1

	if err := json.NewDecoder(r.Body).Decode(&pipeline); err == nil {
		if !pipelineIDRegex.MatchString(pipeline.ID) {
			c.Error(w, fmt.Errorf("Invalid ID '%v'", pipeline.ID), http.StatusBadRequest)
			return
		}
		if len(pipeline.Processors) == 0 {
			c.Error(w, fmt.Errorf("There must be at least one processor specified."), http.StatusBadRequest)
			return
		}
		type portDef struct {
			Number   int
			Protocol string
		}
		usedPorts := make(map[portDef]struct{})
		for id, proc := range pipeline.Processors {
			if !pipelineIDRegex.MatchString(id) {
				c.Error(w, fmt.Errorf("Invalid Processor ID '%v'", proc.ID), http.StatusBadRequest)
				return
			}
			if id != proc.ID {
				if proc.ID == "" {
					proc.ID = id
					pipeline.Processors[id] = proc
				} else {
					c.Error(w, fmt.Errorf("Specified ID for proc '%v' doesn't match `id` field.", id), http.StatusBadRequest)
					return
				}
			}
			for _, port := range proc.PortMapping {
				p := portDef{port.HostPort, port.Protocol}
				switch port.Protocol {
				case "tcp", "udp":

				default:
					c.Error(w, fmt.Errorf("Specified port protocol '%v' is not valid.", port.Protocol), http.StatusBadRequest)
					return
				}
				if p.Number != 0 {
					if _, ok := usedPorts[p]; ok {
						c.Error(w, fmt.Errorf("Port %d Protocol %s is defined multiple times on the host.", p.Number, p.Protocol), http.StatusBadRequest)
						return
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
			c.Error(w, errors.New("Container image must be specified."), http.StatusBadRequest)
			return
		}
		pipeline.Key = ksuid.New().String()
		pipeline.Created = time.Now()
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
		if p, ok := pipelines[pipelineID]; ok {
			created := p.Created
			{ // this block clones the pipeline
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
				op := pipelines[pipelineID]
				newInstances := p.Instances
				if p.Instances = op.Instances; cmp.Equal(p, op) {
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

					operation := func() error {
						err := c.Storage.SavePipeline(p, true)
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
			c.Error(w, fmt.Errorf("not found"), http.StatusNotFound)
		}
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
