package storage

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Storage interface {
	FrameworkID() (string, error)
	SetFrameworkID(string) error

	Tasks(bool) ([]string, <-chan zk.Event, error)
	SaveTasks(map[string]*Task) error

	Pipelines(bool) (map[string]Pipeline, <-chan zk.Event, error)
	SavePipeline(Pipeline, bool) error
	ResizePipeline(string, int) error
	DeletePipeline(string) error

	LastAutoscale() (time.Time, error)
	SetLastAutoscale(time.Time) error
}

func NewStorageBackend(uri string) (Storage, error) {
	if uri, err := url.Parse(uri); err == nil {
		switch uri.Scheme {
		case "zk":
			hosts := zk.FormatServers(strings.Split(uri.Host, ","))
			if backend, err := NewZkStorage(WithZkServers(hosts), WithZkPrefix(uri.Path)); err == nil {
				return backend, nil
			} else {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Unsupported backend type '%s'", uri.Scheme)
		}
	} else {
		return nil, err
	}
}
