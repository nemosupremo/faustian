package storage

import (
	"encoding/json"
	"path"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type zkStorage struct {
	Conn   *zk.Conn
	Prefix string
}

type zkStorageOpt func(*zkStorage) error

func WithZkConnection(c *zk.Conn) zkStorageOpt {
	return func(z *zkStorage) error {
		z.Conn = c
		return nil
	}
}

func WithZkServers(servers []string) zkStorageOpt {
	return func(z *zkStorage) error {
		if c, _, err := zk.Connect(servers, 10*time.Second, zk.WithLogInfo(false)); err == nil {
			z.Conn = c
			return nil
		} else {
			return err
		}
	}
}

func WithZkPrefix(prefix string) zkStorageOpt {
	return func(z *zkStorage) error {
		z.Prefix = prefix
		return nil
	}
}

func NewZkStorage(connectOpt zkStorageOpt, options ...zkStorageOpt) (Storage, error) {
	z := &zkStorage{}
	if err := connectOpt(z); err != nil {
		return nil, err
	}
	for _, opt := range options {
		if err := opt(z); err != nil {
			return nil, err
		}
	}
	if z.Conn == nil {
		panic("NewZkStorage called without any zookeeper connection parameters.")
	}
	if z.Prefix == "" || z.Prefix == "/" {
		z.Prefix = "/faustian"
	}
	if exists, _, err := z.Conn.Exists(path.Join(z.Prefix, "pipelines")); err == nil && !exists {
		_, err := z.Conn.Create(path.Join(z.Prefix, "pipelines"), []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if exists, _, err := z.Conn.Exists(path.Join(z.Prefix, "tasks")); err == nil && !exists {
		_, err := z.Conn.Create(path.Join(z.Prefix, "tasks"), []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return z, nil
}

func (z *zkStorage) FrameworkID() (string, error) {
	if frameworkId, _, err := z.Conn.Get(path.Join(z.Prefix, "framework-id")); err == nil {
		return string(frameworkId), nil
	} else if err == zk.ErrNoNode {
		return "", nil
	} else {
		return "", err
	}
}

func (z *zkStorage) SetFrameworkID(id string) error {
	if exists, _, err := z.Conn.Exists(path.Join(z.Prefix, "framework-id")); err == nil && !exists {
		_, err := z.Conn.Create(path.Join(z.Prefix, "framework-id"), []byte(id), 0, zk.WorldACL(zk.PermAll))
		return err
	} else if err == nil {
		_, err := z.Conn.Set(path.Join(z.Prefix, "framework-id"), []byte(id), -1)
		return err
	} else {
		return err
	}
}

func (z *zkStorage) Tasks(watch bool) ([]string, <-chan zk.Event, error) {
	if watch {
		tasks, _, ch, err := z.Conn.ChildrenW(path.Join(z.Prefix, "tasks"))
		if err == zk.ErrNoNode {
			_, _, ch, err := z.Conn.ExistsW(path.Join(z.Prefix, "tasks"))
			return []string{}, ch, err
		}
		return tasks, ch, err
	}
	tasks, _, err := z.Conn.Children(path.Join(z.Prefix, "tasks"))
	if err == zk.ErrNoNode {
		return []string{}, nil, nil
	}
	return tasks, nil, err
}

func (z *zkStorage) SaveTasks(tasks map[string]*Task) error {
	if b, err := json.Marshal(tasks); err == nil {
		_, err := z.Conn.Set(path.Join(z.Prefix, "tasks"), b, -1)
		return err
	} else {
		return err
	}
}

func (z *zkStorage) Pipelines(watch bool) (map[string]Pipeline, <-chan zk.Event, error) {
	var pipelines []byte
	var ch <-chan zk.Event
	var err error
	if watch {
		pipelines, _, ch, err = z.Conn.GetW(path.Join(z.Prefix, "pipelines"))
	}
	pipelines, _, err = z.Conn.Get(path.Join(z.Prefix, "pipelines"))

	if err == zk.ErrNoNode {
		if watch {
			_, _, ch, err = z.Conn.ExistsW(path.Join(z.Prefix, "pipelines"))
		}
		return map[string]Pipeline{}, ch, err
	} else if err != nil {
		return nil, nil, err
	}
	var p map[string]Pipeline
	if err := json.Unmarshal(pipelines, &p); err == nil {
		return p, ch, nil
	} else {
		return nil, nil, err
	}
}

func (z *zkStorage) SavePipeline(pipeline Pipeline, update bool) error {
	var pipelines map[string]Pipeline
	p, s, err := z.Conn.Get(path.Join(z.Prefix, "pipelines"))

	exists := true
	if err == nil {
		if err := json.Unmarshal(p, &pipelines); err == nil {
			if !update {
				if _, ok := pipelines[pipeline.ID]; ok {
					return ErrPipelineExists
				}
			}
			pipelines[pipeline.ID] = pipeline
		} else {
			return err
		}
	} else if err == zk.ErrNoNode {
		if update {
			return ErrPipelineNotExists
		}
		pipelines = map[string]Pipeline{pipeline.ID: pipeline}
		exists = false
	} else {
		return err
	}

	if b, err := json.Marshal(pipelines); err == nil {
		ops := []interface{}{}

		if tasks, _, err := z.Conn.Children(path.Join(z.Prefix, "tasks")); err == nil {
			for _, task := range tasks {
				if strings.HasPrefix(task, pipeline.ID+"$") {
					ops = append(ops, &zk.DeleteRequest{
						Path: path.Join(z.Prefix, "tasks", task),
					})
				}
			}
		} else if err == zk.ErrNoNode {

		} else {
			return err
		}

		if exists {
			ops = append(ops, &zk.SetDataRequest{
				Path:    path.Join(z.Prefix, "pipelines"),
				Data:    b,
				Version: s.Version,
			})
		} else {
			ops = append(ops, &zk.CreateRequest{
				Path: path.Join(z.Prefix, "pipelines"),
				Data: b,
				Acl:  zk.WorldACL(zk.PermAll),
			})
		}
		pipelineTasks := pipeline.GenerateTasks()
		for _, task := range pipelineTasks {
			b, _ := json.Marshal(task)
			ops = append(ops, &zk.CreateRequest{
				Path: path.Join(z.Prefix, "tasks", string(task.ID)),
				Data: b,
				Acl:  zk.WorldACL(zk.PermAll),
			})
		}

		if _, err = z.Conn.Multi(ops...); err == zk.ErrBadVersion || err == zk.ErrNoNode {
			return ErrConflict
		} else {
			return err
		}
	} else {
		return err
	}
}

func (z *zkStorage) ResizePipeline(pipelineID string, size int) error {
	var pipelines map[string]Pipeline
	var pipeline Pipeline
	var oldSize int
	p, s, err := z.Conn.Get(path.Join(z.Prefix, "pipelines"))

	exists := true
	if err == nil {
		if err := json.Unmarshal(p, &pipelines); err == nil {
			var ok bool
			pipeline, ok = pipelines[pipelineID]
			if !ok {
				return ErrPipelineNotExists
			}
			if pipeline.Instances == size {
				return nil
			}
			oldSize = pipeline.Instances
			pipeline.Instances = size
			pipelines[pipelineID] = pipeline
		} else {
			return err
		}
	} else if err == zk.ErrNoNode {
		return ErrPipelineNotExists
	} else {
		return err
	}

	if b, err := json.Marshal(pipelines); err == nil {
		ops := []interface{}{}

		if exists {
			ops = append(ops, &zk.SetDataRequest{
				Path:    path.Join(z.Prefix, "pipelines"),
				Data:    b,
				Version: s.Version,
			})
		} else {
			ops = append(ops, &zk.CreateRequest{
				Path: path.Join(z.Prefix, "pipelines"),
				Data: b,
				Acl:  zk.WorldACL(zk.PermAll),
			})
		}

		if oldSize > size {
			// shrink pipeline
			j := 0
			if tasks, _, err := z.Conn.Children(path.Join(z.Prefix, "tasks")); err == nil {
				for _, task := range tasks {
					if strings.HasPrefix(task, pipelineID+"$") {
						ops = append(ops, &zk.DeleteRequest{
							Path: path.Join(z.Prefix, "tasks", task),
						})
						j++
						if j >= (oldSize - size) {
							break
						}
					}
				}
			} else if err == zk.ErrNoNode {

			} else {
				return err
			}
		} else if oldSize < size {
			// grow pipeline
			pipelineTasks := pipeline.GenerateTasks()
			pipelineTasks = pipelineTasks[:(size - oldSize)]
			for _, task := range pipelineTasks {
				b, _ := json.Marshal(task)
				ops = append(ops, &zk.CreateRequest{
					Path: path.Join(z.Prefix, "tasks", string(task.ID)),
					Data: b,
					Acl:  zk.WorldACL(zk.PermAll),
				})
			}
		}

		if _, err = z.Conn.Multi(ops...); err == zk.ErrBadVersion || err == zk.ErrNoNode {
			return ErrConflict
		} else {
			return err
		}
	} else {
		return err
	}
}

func (z *zkStorage) DeletePipeline(pipelineID string) error {
	p, s, err := z.Conn.Get(path.Join(z.Prefix, "pipelines"))

	if err == nil {
		var pipelines map[string]Pipeline
		if err := json.Unmarshal(p, &pipelines); err == nil {
			if _, ok := pipelines[pipelineID]; ok {
				delete(pipelines, pipelineID)
				if b, err := json.Marshal(pipelines); err == nil {

					ops := []interface{}{
						&zk.SetDataRequest{
							Path:    path.Join(z.Prefix, "pipelines"),
							Data:    b,
							Version: s.Version,
						},
					}

					if tasks, _, err := z.Conn.Children(path.Join(z.Prefix, "tasks")); err == nil {
						for _, task := range tasks {
							if strings.HasPrefix(task, pipelineID+"$") {
								ops = append(ops, &zk.DeleteRequest{
									Path: path.Join(z.Prefix, "tasks", task),
								})
							}
						}
					} else if err == zk.ErrNoNode {

					} else {
						return err
					}

					if _, err := z.Conn.Multi(ops...); err == zk.ErrBadVersion || err == zk.ErrNoNode {
						return ErrConflict
					} else {
						return err
					}
				} else {
					return err
				}
			} else {
				return nil
			}
		} else {
			return err
		}
	} else if err == zk.ErrNoNode {
		return nil
	} else {
		return err
	}
}

func (z *zkStorage) LastAutoscale() (time.Time, error) {
	if la, _, err := z.Conn.Get(path.Join(z.Prefix, "last-autoscale")); err == nil {
		var t time.Time
		if err := t.UnmarshalText(la); err == nil {
			return t, nil
		} else {
			return t, err
		}
	} else if err == zk.ErrNoNode {
		return time.Now(), nil
	} else {
		return time.Time{}, err
	}
}

func (z *zkStorage) SetLastAutoscale(t time.Time) error {
	b, _ := t.MarshalText()
	if exists, _, err := z.Conn.Exists(path.Join(z.Prefix, "last-autoscale")); err == nil && !exists {
		_, err := z.Conn.Create(path.Join(z.Prefix, "last-autoscale"), b, 0, zk.WorldACL(zk.PermAll))
		return err
	} else if err == nil {
		_, err := z.Conn.Set(path.Join(z.Prefix, "last-autoscale"), b, -1)
		return err
	} else {
		return err
	}
}

func (z *zkStorage) GetZkConn() *zk.Conn {
	return z.Conn
}
