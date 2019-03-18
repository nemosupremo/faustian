package mesos

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nemosupremo/faustian/mesos/msg"
	"github.com/nemosupremo/faustian/mesos/recordio"
	"github.com/segmentio/ksuid"
	log "github.com/sirupsen/logrus"
)

const EXECUTOR_CPU = 0.01
const EXECUTOR_MEM = 32
const EXECUTOR_DISK = 1

var errNotSubscribed = errors.New("The scheduler is not currently subscribed.")
var errAborted = errors.New("Aborted mesos connection due to event.")

type Scheduler interface {
	HeartbeatInterval() time.Duration
	IsSubscribed() bool

	Subscribe() error
	Teardown() error
	Disconnect()

	Accept(msg.Offer, Resources, []msg.TaskInfo) error
	Decline([]string) error

	Suppress([]string) error
	Revive(string) error
	// Shutdown()
	Kill(string, string) error

	Acknowledge(string, string, string) error
	AcknowledgeOperationStatus(string, string, string, string) error

	Reconcile([]string) error
	// ReconcileOperations()

	// Message()
	// Request()
}

type FrameworkIDStore interface {
	FrameworkID() (string, error)
	SetFrameworkID(string) error
}

type EventCallback func(Scheduler, msg.Event)

type scheduler struct {
	Name     string
	User     string
	Roles    []string
	Hostname string
	WebUI    string
	Masters  struct {
		Protocol string
		Hosts    []string
		Host     string
	}

	Client *http.Client
	ID     FrameworkIDStore

	Subscribed     int32
	Callbacks      []EventCallback
	SubscriberWait sync.WaitGroup
	SubControl     struct {
		sync.Mutex
		C chan struct{}
	}

	MesosStreamId     string
	heartbeatInterval time.Duration
}

type SchedulerConfig struct {
	Name        string
	User        string
	Roles       []string
	Hostname    string
	WebUI       string
	MesosMaster string
	Callbacks   []EventCallback
	FrameworkID FrameworkIDStore
}

func NewScheduler(frameworkId FrameworkIDStore, conf SchedulerConfig) (Scheduler, error) {
	s := &scheduler{
		Name:      conf.Name,
		User:      conf.User,
		Roles:     conf.Roles,
		Hostname:  conf.Hostname,
		WebUI:     conf.WebUI,
		Callbacks: conf.Callbacks,
		ID:        frameworkId,

		Client: &http.Client{},
	}

	if masters, scheme, err := mesosMasters(conf.MesosMaster); err == nil {
		s.Masters.Protocol = scheme
		s.Masters.Hosts = masters
	} else {
		return nil, err
	}

	return s, nil
}

func (s *scheduler) mesosURL(path string) *url.URL {
	if s.Masters.Host == "" {
		s.Masters.Host = s.Masters.Hosts[0]
	}
	u := new(url.URL)
	u.Scheme = s.Masters.Protocol
	u.Host = s.Masters.Host
	u.Path = path
	return u
}

func (s *scheduler) IsSubscribed() bool {
	return atomic.LoadInt32(&s.Subscribed) == 1
}

func (s *scheduler) HeartbeatInterval() time.Duration {
	return s.heartbeatInterval
}

func (s *scheduler) StreamId() string {
	if s.IsSubscribed() {
		return s.MesosStreamId
	}
	return ""
}

func (s *scheduler) subscribeLoop(resp *http.Response, fr recordio.Reader) error {
	s.SubscriberWait.Add(1)
	atomic.StoreInt32(&s.Subscribed, 1)
	defer atomic.StoreInt32(&s.Subscribed, 0)
	defer s.SubscriberWait.Done()
	s.SubControl.Lock()
	s.SubControl.C = make(chan struct{})
	s.SubControl.Unlock()

	events := make(chan msg.Event)
	var wg sync.WaitGroup

	var readErr error
	go func(fr recordio.Reader) {
		wg.Add(1)
		defer wg.Done()
		for {
			if _, r, err := fr.ReadFrame(); err == nil {
				var event msg.Event
				if json.NewDecoder(r).Decode(&event); err == nil {
					select {
					case events <- event:
					case <-s.SubControl.C:
					}
				} else {
					readErr = err
					resp.Body.Close()
					close(events)
					return
				}
			} else {
				readErr = err
				resp.Body.Close()
				close(events)
				return
			}
		}
	}(fr)

	for {
		select {
		case event, ok := <-events:
			if ok {
				for _, callback := range s.Callbacks {
					callback(s, event)
				}
				switch event.Type {
				case msg.ERROR:
					return errAborted
				}
			} else {
				return readErr
			}
		case <-s.SubControl.C:
			resp.Body.Close()
		}
	}
}

func (s *scheduler) Subscribe() error {
	if s.IsSubscribed() {
		s.Disconnect()
	}
	var m msg.Subscribe
	m.Type = msg.SUBSCRIBE
	m.Subscribe.FrameworkInfo.User = ""
	m.Subscribe.FrameworkInfo.Name = s.Name
	if id, err := s.ID.FrameworkID(); err == nil {
		if id != "" {
			m.Subscribe.FrameworkInfo.ID = &msg.FrameworkID{Value: id}
			m.FrameworkID = m.Subscribe.FrameworkInfo.ID
		}
	} else {
		return err
	}
	m.Subscribe.FrameworkInfo.User = s.User
	m.Subscribe.FrameworkInfo.FailoverTimeout = (24 * 7 * time.Hour).Seconds()
	m.Subscribe.FrameworkInfo.FailoverTimeout = 300
	m.Subscribe.FrameworkInfo.FailoverTimeout = (24 * 1 * time.Hour).Seconds()
	m.Subscribe.FrameworkInfo.Roles = s.Roles
	m.Subscribe.FrameworkInfo.Hostname = s.Hostname
	m.Subscribe.FrameworkInfo.WebUIURL = s.WebUI
	m.Subscribe.FrameworkInfo.Capabilities = []msg.Capability{
		{Type: msg.MULTI_ROLE},
		{Type: msg.TASK_KILLING_STATE},
		{Type: msg.PARTITION_AWARE},
	}

	var body io.Reader
	if b, err := json.Marshal(m); err == nil {
		body = bytes.NewReader(b)
	} else {
		return err
	}

	if resp, err := s.Client.Post(s.mesosURL("/api/v1/scheduler").String(), "application/json", body); err == nil {
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
			fr := recordio.NewReader(resp.Body, 10485760)
			if _, r, err := fr.ReadFrame(); err == nil {
				var event msg.Event
				if json.NewDecoder(r).Decode(&event); err == nil {
					if event.Type == "SUBSCRIBED" {
						s.MesosStreamId = resp.Header.Get("Mesos-Stream-Id")
						if err := s.ID.SetFrameworkID(event.Subscribed.ID.Value); err != nil {
							resp.Body.Close()
							return err
						}
						s.heartbeatInterval = time.Duration(event.Subscribed.HeartbeatInterval * float64(time.Second))
						go s.subscribeLoop(resp, fr)
						return nil
					} else if event.Type == msg.ERROR {
						resp.Body.Close()
						if event.Error.Message == "Framework has been removed" {
							log.Warn("Got framework removed error when trying to subscribe. Clearing frameworkID and trying again.")
							if err := s.ID.SetFrameworkID(""); err != nil {
								return err
							}
							return s.Subscribe()
						} else {
							return fmt.Errorf("Got ERROR event when trying to subscribe: %v", event.Error.Message)
						}
					} else {
						resp.Body.Close()
						return fmt.Errorf("Unexpected response from subscribe event. Expected event SUBSCRIBED got %v.", event.Type)
					}
				} else {
					resp.Body.Close()
					return err
				}
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Subscribe()
			} else {
				resp.Body.Close()
				return err
			}
		} else {
			return newMesosError(resp)
		}
	} else {
		return err
	}
}

func (s *scheduler) Disconnect() {
	if s.IsSubscribed() {
		s.SubControl.Lock()
		if s.IsSubscribed() {
			close(s.SubControl.C)
			s.SubscriberWait.Wait()
			s.SubControl.C = make(chan struct{})
		}
		s.SubControl.Unlock()
	}
}

func (s *scheduler) Teardown() error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		m := struct {
			Type msg.MessageType `json:"type"`
			ID   msg.FrameworkID `json:"framework_id"`
		}{Type: msg.TEARDOWN, ID: msg.FrameworkID{Value: fid}}

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Teardown()
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Accept(offer msg.Offer, executorResources Resources, taskInfos []msg.TaskInfo) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		type launchOperation struct {
			Type   msg.MessageType `json:"type"`
			Launch struct {
				TaskInfos []msg.TaskInfo `json:"task_infos,omitempty"`
			} `json:"launch"`
			LaunchGroup struct {
				ExecutorInfo struct {
					ExecutorID struct {
						Value string `json:"value"`
					} `json:"executor_id"`
					FrameworkID struct {
						Value string `json:"value"`
					} `json:"framework_id"`
					Type          string `json:"type"`
					ContainerInfo struct {
						Type   string `json:"type"`
						Docker struct {
							Image       string            `json:"image"`
							Network     string            `json:"network,omitempty"`
							PortMapping []msg.PortMapping `json:"port_mappings"`
							Privileged  bool              `json:"privileged"`
						} `json:"docker"`
						Mesos struct {
							Image struct {
								Type   string `json:"type"`
								Docker struct {
									Name string `json:"name"`
								} `json:"docker"`
							} `json:"image"`
						} `json:"mesos"`
						// NetworkInfo []msg.NetworkInfo `json:"network_infos"`
					}
					Resources []msg.TaskInfoResource `json:"resources"`
				} `json:"executor"`
				TaskGroup struct {
					Tasks []msg.TaskInfo `json:"tasks,omitempty"`
				} `json:"task_group"`
			} `json:"launch_group"`
		}

		var m struct {
			Type   msg.MessageType `json:"type"`
			ID     msg.FrameworkID `json:"framework_id"`
			Accept struct {
				OfferIds []struct {
					Value string `json:"value"`
				} `json:"offer_ids"`
				Operations []launchOperation `json:"operations"`
			} `json:"accept"`
		}
		m.Type = msg.ACCEPT
		m.ID = msg.FrameworkID{Value: fid}
		m.Accept.OfferIds = append(m.Accept.OfferIds, struct {
			Value string `json:"value"`
		}{offer.ID.Value})
		m.Accept.Operations = append(m.Accept.Operations, launchOperation{})
		m.Accept.Operations[0].Type = msg.LAUNCH_GROUP
		m.Accept.Operations[0].LaunchGroup.TaskGroup.Tasks = taskInfos
		m.Accept.Operations[0].LaunchGroup.ExecutorInfo.FrameworkID.Value = fid
		m.Accept.Operations[0].LaunchGroup.ExecutorInfo.ExecutorID.Value = ksuid.New().String()
		m.Accept.Operations[0].LaunchGroup.ExecutorInfo.Type = "DEFAULT"
		m.Accept.Operations[0].LaunchGroup.ExecutorInfo.Resources = []msg.TaskInfoResource{
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "cpus",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{executorResources.CPU},
			},
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "mem",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{executorResources.Mem},
			},
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "disk",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{executorResources.Disk},
			},
		}
		// m.Accept.Operations[0].LaunchGroup.ExecutorInfo.ContainerInfo.Type = "MESOS"
		// m.Accept.Operations[0].LaunchGroup.ExecutorInfo.ContainerInfo.NetworkInfo = taskInfos[0].ContainerInfo.NetworkInfo

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Accept(offer, executorResources, taskInfos)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Decline(offers []string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if len(offers) == 0 {
		return nil
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type    msg.MessageType `json:"type"`
			ID      msg.FrameworkID `json:"framework_id"`
			Decline struct {
				OfferIds []struct {
					Value string `json:"value"`
				} `json:"offer_ids"`
				Filters struct {
					RefuseSeconds float64 `json:"refuse_seconds"`
				} `json:"filters"`
			} `json:"decline"`
		}
		m.Type = msg.DECLINE
		m.ID = msg.FrameworkID{Value: fid}
		for _, offer := range offers {
			m.Decline.OfferIds = append(m.Decline.OfferIds, struct {
				Value string `json:"value"`
			}{offer})
		}
		m.Decline.Filters.RefuseSeconds = 5

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Decline(offers)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Acknowledge(agentID string, taskID string, UUID string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if UUID == "" {
		return nil
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type        msg.MessageType `json:"type"`
			ID          msg.FrameworkID `json:"framework_id"`
			Acknowledge struct {
				AgentID struct {
					Value string `json:"value"`
				} `json:"agent_id"`
				TaskID struct {
					Value string `json:"value"`
				} `json:"task_id"`
				UUID string `json:"uuid"`
			} `json:"acknowledge"`
		}
		m.Type = msg.ACKNOWLEDGE
		m.ID = msg.FrameworkID{Value: fid}
		m.Acknowledge.AgentID.Value = agentID
		m.Acknowledge.TaskID.Value = taskID
		m.Acknowledge.UUID = UUID

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Acknowledge(agentID, taskID, UUID)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) AcknowledgeOperationStatus(agentID string, resourceID string, operationID string, UUID string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if UUID == "" {
		return nil
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type        msg.MessageType `json:"type"`
			ID          msg.FrameworkID `json:"framework_id"`
			Acknowledge struct {
				AgentID struct {
					Value string `json:"value"`
				} `json:"agent_id"`
				ResourceID struct {
					Value string `json:"value"`
				} `json:"resource_provider_id"`
				UUID        string `json:"uuid"`
				OperationID string `json:"operation_id"`
			} `json:"acknowledge_operation_status"`
		}
		m.Type = msg.ACKNOWLEDGE_OPERATION_STATUS
		m.ID = msg.FrameworkID{Value: fid}
		m.Acknowledge.AgentID.Value = agentID
		m.Acknowledge.ResourceID.Value = resourceID
		m.Acknowledge.OperationID = operationID
		m.Acknowledge.UUID = UUID

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.AcknowledgeOperationStatus(agentID, resourceID, operationID, UUID)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Reconcile(tasks []string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type      msg.MessageType `json:"type"`
			ID        msg.FrameworkID `json:"framework_id"`
			Reconcile struct {
				Tasks []struct {
					TaskID struct {
						Value string `json:"value"`
					} `json:"task_id"`
				} `json:"tasks"`
			} `json:"reconcile"`
		}
		m.Type = msg.RECONCILE
		m.ID = msg.FrameworkID{Value: fid}
		for _, t := range tasks {
			var _t struct {
				TaskID struct {
					Value string `json:"value"`
				} `json:"task_id"`
			}
			_t.TaskID.Value = t
			m.Reconcile.Tasks = append(m.Reconcile.Tasks, _t)
		}

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Reconcile(tasks)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Suppress(roles []string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type     msg.MessageType `json:"type"`
			ID       msg.FrameworkID `json:"framework_id"`
			Suppress struct {
				Roles []string `json:"roles,omitempty"`
			} `json:"suppress"`
		}
		m.Type = msg.SUPPRESS
		m.ID = msg.FrameworkID{Value: fid}
		m.Suppress.Roles = roles

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Suppress(roles)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Revive(role string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type   msg.MessageType `json:"type"`
			ID     msg.FrameworkID `json:"framework_id"`
			Revive struct {
				Role string `json:"role,omitempty"`
			} `json:"revive"`
		}
		m.Type = msg.REVIVE
		m.ID = msg.FrameworkID{Value: fid}
		m.Revive.Role = role

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Revive(role)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Kill(taskID string, agentID string) error {
	if !s.IsSubscribed() {
		return errNotSubscribed
	}
	if fid, err := s.ID.FrameworkID(); err == nil {
		var m struct {
			Type msg.MessageType `json:"type"`
			ID   msg.FrameworkID `json:"framework_id"`
			Kill struct {
				TaskID struct {
					Value string `json:"value"`
				} `json:"task_id"`
				AgentID *struct {
					Value string `json:"value"`
				} `json:"agent_id,omitempty"`
			} `json:"kill"`
		}
		m.Type = msg.KILL
		m.ID = msg.FrameworkID{Value: fid}
		m.Kill.TaskID.Value = taskID
		if agentID != "" {
			m.Kill.AgentID = &struct {
				Value string `json:"value"`
			}{agentID}
		}

		if resp, err := s.Post(s.mesosURL("/api/v1/scheduler").String(), m); err == nil {
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				resp.Body.Close()
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				resp.Body.Close()
				s.Masters.Host = resp.Header.Get("Location")
				return s.Kill(taskID, agentID)
			} else {
				return newMesosError(resp)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (s *scheduler) Post(url string, body interface{}) (*http.Response, error) {
	var r io.Reader
	if b, ok := body.(io.Reader); ok {
		r = b
	} else {
		if b, err := json.Marshal(body); err == nil {
			r = bytes.NewReader(b)
		} else {
			return nil, err
		}
	}

	if req, err := http.NewRequest("POST", url, r); err == nil {
		req.Header.Set("Content-Type", "application/json")
		if s.MesosStreamId != "" {
			req.Header.Set("Mesos-Stream-Id", s.MesosStreamId)
		}
		return s.Client.Do(req)
	} else {
		return nil, err
	}

}
