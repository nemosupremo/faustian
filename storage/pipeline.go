package storage

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/nemosupremo/datasize"
	"github.com/nemosupremo/faustian/mesos"
	"github.com/nemosupremo/faustian/mesos/msg"
	"github.com/segmentio/ksuid"
)

var ErrPipelineExists = errors.New("A pipeline already exists with this ID.")
var ErrPipelineNotExists = errors.New("This pipeline does not exist.")
var ErrConflict = errors.New("Storage backend conflict")

type TaskID string

func NewTaskID(pipelineID string, taskID string, procID string) TaskID {
	if procID == "" {
		return TaskID(pipelineID + "$" + taskID)
	}
	return TaskID(pipelineID + "$" + taskID + ":" + procID)
}

func (t TaskID) GroupID() string {
	idx := strings.IndexByte(string(t), ':')
	if idx > 0 {
		return string(t)[:idx]
	}
	return string(t)
}

func (t TaskID) PipelineID() string {
	idx := strings.IndexByte(string(t), '$')
	if idx > 0 {
		return string(t)[:idx]
	}
	return ""
}

func (t TaskID) ProcessorID() string {
	idx := strings.IndexByte(string(t), ':')
	if idx > 0 {
		return string(t)[idx+1:]
	}
	return ""
}

func (t TaskID) WithProcessID(id string) TaskID {
	groupId := t.GroupID()
	return TaskID(groupId + ":" + id)
}

func (t TaskID) String() string {
	return string(t)
}

type Pipeline struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Container struct {
		Image      string `json:"image"`
		Network    string `json:"network"`
		Parameters []struct {
			Key   string `value:"key"`
			Value string `json:"value"`
		} `json:"parameters"`
	} `json:"container"`
	Processors map[string]struct {
		ID          string            `json:"id"`
		Shell       bool              `json:"shell"`
		Command     string            `json:"command"`
		Arguments   []string          `json:"arguments"`
		Environment map[string]string `json:"environment"`
		Resources   struct {
			CPU  float64 `json:"cpu"`
			Mem  float64 `json:"mem"`
			Disk float64 `json:"disk"`
		} `json:"resources"`
		PortMapping []struct {
			HostPort      int    `json:"host_port"`
			ContainerPort int    `json:"container_port"`
			Protocol      string `json:"protocol"`
		} `json:"port_mappings"`
		KillGracePeriod int `json:"kill_grace_period"`
	} `json:"processes"`
	Instances         int               `json:"instances"`
	Roles             []string          `json:"roles"`
	Labels            map[string]string `json:"labels"`
	Environment       map[string]string `json:"environment"`
	Created           time.Time         `json:"created"`
	ExecutorResources struct {
		CPU  float64 `json:"cpu"`
		Mem  float64 `json:"mem"`
		Disk float64 `json:"disk"`
	} `json:"executor_resources"`
}

type TaskProcess struct {
	ID      string    `json:"id"`
	Status  string    `json:"status,omitempty"`
	AgentID string    `json:"agent_id,omitempty"`
	Updated time.Time `json:"updated"`
}

type Task struct {
	ID          TaskID                  `json:"id"`
	PipelineID  string                  `json:"pipeline_id"`
	ProcessorID string                  `json:"processor_id"`
	Message     string                  `json:"message,omitempty"`
	Launching   bool                    `json:"-"`
	Launched    time.Time               `json:"launched"`
	Failures    int                     `json:"failures"`
	Updated     time.Time               `json:"updated"`
	Processes   map[string]*TaskProcess `json:"processes"`
}

func (p *Pipeline) SatisfiesOffer(taskID TaskID, offer msg.Offer) bool {
	cpuResources := float64(mesos.EXECUTOR_CPU)
	memResources := float64(mesos.EXECUTOR_MEM)
	diskResources := float64(1)
	requiredPorts := []int{}
	for _, proc := range p.Processors {
		cpuResources += proc.Resources.CPU
		memResources += proc.Resources.Mem
		diskResources += proc.Resources.Disk
		for _, mapping := range proc.PortMapping {
			requiredPorts = append(requiredPorts, mapping.HostPort)
		}
	}

	requiredResources := map[string]bool{
		"cpus":  true,
		"mem":   true,
		"disk":  true,
		"ports": true,
	}
	usedWildPorts := 0
	for _, resource := range offer.Resources {
		delete(requiredResources, resource.Name)
		switch resource.Name {
		case "cpus":
			if cpuResources > 0 {
				if cpuResources > resource.Scalar.Value {
					return false
				}
			}
		case "mem":
			if memResources > 0 {
				if memResources > resource.Scalar.Value {
					return false
				}
			}
		case "disk":
			if diskResources > 0 {
				if diskResources > resource.Scalar.Value {
					return false
				}
			}
		case "ports":
			for _, requiredPort := range requiredPorts {
				if requiredPort > 0 {
					found := false
					for _, portRange := range resource.Ranges.Range {
						if requiredPort >= portRange.Begin && requiredPort <= portRange.End {
							found = true
							break
						}
					}
					if !found {
						return false
					}
				} else {
					availablePortCounts := 0
					for _, portRange := range resource.Ranges.Range {
						availablePortCounts = (portRange.End - portRange.Begin) + 1
					}
					if availablePortCounts < usedWildPorts+1 {
						return false
					}
				}
				usedWildPorts += 1
			}
		}
	}
	if len(requiredResources) > 0 {
		return false
	}

	if len(p.Roles) > 0 {
		foundRole := false
		for _, role := range p.Roles {
			if role == offer.AllocationInfo.Role {
				foundRole = true
				break
			}
		}
		if !foundRole {
			return false
		}
	}

	return true

}

func (p *Pipeline) RequiredCPU() int {
	cpu := p.ExecutorResources.CPU
	for _, proc := range p.Processors {
		cpu += proc.Resources.CPU
	}
	return int(math.Ceil(cpu * float64(p.Instances)))
}

func (p *Pipeline) RequiredMem() datasize.ByteSize {
	mem := datasize.ByteSize(p.ExecutorResources.Mem) * datasize.MB
	for _, proc := range p.Processors {
		mem += datasize.ByteSize(proc.Resources.Mem) * datasize.MB
	}
	return mem * datasize.ByteSize(p.Instances)
}

func (p *Pipeline) TaskInfo(taskID TaskID, offer msg.Offer) []msg.TaskInfo {
	var tasks []msg.TaskInfo

	reservedPorts := map[int]struct{}{}
	portSearchFactor := rand.Float64()
	for _, proc := range p.Processors {
		var t msg.TaskInfo

		t.Name = p.ID
		t.TaskID.Value = taskID.WithProcessID(proc.ID).String()
		t.AgentID.Value = offer.AgentID.Value
		t.Resources = []msg.TaskInfoResource{
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "cpus",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{proc.Resources.CPU},
			},
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "mem",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{proc.Resources.Mem},
			},
			{
				AllocationInfo: offer.AllocationInfo,
				Name:           "disk",
				Type:           "SCALAR",
				Role:           offer.AllocationInfo.Role,
				Scalar: &struct {
					Value float64 `json:"value"`
				}{32},
			},
		}

		t.ContainerInfo.Type = "MESOS"
		t.ContainerInfo.Mesos.Image.Type = "DOCKER"
		t.ContainerInfo.Mesos.Image.Docker.Name = p.Container.Image

		t.ContainerInfo.NetworkInfo = append(t.ContainerInfo.NetworkInfo, msg.NetworkInfo{})

		for _, mapping := range proc.PortMapping {
			if mapping.HostPort != 0 {
				reservedPorts[mapping.HostPort] = struct{}{}
			}
		}
		requestedPorts := map[int]struct{}{}
		switch p.Container.Network {
		case "HOST":

		case "BRIDGE":
			for _, mapping := range proc.PortMapping {
				hostPort := mapping.HostPort
				if hostPort == 0 {
				findPort:
					for _, resource := range offer.Resources {
						if resource.Name == "ports" {
							for _, portRange := range resource.Ranges.Range {
								startPort := int(float64(portRange.End-portRange.Begin+1) * portSearchFactor)
								for _p := portRange.Begin; _p <= portRange.End; _p++ {
									p := startPort + _p
									if p > portRange.End {
										p = (p - portRange.End) + portRange.Begin - 1
									}

									if _, ok := reservedPorts[p]; !ok {
										hostPort = p
										reservedPorts[p] = struct{}{}
										break findPort
									}
								}
							}
							break
						}
					}
				}
				if hostPort == 0 {
					// maybe this should be a panic?
					// this offer shouldn't have been satisfied
					continue
				}
				requestedPorts[hostPort] = struct{}{}
				mapping.HostPort = hostPort
				if mapping.ContainerPort == 0 {
					mapping.ContainerPort = mapping.HostPort
				}
				t.ContainerInfo.NetworkInfo[0].PortMapping = append(t.ContainerInfo.NetworkInfo[0].PortMapping, mapping)
				t.Discovery.Ports.Ports = append(t.Discovery.Ports.Ports, msg.Port{
					Number:   hostPort,
					Protocol: mapping.Protocol,
				})
				t.Discovery.Ports.Ports[len(t.Discovery.Ports.Ports)-1].Labels.Labels = []msg.Label{
					{
						Key:   "network-scope",
						Value: "host",
					},
				}
			}
			t.ContainerInfo.NetworkInfo[0].Name = "mesos-bridge"
		}

		if len(requestedPorts) > 0 {
			portIds := make([]int, 0, len(requestedPorts))
			for portId, _ := range requestedPorts {
				portIds = append(portIds, portId)
			}
			sort.Ints(portIds)
			ranges := []msg.Range{}
			var currRange msg.Range
			for _, portId := range portIds {
				if portId-currRange.End > 1 {
					if currRange.Begin != 0 {
						ranges = append(ranges, currRange)
					}
					currRange.Begin = portId
				}
				currRange.End = portId
			}
			if currRange.Begin != 0 {
				ranges = append(ranges, currRange)
			}
			t.Resources = append(t.Resources, msg.TaskInfoResource{
				AllocationInfo: offer.AllocationInfo,
				Name:           "ports",
				Type:           "RANGES",
				Role:           offer.AllocationInfo.Role,
				Ranges: &struct {
					Range []msg.Range `json:"range"`
				}{ranges},
			})
		}

		for name, value := range p.Labels {
			t.Labels.Labels = append(t.Labels.Labels, msg.Label{
				Key:   name,
				Value: value,
			})
		}

		t.CommandInfo.Shell = proc.Shell
		t.CommandInfo.Value = proc.Command
		t.CommandInfo.Arguments = proc.Arguments
		for name, value := range p.Environment {
			t.CommandInfo.Environment.Variable = append(t.CommandInfo.Environment.Variable, msg.Variable{
				Name:  name,
				Value: value,
			})
		}

		for name, value := range proc.Environment {
			t.CommandInfo.Environment.Variable = append(t.CommandInfo.Environment.Variable, msg.Variable{
				Name:  name,
				Value: value,
			})
		}

		t.Discovery.Visibility = "FRAMEWORK"
		t.Discovery.Name = p.ID

		t.KillPolicy.GracePeriod.Nanoseconds = (time.Duration(proc.KillGracePeriod) * time.Second).Nanoseconds()

		tasks = append(tasks, t)
	}

	return tasks
}

func (p *Pipeline) GenerateTasks() []Task {
	tasks := make([]Task, 0, p.Instances*len(p.Processors))
	for i := 0; i < p.Instances; i++ {
		instanceID := ksuid.New().String()
		t := Task{
			ID:          NewTaskID(p.ID, instanceID, ""),
			PipelineID:  p.ID,
			ProcessorID: "",
			Updated:     time.Now(),
		}
		tasks = append(tasks, t)
	}
	return tasks
}

func (t *Task) IsRunnable() bool {
	if t.Launching {
		return false
	}
	if len(t.Processes) == 0 {
		return true
	}
	hasKilled := false
	hasFailed := false
	for _, p := range t.Processes {
		switch p.Status {
		// TERMINAL
		case "TASK_ERROR":
			// return true
		case "TASK_FAILED":
			hasFailed = true
			// return true
		case "TASK_DROPPED":
			// return true
		case "TASK_GONE":
			// return true
		case "TASK_GONE_BY_OPERATOR":
			// return true
		case "TASK_FINISHED":
			return false
		case "TASK_UNKNOWN":
			// return true
		case "TASK_KILLING":
			return false
		case "TASK_KILLED":
			hasKilled = true
		case "TASK_LOST":
			// return true
		// NON-TERMINAL
		case "TASK_RUNNING":
			return false
		case "TASK_STAGING":
			return false
		case "TASK_STARTING":
			return false
		case "TASK_UNREACHABLE":
			return false
		}
	}

	if hasKilled && !hasFailed {
		return false
	}
	return true
}
