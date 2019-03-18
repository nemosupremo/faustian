package msg

type MessageType string

const (
	SUBSCRIBE               MessageType = "SUBSCRIBE"
	TEARDOWN                            = "TEARDOWN"
	DECLINE                             = "DECLINE"
	UPDATE                              = "UPDATE"
	UPDATE_OPERATION_STATUS             = "UPDATE_OPERATION_STATUS"
	MESSAGE                             = "MESSAGE"
	FAILURE                             = "FAILURE"
	OFFERS                              = "OFFERS"
	RESCIND                             = "RESCIND"

	ACCEPT                       = "ACCEPT"
	LAUNCH                       = "LAUNCH"
	LAUNCH_GROUP                 = "LAUNCH_GROUP"
	HEARTBEAT                    = "HEARTBEAT"
	ERROR                        = "ERROR"
	RECONCILE                    = "RECONCILE"
	SUPPRESS                     = "SUPPRESS"
	REVIVE                       = "REVIVE"
	KILL                         = "KILL"
	ACKNOWLEDGE                  = "ACKNOWLEDGE"
	ACKNOWLEDGE_OPERATION_STATUS = "ACKNOWLEDGE_OPERATION_STATUS"

	SUBSCRIPTION_FAILED = "_SUBSCRIPTION_FAILED"
)

type CapabilityType string

const (
	MULTI_ROLE         CapabilityType = "MULTI_ROLE"
	TASK_KILLING_STATE                = "TASK_KILLING_STATE"
	PARTITION_AWARE                   = "PARTITION_AWARE"
)

type Capability struct {
	Type CapabilityType `json:"type"`
}

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Variable struct {
	Name  string `json:"name"`
	Type  string `json:"type,omitempty"`
	Value string `json:"value"`
}

type FrameworkID struct {
	Value string `json:"value,omitempty"`
}

type Subscribe struct {
	Type        MessageType  `json:"type"`
	FrameworkID *FrameworkID `json:"framework_id,omitempty"`
	Subscribe   struct {
		FrameworkInfo struct {
			User            string       `json:"user"`
			Name            string       `json:"name"`
			ID              *FrameworkID `json:"id,omitempty"`
			FailoverTimeout float64      `json:"failover_timeout,omitempty"`
			Checkpoint      bool         `json:"checkpoint,omitempty"`
			Role            string       `json:"role,omitempty"`
			Roles           []string     `json:"roles,omitempty"`
			Hostname        string       `json:"hostname,omitempty"`
			Principal       string       `json:"principal,omitempty"`
			WebUIURL        string       `json:"webui_url"`
			Capabilities    []Capability `json:"capabilities,omitempty"`
			Labels          []Label      `json:"labels,omitempty"`
		} `json:"framework_info"`
	} `json:"subscribe"`
}

type Offer struct {
	AllocationInfo struct {
		Role string `json:"role"`
	} `json:"allocation_info"`
	ID struct {
		Value string `json:"value"`
	} `json:"id"`
	FrameworkID struct {
		Value string `json:"value"`
	} `json:"framework_id"`
	AgentID struct {
		Value string `json:"value"`
	} `json:"agent_id"`
	Hostname string `json:"hostname"`
	Url      struct {
		Path    string `json:"path"`
		Scheme  string `json:"scheme"`
		Address struct {
			Hostname string `json:"hostname"`
			Ip       string `json:"ip"`
			Port     int    `json:"port"`
		} `json:"address"`
	} `json:"url"`
	Attributes []struct {
		Name string `json:"name"`
		Type string `json:"type"`
		Text struct {
			Value string `json:"value"`
		} `json:"text"`
	} `json:"attributes"`
	Resources []struct {
		AllocationInfo struct {
			Role string `json:"role"`
		} `json:"allocation_info"`
		Name   string `json:"name"`
		Role   string `json:"role"`
		Type   string `json:"type"`
		Ranges struct {
			Range []Range `json:"range"`
		} `json:"ranges"`
		Scalar struct {
			Value float64 `json:"value"`
		} `json:"scalar"`
	} `json:"resources"`
}

type Event struct {
	Type  MessageType `json:"type"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
	Subscribed struct {
		ID                FrameworkID `json:"framework_id"`
		HeartbeatInterval float64     `json:"heartbeat_interval_seconds"`
	} `json:"subscribed"`
	Offers struct {
		Offers []Offer `json:"offers"`
	} `json:"offers"`
	Update struct {
		Status struct {
			TaskID struct {
				Value string `json:"value"`
			} `json:"task_id"`
			AgentID struct {
				Value string `json:"value"`
			} `json:"agent_id"`
			State   string `json:"state"`
			Source  string `json:"source"`
			UUID    string `json:"uuid"`
			Bytes   string `json:"bytes"`
			Message string `json:"message"`
			Reason  string `json:"reason"`
		} `json:"status"`
	} `json:"update"`
	UpdateOperationStatus struct {
		Status struct {
			State   string `json:"state"`
			AgentID struct {
				Value string `json:"value"`
			} `json:"agent_id"`
			ResourceProviderID struct {
				Value string `json:"value"`
			} `json:"resource_provider_id"`
			OperationID string `json:"operation_id"`
			UUID        string `json:"uuid"`
		}
	} `json:"update_operation_status"`
}

type TaskInfoResource struct {
	AllocationInfo struct {
		Role string `json:"role"`
	} `json:"allocation_info"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Role   string `json:"role"`
	Scalar *struct {
		Value float64 `json:"value"`
	} `json:"scalar,omitempty"`
	Ranges *struct {
		Range []Range `json:"range"`
	} `json:"ranges,omitempty"`
}

type PortMapping struct {
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Protocol      string `json:"protocol"`
}

type Port struct {
	Number     int    `json:"number"`
	Name       string `json:"name,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	Visibility string `json:"visibility,omitempty"`
	Labels     struct {
		Labels []Label `json:"labels,omitempty"`
	} `json:"labels,omitempty"`
}

type Range struct {
	Begin int `json:"begin"`
	End   int `json:"end"`
}

type NetworkInfo struct {
	IPAddresses []struct {
		Protocol  string `json:"protocol"`
		IPAddress string `json:"ip_address"`
	} `json:"ip_addresses,omitempty"`
	Name        string        `json:"name,omitempty"`
	Groups      []string      `json:"groups,omitempty"`
	PortMapping []PortMapping `json:"port_mappings"`
}

type TaskInfo struct {
	Name   string `json:"name"`
	TaskID struct {
		Value string `json:"value"`
	} `json:"task_id"`
	AgentID struct {
		Value string `json:"value"`
	} `json:"agent_id"`
	Resources   []TaskInfoResource `json:"resources"`
	CommandInfo struct {
		URIs []struct {
			Value      string `json:"value,omitempty"`
			Executable bool   `json:"executable"`
			Extract    bool   `json:"extract"`
			Cache      bool   `json:"cache"`
			OutputFile string `json:"output_file,omitempty"`
		} `json:"uris"`
		Environment struct {
			Variable []Variable `json:"variables"`
		} `json:"environment"`
		Shell     bool     `json:"shell"`
		Value     string   `json:"value,omitempty"`
		Arguments []string `json:"arguments"`
	} `json:"command"`
	ContainerInfo struct {
		Type   string `json:"type"`
		Docker struct {
			Image       string        `json:"image"`
			Network     string        `json:"network,omitempty"`
			PortMapping []PortMapping `json:"port_mappings"`
			Privileged  bool          `json:"privileged"`
		} `json:"docker"`
		Mesos struct {
			Image struct {
				Type   string `json:"type"`
				Docker struct {
					Name string `json:"name"`
				} `json:"docker"`
			} `json:"image"`
		} `json:"mesos"`
		NetworkInfo []NetworkInfo `json:"network_infos"`
	} `json:"container"`
	Discovery struct {
		Visibility  string `json:"visibility"`
		Name        string `json:"name,omitempty"`
		Environment string `json:"environment,omitempty"`
		Location    string `json:"location,omitempty"`
		Version     string `json:"version,omitempty"`
		Ports       struct {
			Ports []Port `json:"ports"`
		} `json:"ports"`
		Labels struct {
			Labels []Label `json:"labels,omitempty"`
		} `json:"labels"`
	} `json:"discovery"`
	KillPolicy struct {
		GracePeriod struct {
			Nanoseconds int64 `json:"nanoseconds"`
		} `json:"grace_period"`
	} `json:"kill_policy"`
	Labels struct {
		Labels []Label `json:"labels,omitempty"`
	} `json:"labels"`
}
