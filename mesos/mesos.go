package mesos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var errMesosNoPath = errors.New("No path specified for mesos zk lookup.")
var errMesosParseError = errors.New("Error parsing mesos master data in zk.")
var errMesosNoMaster = errors.New("Error finding mesos master.")
var errUnknownScheme = errors.New("Unknown mesos scheme.")
var errMesosUnreachable = errors.New("No reachable mesos masters.")

type Resources struct {
	CPU  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
	Disk float64 `json:"disk"`
}

func mesosMasters(master string) ([]string, string, error) {
	var masterHosts []string
	protocol := "http"
	if path, err := url.Parse(master); err == nil {
		switch path.Scheme {
		case "zks":
			protocol = "https"
			fallthrough
		case "zk":
			if path.Path == "" || path.Path == "/" {
				return nil, protocol, errMesosNoPath
			}
			zookeeperPath := path.Path
			if zookeeperPath[0] != '/' {
				zookeeperPath = "/" + zookeeperPath
			}

			if zoo, _, err := zk.Connect(zk.FormatServers(strings.Split(path.Host, ",")), 10*time.Second, zk.WithLogInfo(false)); err == nil {
				defer zoo.Close()
				if children, _, err := zoo.Children(zookeeperPath); err == nil {
					sort.Strings(children)
					for _, child := range children {
						if strings.HasPrefix(child, "json.info_") {
							if data, _, err := zoo.Get(zookeeperPath + "/" + child); err == nil {
								var masterInfo struct {
									Address struct {
										Hostname string `json:"hostname"`
										Ip       string `json:"ip"`
										Port     int    `json:"port"`
									} `json:"address"`
									Hostname string `json:"hostname"`
									Id       string `json:"id"`
									Ip       int64  `json:"ip"`
									Pid      string `json:"pid"`
									Port     int    `json:"port"`
									Version  string `json:"version"`
								}
								if err := json.Unmarshal(data, &masterInfo); err == nil {
									masterHosts = []string{fmt.Sprintf("%s:%d", masterInfo.Address.Hostname, masterInfo.Address.Port)}
									break
								} else {
									return nil, protocol, errMesosParseError

								}
							}
						}
					}
				} else {
					return nil, protocol, errMesosNoMaster
				}
			}
		case "https":
			protocol = "https"
			fallthrough
		case "http":
			masterHosts = strings.Split(path.Host, ",")
		default:
			return nil, protocol, errUnknownScheme
		}
	} else {
		masterHosts = strings.Split(master, ",")
	}

	if len(masterHosts) == 0 {
		return nil, protocol, errMesosUnreachable
	}
	return masterHosts, protocol, nil
}

func IsTerminalState(state string) bool {
	switch state {
	// TERMINAL
	case "TASK_ERROR":
		return true
	case "TASK_FAILED":
		return true
	case "TASK_DROPPED":
		return true
	case "TASK_GONE":
		return true
	case "TASK_GONE_BY_OPERATOR":
		return true
	case "TASK_FINISHED":
		return true
	case "TASK_UNKNOWN":
		return true
	case "TASK_KILLED":
		return true
	case "TASK_LOST":
		return true
	// NON-TERMINAL
	case "TASK_KILLING":
		return false
	case "TASK_RUNNING":
		return false
	case "TASK_STAGING":
		return false
	case "TASK_STARTING":
		return false
	case "TASK_UNREACHABLE":
		return false
	}
	return false
}
