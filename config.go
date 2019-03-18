package faustian

import (
	"fmt"
	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"
)

func parseZookeeperUri(uri string) ([]string, string, error) {
	if path, err := url.Parse(uri); err == nil {
		switch path.Scheme {
		case "zk":
			zookeeperPath := path.Path
			if zookeeperPath[0] != '/' {
				zookeeperPath = "/" + zookeeperPath
			}
			if zookeeperPath == "/" {
				zookeeperPath = "/faustian"
			}
			return strings.Split(path.Host, ","), zookeeperPath, nil
		default:
			return nil, "", fmt.Errorf("Invalid scheme %v for zookeeper setting.", path.Scheme)
		}
	} else {
		return nil, "", fmt.Errorf("Invalid uri passed for zookeeper setting.")
	}
}

type zkLogger struct{}

func (z zkLogger) Printf(format string, args ...interface{}) {
	log.Warnf("Zookeeper Client: "+format, args...)
}
