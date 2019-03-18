package mesos

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type mesosError struct {
	StatusCode int
	Message    string
}

func newMesosError(resp *http.Response) error {
	if b, err := ioutil.ReadAll(resp.Body); err == nil {
		resp.Body.Close()
		return mesosError{
			StatusCode: resp.StatusCode,
			Message:    string(b),
		}
	} else {
		return err
	}
}

func (m mesosError) Error() string {
	return fmt.Sprintf("Mesos Communication Error - Code %d: %s", m.StatusCode, m.Message)
}
