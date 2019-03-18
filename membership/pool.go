package membership

import (
	"errors"
)

var errAlreadyJoined = errors.New("already joined member pool")

type OnMembershipChanged func(bool)

type MemberPool interface {
	Leave()
	Join() (PoolMember, error)
	IsLeader() bool
}

type PoolMember struct {
	Host    string `json:"host"`
	Id      string `json:"id"`
	Version int32  `json:"version"`
	Leader  bool   `json:"-"`
}

func (pm *PoolMember) Node() string {
	return pm.Host + "$" + pm.Id
}

type Members []PoolMember

func (a Members) Len() int      { return len(a) }
func (a Members) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Members) Less(i, j int) bool {
	return (a[i].Host == a[j].Host && a[i].Id < a[j].Id) || a[i].Host < a[j].Host
}
