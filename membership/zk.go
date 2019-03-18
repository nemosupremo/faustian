package membership

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/segmentio/ksuid"
	log "github.com/sirupsen/logrus"
)

type zkMemberPool struct {
	zk   *zk.Conn
	host string
	path string

	id           string
	createdNodes []string
	myself       PoolMember

	members     []PoolMember
	membersLock sync.Mutex
	leaderId    string

	stop   chan bool
	status sync.WaitGroup

	changeListener OnMembershipChanged

	running int32
}

func NewZkMemberPool(zk *zk.Conn, host string, path string, onChange OnMembershipChanged) (MemberPool, error) {
	mp := new(zkMemberPool)
	mp.zk = zk
	mp.host = host
	mp.path = path
	mp.stop = make(chan bool)
	mp.changeListener = onChange
	mp.id = ksuid.New().String()
	return mp, nil
}

func (mp *zkMemberPool) Join() (PoolMember, error) {
	if atomic.LoadInt32(&mp.running) == 1 {
		return PoolMember{}, errAlreadyJoined
	}

	atomic.StoreInt32(&mp.running, 1)
	pm := PoolMember{
		Host: mp.host,
		Id:   mp.id,
	}
	mp.myself = pm
	data, _ := json.Marshal(pm)
	if p, err := mp.zk.CreateProtectedEphemeralSequential(mp.path+"/"+pm.Node(), data, zk.WorldACL(zk.PermAll)); err == zk.ErrNoNode {
		log.Debugf("Membership: Zookeeper node %s does not exist. Going to create it.", mp.path)
		if _, err := mp.zk.Create(mp.path, []byte{}, 0, zk.WorldACL(zk.PermAll)); err != nil && err != zk.ErrNodeExists {
			return pm, fmt.Errorf("Failed to create nodes zknode: %s", err.Error())
		}
		if p, err := mp.zk.CreateProtectedEphemeralSequential(mp.path+"/"+pm.Node(), data, zk.WorldACL(zk.PermAll)); err == nil {
			mp.createdNodes = append(mp.createdNodes, p)
		} else {
			return pm, fmt.Errorf("Failed to create ephmeral node: %s", err.Error())
		}
	} else if err == nil {
		mp.createdNodes = append(mp.createdNodes, p)
	} else if err != nil {
		return pm, fmt.Errorf("Failed to create ephmeral node: %s", err.Error())
	}
	go mp.watch(mp.zk)
	return pm, nil
}

func (mp *zkMemberPool) Leave() {
	atomic.StoreInt32(&mp.running, 0)
	close(mp.stop)
	mp.status.Wait()
	for _, n := range mp.createdNodes {
		mp.zk.Delete(n, -1)
	}
}

func (mp *zkMemberPool) IsLeader() bool {
	mp.membersLock.Lock()
	defer mp.membersLock.Unlock()
	return mp.leaderId == mp.id
}

func (mp *zkMemberPool) watch(zk *zk.Conn) error {
	mp.status.Add(1)
	defer mp.status.Done()
	tmr := time.NewTimer(5 * time.Second)
	for {
		if ch, err := mp.onMembershipChange(); err == nil {
			select {
			case <-ch:
			case <-mp.stop:
				return nil
			}
		} else {
			log.Warnf("Error watching children: %s", err.Error())
			tmr.Reset(5 * time.Second)
			select {
			case <-tmr.C:
			case <-mp.stop:
				return nil
			}
		}
	}
}

func (mp *zkMemberPool) onMembershipChange() (<-chan zk.Event, error) {
	mp.status.Add(1)
	defer mp.status.Done()
	if children, stat, ch, err := mp.zk.ChildrenW(mp.path); err == nil {
		newMembers := make([]PoolMember, len(children))
		leader := ""
		leaderNo := math.MaxInt32 + 1
		foundMyself := false
		for idx, child := range children {
			log.Debugf(" Membership Child: %s", child)
			name := strings.SplitN(child, "-", 2)[1]
			name = name[:len(name)-10] // remove sequntial bit
			b := strings.Split(name, "$")
			if len(b) != 2 {
				log.Warnf("Membership: Invalid node " + name)
				continue
			}
			newMembers[idx] = PoolMember{Host: b[0], Id: b[1], Version: stat.Cversion}
			if b[1] == mp.id {
				foundMyself = true
			}
			if seqNo, err := strconv.Atoi(child[len(child)-10:]); err == nil {
				if seqNo < leaderNo {
					leader = b[1]
				}
			} else {
				log.Warnf("Membership: Failed to parse sequence number for node %v", child)
			}
		}
		mp.membersLock.Lock()
		mp.members = newMembers
		mp.leaderId = leader
		mp.membersLock.Unlock()
		log.Debugf("Membership: New Leader: %v Myself: %v", leader, mp.id)
		if !foundMyself {
			go func(pm PoolMember) {
				data, _ := json.Marshal(pm)
				if p, err := mp.zk.CreateProtectedEphemeralSequential(mp.path+"/"+pm.Node(), data, zk.WorldACL(zk.PermAll)); err == nil {
					mp.createdNodes = append(mp.createdNodes, p)
				} else {
					log.Warnf("Failed to register my own membership in zookeeper - this node will not be eligible to become leader.")
					mp.Leave()
				}
			}(mp.myself)
		}
		if mp.changeListener != nil {
			mp.changeListener(mp.IsLeader())
		}
		return ch, nil
	} else {
		log.Warnf("Membership: Failed to query for children: %v", err)
		return nil, err
	}
}
