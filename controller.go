package faustian

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cenkalti/backoff"
	"github.com/nemosupremo/datasize"
	"github.com/nemosupremo/faustian/autoscaling"
	"github.com/nemosupremo/faustian/membership"
	"github.com/nemosupremo/faustian/mesos"
	"github.com/nemosupremo/faustian/mesos/msg"
	"github.com/nemosupremo/faustian/storage"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type Controller struct {
	Zk         *zk.Conn
	Scheduler  mesos.Scheduler
	Membership membership.MemberPool
	Storage    storage.Storage
	Addr       string
	Listen     string

	MesosEvents  chan msg.Event
	LeaderChange chan bool

	AutoScaler           autoscaling.AutoScaler
	AutoscaleDefaultRole string
	ScaledGroups         map[string]string

	version string
	ping    chan chan<- Pong
	wg      sync.WaitGroup
	quit    chan struct{}
	hasQuit int32

	Config struct {
		Zk struct {
			Path string
		}
	}
	Vault struct {
		Addr  string
		Token string
	}
}

type ControllerConfig struct {
	Hostname string
	Port     int
	Zk       struct {
		Uri   string
		Hosts []string
		Path  string
	}
	Scheduler   mesos.SchedulerConfig
	Autoscaling struct {
		Provider    string
		DefaultRole string
	}

	Vault struct {
		Addr           string
		GatekeeperAddr string
		Token          string
	}

	Aws struct {
		AccessKey string
		SecretKey string
		AsgMap    []string
	}
}

type Pong struct {
	OK        bool          `json:"ok"`
	Leader    bool          `json:"leader"`
	Version   string        `json:"version"`
	Uptime    time.Duration `json:"uptime"`
	Pipelines int           `json:"pipelines"`
	Tasks     int           `json:"tasks"`
}

func NewController(config ControllerConfig) (*Controller, error) {
	c := &Controller{
		MesosEvents:  make(chan msg.Event),
		LeaderChange: make(chan bool),
		ScaledGroups: make(map[string]string),
		quit:         make(chan struct{}),
		ping:         make(chan chan<- Pong),

		AutoscaleDefaultRole: config.Autoscaling.DefaultRole,
	}

	c.Listen = config.Hostname + ":" + strconv.Itoa(config.Port)
	if config.Hostname == "" {
		config.Hostname, _ = os.Hostname()
	}

	c.Addr = config.Hostname + ":" + strconv.Itoa(config.Port)
	if config.Scheduler.Hostname == "" {
		config.Scheduler.Hostname = config.Hostname
	}
	if config.Scheduler.WebUI == "" {
		u := &url.URL{
			Scheme: "http",
			Host:   c.Addr,
			Path:   "/",
		}
		config.Scheduler.WebUI = u.String()
	}
	config.Scheduler.Callbacks = []mesos.EventCallback{c.OnSchedulerEvent}

	if config.Zk.Uri != "" {
		if hosts, path, err := parseZookeeperUri(config.Zk.Uri); err == nil {
			config.Zk.Hosts = hosts
			config.Zk.Path = path
		} else {
			return nil, err
		}
	}
	if len(config.Zk.Hosts) == 0 {
		return nil, fmt.Errorf("No hosts provided for zookeeper.")
	}

	if zoo, _, err := zk.Connect(zk.FormatServers(config.Zk.Hosts), 10*time.Second, zk.WithLogInfo(false), zk.WithLogger(zkLogger{})); err == nil {
		c.Zk = zoo
		err := func(path []string) error {
			for i := 1; i <= len(path); i++ {
				newpath := "/" + strings.Join(path[:i], "/")
				_, err := zoo.Create(newpath, []byte{}, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					// Skip if node already exists
					if err != zk.ErrNodeExists {
						return err
					}
				}
			}
			return nil
		}(strings.Split(strings.TrimSuffix(config.Zk.Path[1:], "/"), "/"))
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	if s, err := storage.NewZkStorage(storage.WithZkConnection(c.Zk), storage.WithZkPrefix(config.Zk.Path)); err == nil {
		c.Storage = s
	} else {
		return nil, err
	}

	if m, err := membership.NewZkMemberPool(c.Zk, c.Addr, path.Join(config.Zk.Path, "membership"), c.OnLeadershipChange); err == nil {
		c.Membership = m
	} else {
		return nil, err
	}

	if s, err := mesos.NewScheduler(c.Storage, config.Scheduler); err == nil {
		c.Scheduler = s
	} else {
		return nil, err
	}

	c.Vault.Addr = config.Vault.Addr
	if os.Getenv("MESOS_TASK_ID") != "" && config.Vault.GatekeeperAddr != "" {
		if token, err := gatekeeperToken(config.Vault.GatekeeperAddr, config.Vault.Addr, os.Getenv("MESOS_TASK_ID")); err == nil {
			c.Vault.Token = token
			go (&vaultToken{c.Vault.Addr, c.Vault.Token}).RenewalWorker(nil)
		} else {
			if _, ok := err.(VaultSecurityError); ok {
				log.Warn("Security violation when trying to retrieve Token from Gatekeeper")
				return nil, err
			}
			log.Errorf("Error when trying to retrieve Gatekeeper token: %v", err)
			return nil, err
		}
	} else if config.Vault.Token != "" {
		c.Vault.Token = config.Vault.Token
	}

	switch config.Autoscaling.Provider {
	case "":

	case "aws":
		var awsCredentials *credentials.Credentials
		if config.Aws.SecretKey != "" {
			awsCredentials = credentials.NewStaticCredentials(config.Aws.AccessKey, config.Aws.SecretKey, "")
		} else if c.Vault.Token != "" {
			awsCredentials = credentials.NewCredentials(NewVaultAwsCredProvider(c.Vault.Addr, c.Vault.Token))
		}

		var groupIds []string
		for _, m := range config.Aws.AsgMap {
			x := strings.Split(m, ":")
			if len(x) != 2 {
				return nil, errors.New("Invalid value in autoscaling group map: " + m)
			}
			c.ScaledGroups[x[0]] = x[1]
			groupIds = append(groupIds, x[1])
		}

		if as, err := autoscaling.NewAwsAutoScaler(awsCredentials, groupIds...); err == nil {
			c.AutoScaler = as
		} else {
			return nil, err
		}
	default:
		return nil, errors.New("Invalid autoscaling provider: " + config.Autoscaling.Provider)
	}

	return c, nil
}

func (c *Controller) SetVersion(v string) {
	c.version = v
}

func (c *Controller) Run() error {
	c.wg.Add(1)
	isLeader := false
	isSupressed := false

	tasks := make(map[string]*storage.Task)
	pipelines := make(map[string]storage.Pipeline)
	tasksKilling := make(map[string]struct{})

	ticker := time.NewTicker(1 * time.Minute)
	var lastMesosHeartbeat time.Time
	var taskIds []string
	var tasksWatch <-chan zk.Event
	var pipelineWatch <-chan zk.Event
	var lastAutoscale time.Time

	reconcileProtectCounter := 0
	reconcileTimer := backoff.NewTicker(backoff.NewExponentialBackOff())
	reconcileTimer.Stop()
	var reconcileTasks []string

	syncTasks := NewCoalesceChan(100 * time.Millisecond)

	if _, err := c.Membership.Join(); err != nil {
		return err
	}

	defer c.wg.Done()
	defer log.Warn("Exiting Controller Run")
	defer syncTasks.Close()
	defer func() {
		go func(ch <-chan bool) {
			for range ch {
			}
		}(c.LeaderChange)
		c.Membership.Leave()
		close(c.LeaderChange)
	}()
	defer c.Scheduler.Disconnect()
	defer c.tryShutdown()

	log.Info("Starting Controller...")
	controllerStart := time.Now()
	go c.Serve()
	for {
		select {
		case <-ticker.C:
			if isLeader && c.Scheduler.IsSubscribed() {
				if !lastMesosHeartbeat.IsZero() &&
					time.Now().Sub(lastMesosHeartbeat)/c.Scheduler.HeartbeatInterval() >= 5 {
					log.Debugf("Last Heartbeat: %v", lastMesosHeartbeat)
					log.Warnf("Haven't received mesos heartbeat in at least 5 continuous intervals.")
					log.Warn("As the leader, shutting down.")
					return errors.New("No hearbeats")
				}

				if isSupressed {
					for _, task := range tasks {
						if task.IsRunnable() {
							if err := c.Scheduler.Revive(""); err == nil {
								log.Info("Revive'd framework.")
								isSupressed = false
							} else {
								log.Warnf("Failed to revive framework: %v", err)
								return err
							}
							break
						}
					}
				} else {
					suppress := true
					for _, task := range tasks {
						if task.IsRunnable() {
							suppress = false
							break
						}
					}
					if suppress && len(reconcileTasks) == 0 {
						if err := c.Scheduler.Suppress(nil); err == nil {
							log.Info("Surpress'd framework.")
							isSupressed = true
						} else {
							log.Warnf("Failed to suppress framework: %v", err)
							return err
						}
					}
				}

				if len(reconcileTasks) == 0 {
					reconcileProtectCounter += 1
					if reconcileProtectCounter%5 == 0 {
						log.Debugf("Starting normal reconcile...")
						if err := c.Scheduler.Reconcile(nil); err != nil {
							log.Warnf("Failed to call reconcile task: %v", err.Error())
							return err
						}
					}
				}

				if c.AutoScaler != nil {
					for role, asgID := range c.ScaledGroups {
						var cpu int
						var mem datasize.ByteSize
						for _, pipeline := range pipelines {
							roles := pipeline.Roles
							if len(roles) == 0 {
								roles = []string{c.AutoscaleDefaultRole}
							}
							for _, r := range roles {
								if _, ok := c.ScaledGroups[r]; ok {
									if r == role {
										cpu += pipeline.RequiredCPU()
										mem += pipeline.RequiredMem()
									}
									break
								}
							}
						}

						if currentCapacity, err := c.AutoScaler.CurrentCapacity(asgID); err == nil {
							if desiredCapacity, err := c.AutoScaler.DesiredCapacity(asgID, cpu, mem); err == nil {
								log.Debugf("[Role: %s] Desired cpu: %v mem: %v capacity: %d current: %d", role, cpu, mem, desiredCapacity, currentCapacity)
								if desiredCapacity > currentCapacity || desiredCapacity < (currentCapacity-1) {
									scaleUp := desiredCapacity > currentCapacity
									if scaleUp || time.Now().Sub(lastAutoscale) > 5*time.Minute {
										log.Infof("Autoscaling %v cluster from %d nodes to %d nodes.", role, currentCapacity, desiredCapacity)
										if err := c.AutoScaler.Scale(asgID, desiredCapacity); err == nil {
											lastAutoscale = time.Now()
											c.Storage.SetLastAutoscale(lastAutoscale)
										} else {
											log.Warnf("Failed to autoscale '%s' cluster: %v", role, err)
										}
									}
								}
							} else {
								log.Warnf("Failed to get desired capacity for autoscaling group for role '%v': %v", role, err)
							}
						} else {
							log.Warnf("Failed to get current capacity for autoscaling group for role '%v': %v", role, err)
						}
					}
				}
			}
		case event := <-c.MesosEvents:
			if event.Type == msg.HEARTBEAT {
				log.Tracef("Received Mesos Event %s", event.Type)
			} else {
				log.Debugf("Received Mesos Event %s", event.Type)
			}
			if isLeader {
				switch event.Type {
				case msg.OFFERS:
					var declinedOffers []string
					refreshedPipeline := false
					for _, offer := range event.Offers.Offers {
						decline := true
						if len(reconcileTasks) == 0 { // don't accept any offers if we are trying to reconcile
						label_offer_tasks_loop:
							for _, task := range tasks {
								// don't launch a task if the pipeline contains tasks we are trying to kill
								// maybe move to IsRunnable?
								for taskId, _ := range tasksKilling {
									if storage.TaskID(taskId).PipelineID() == task.ID.PipelineID() {
										continue label_offer_tasks_loop
									}
								}
								if task.IsRunnable() {
									pipeline, ok := pipelines[task.PipelineID]
									if !refreshedPipeline && !ok {
										var err error
										pipelines, _, err = c.Storage.Pipelines(false)
										if err != nil {
											log.Warnf("Failed to read pipelines from storage backend: %v", err)
											log.Warn("Failed to read pipelines from storage backend as leader. Shutting down.")
											return err
										}
										refreshedPipeline = true
										pipeline, ok = pipelines[task.PipelineID]
									}

									if ok {
										if pipeline.SatisfiesOffer(task.ID, offer) {
											log.Debugf("Accepting offer %v to launch task %v", offer.ID.Value, task.ID)
											taskInfos := pipeline.TaskInfo(task.ID, offer)
											if err := c.Scheduler.Accept(offer, mesos.Resources(pipeline.ExecutorResources), taskInfos); err == nil {
												task.Launched = time.Now()
												task.Launching = true
												decline = false
												syncTasks.Wake()
												break
											} else {
												log.Warnf("Failed to accept offer: %v", err)
											}
										} else {
											log.Debugf("Declining offer %v for task %v", offer.ID.Value, task.ID)
										}
									} else {
										log.Warnf("Task ID %v refers to non-existant pipeline %v", task.ID, task.PipelineID)
									}
								}
							}
						}
						if decline {
							declinedOffers = append(declinedOffers, offer.ID.Value)
						}
					}
					log.Debugf("Declining %d offers [Is Reconciling: %v]", len(declinedOffers), len(reconcileTasks) > 0)
					if err := c.Scheduler.Decline(declinedOffers); err != nil {
						log.Warnf("Failed to decline offers: %v", err.Error())
						return err
					}
				case msg.RESCIND:
					log.Infof("Received rescind event.")
				case msg.UPDATE:
					log.Debugf("Received UPDATE for Task %v. Status: %v", event.Update.Status.TaskID.Value, event.Update.Status.State)

					switch event.Update.Status.State {
					case "TASK_ERROR", "TASK_FAILED":
						log.Warnf("Task %v is in state of %v: %v", event.Update.Status.TaskID.Value, event.Update.Status.State, event.Update.Status.Message)
					}
					if len(reconcileTasks) > 0 {
						taskId := event.Update.Status.TaskID.Value
						if idx := sort.SearchStrings(reconcileTasks, taskId); idx < len(reconcileTasks) && reconcileTasks[idx] == taskId {
							reconcileTasks = append(reconcileTasks[:idx], reconcileTasks[idx+1:]...)
							taskID := storage.TaskID(taskId)
							if t, ok := tasks[taskID.GroupID()]; ok {
								t.Processes[taskID.ProcessorID()] = &storage.TaskProcess{
									ID:      event.Update.Status.TaskID.Value,
									Status:  event.Update.Status.State,
									AgentID: event.Update.Status.AgentID.Value,
									Updated: time.Now(),
								}
							} else {
								tasks[taskID.GroupID()] = &storage.Task{
									ID:         storage.TaskID(taskId),
									PipelineID: storage.TaskID(taskId).PipelineID(),
									Updated:    time.Now(),
									Processes: map[string]*storage.TaskProcess{
										taskID.ProcessorID(): &storage.TaskProcess{
											ID:      event.Update.Status.TaskID.Value,
											Status:  event.Update.Status.State,
											AgentID: event.Update.Status.AgentID.Value,
											Updated: time.Now(),
										},
									},
								}
							}

							if len(reconcileTasks) == 0 {
								reconcileTimer.Stop()
							}
						}
					}

					statusTaskID := storage.TaskID(event.Update.Status.TaskID.Value)
					if task, ok := tasks[statusTaskID.GroupID()]; ok {
						task.Launching = false
						task.Updated = time.Now()
						switch event.Update.Status.State {
						case "TASK_ERROR", "TASK_FAILED", "TASK_DROPPED", "TASK_GONE", "TASK_GONE_BY_OPERATOR", "TASK_LOST":
							task.Failures += 1
							log.Warnf("Received unexpected terminal update for task %v: %v", event.Update.Status.TaskID.Value, event.Update.Status.Message)
						}
						task.Processes[statusTaskID.ProcessorID()] = &storage.TaskProcess{
							ID:      event.Update.Status.TaskID.Value,
							Status:  event.Update.Status.State,
							AgentID: event.Update.Status.AgentID.Value,
							Updated: task.Updated,
						}
						if len(reconcileTasks) == 0 {
							syncTasks.Wake()
						}
					} else {
						switch event.Update.Status.State {
						case "TASK_KILLING":
							tasksKilling[event.Update.Status.TaskID.Value] = struct{}{}
						case "TASK_KILLED", "TASK_FINISHED", "TASK_UNKNOWN":
							delete(tasksKilling, event.Update.Status.TaskID.Value)
						case "TASK_ERROR", "TASK_FAILED", "TASK_DROPPED", "TASK_GONE", "TASK_GONE_BY_OPERATOR", "TASK_LOST":
							log.Warnf("Received unexpected terminal update for task %v: %v", event.Update.Status.TaskID.Value, event.Update.Status.Message)
							delete(tasksKilling, event.Update.Status.TaskID.Value)
						default:
							log.Warnf("Received status update for task %v, but we have no knowledge of this task. Killing task...", event.Update.Status.TaskID.Value)
							if err := c.Scheduler.Kill(event.Update.Status.TaskID.Value, event.Update.Status.AgentID.Value); err != nil {
								log.Warnf("Failed to kill task %v: %v", event.Update.Status.TaskID.Value, err)
							}
						}
					}
					if err := c.Scheduler.Acknowledge(event.Update.Status.AgentID.Value, event.Update.Status.TaskID.Value, event.Update.Status.UUID); err != nil {
						log.Warnf("Failed to acknowledged status update: %v", err)
					}
				case msg.UPDATE_OPERATION_STATUS:
					if err := c.Scheduler.AcknowledgeOperationStatus(event.UpdateOperationStatus.Status.AgentID.Value,
						event.UpdateOperationStatus.Status.ResourceProviderID.Value,
						event.UpdateOperationStatus.Status.OperationID,
						event.UpdateOperationStatus.Status.UUID); err != nil {
						log.Warnf("Failed to acknowledged operation status update: %v", err)
					}
				case msg.HEARTBEAT:
					lastMesosHeartbeat = time.Now()
				case msg.MESSAGE:
					log.Infof("Received message event.")
				case msg.FAILURE:
					log.Infof("Received agent failure event")
				case msg.ERROR:
					log.Warnf("Received an error message from Mesos: %v", event.Error.Message)
					log.Warn("As the leader, shutting down.")
					return fmt.Errorf("Mesos Scheduler Error: %v", event.Error.Message)
				default:
					log.Debugf("Received unhandled message type: %v", event.Type)
				}
			}
		case <-tasksWatch:
			log.Debug("Notified of tasks change")
			// if isleader
			if newTasks, newWatch, err := c.Storage.Tasks(true); err == nil {
				sort.Strings(newTasks)
				tasksWatch = newWatch
				hasNew := false
				for _, taskId := range newTasks {
					if _, ok := tasks[taskId]; !ok {
						tasks[taskId] = &storage.Task{
							ID:         storage.TaskID(taskId),
							PipelineID: storage.TaskID(taskId).PipelineID(),
							Updated:    time.Now(),
							Processes:  make(map[string]*storage.TaskProcess),
						}
						hasNew = true
					}
				}
				syncTasks.Wake()
				for _, taskId := range taskIds {
					if idx := sort.SearchStrings(newTasks, taskId); idx == len(newTasks) || newTasks[idx] != taskId {
						log.Infof("Task %v was removed from the state. Killing task...", taskId)
						for _, process := range tasks[taskId].Processes {
							if err := c.Scheduler.Kill(process.ID, process.AgentID); err == nil {
								tasksKilling[process.ID] = struct{}{}
							} else {
								log.Warnf("Failed to kill task %v: %v", taskId, err)
							}
						}
						delete(tasks, taskId)
					}
				}
				taskIds = newTasks

				if isSupressed && hasNew {
					if err := c.Scheduler.Revive(""); err == nil {
						log.Info("Revive'd framework.")
						isSupressed = false
					} else {
						log.Warnf("Failed to revive framework: %v", err)
						return err
					}
				}
			} else {
				log.Warnf("Failed to read tasks from storage backend: %v", err)
				if isLeader {
					log.Warn("Failed to read tasks from storage backend as leader. Shutting down.")
					return err
				}
			}
		case <-pipelineWatch:
			log.Debug("Notified of pipelines change")
			if newPipelines, newWatch, err := c.Storage.Pipelines(true); err == nil {
				pipelineWatch = newWatch
				for _, pipeline := range pipelines {
					if _, ok := newPipelines[pipeline.ID]; !ok {
						log.Infof("Pipeline %v is no longer active.", pipeline.ID)
					}
				}
				pipelines = newPipelines
			} else {
				log.Warnf("Failed to read pipelines from storage backend: %v", err)
				if isLeader {
					log.Warn("Failed to read pipelines from storage backend as leader. Shutting down.")
					return err
				}
			}
		case <-syncTasks.C:
			if isLeader {
				if err := c.Storage.SaveTasks(tasks); err == nil {
					log.Debug("Synced all tasks information with Zookeeper.")
				} else {
					log.Warnf("Failed to save all task information with Zookeeper: %v", err)
				}
			}
		case isNowLeader := <-c.LeaderChange:
			log.Debugf("Notified of leadership change. Leader: %v", isNowLeader)
			if isLeader != isNowLeader {
				if isNowLeader {
					log.Info("Elected leader.")
					if err := c.Scheduler.Subscribe(); err != nil {
						log.Warnf("Failed to subscribe: %v", err.Error())
						return err
					}
					{
						fid, _ := c.Storage.FrameworkID()
						log.Debugf("Subscribed. FrameworkID: %v", fid)
					}
					var err error
					pipelines, pipelineWatch, err = c.Storage.Pipelines(true)
					if err != nil {
						log.Warnf("Failed to read pipelines from storage backend while being promoted: %v", err)
						log.Warn("Since I couldn't get the pipelines, I will shutdown.")
						return err
					}

					taskIds, tasksWatch, err = c.Storage.Tasks(true)
					if err != nil {
						log.Warnf("Failed to read tasks from storage backend while being promoted: %v", err)
						log.Warn("Since I couldn't get the tasks, I will shutdown.")
						return err
					}

					tasksKilling = make(map[string]struct{})

					log.Debugf("Status: %d Pipelines; %d Tasks", len(pipelines), len(taskIds))
					if len(taskIds) > 0 {
						reconcileTasks = []string{}
						for _, task := range taskIds {
							taskID := storage.TaskID(task)
							if pipeline, ok := pipelines[taskID.PipelineID()]; ok {
								for _, proc := range pipeline.Processors {
									reconcileTasks = append(reconcileTasks, string(taskID.WithProcessID(proc.ID)))
								}
							}
						}

						sort.Strings(reconcileTasks)
						b := &backoff.ExponentialBackOff{
							InitialInterval:     1500 * time.Millisecond,
							RandomizationFactor: backoff.DefaultRandomizationFactor,
							Multiplier:          backoff.DefaultMultiplier,
							MaxInterval:         backoff.DefaultMaxInterval,
							MaxElapsedTime:      0,
							Clock:               backoff.SystemClock,
						}
						b.Reset()
						reconcileTimer = backoff.NewTicker(b)
					}
					reconcileProtectCounter = 1

					if err := c.Scheduler.Revive(""); err == nil {
						log.Info("Revive'd framework.")
						isSupressed = false
					} else {
						log.Warnf("Failed to suppress framework: %v", err)
						return err
					}

					if a, err := c.Storage.LastAutoscale(); err == nil {
						lastAutoscale = a
					} else {
						log.Warnf("Failed to read last autoscale time: %v", err)
						log.Warn("Since I couldn't get the autoscale time, I will shutdown.")
						return err
					}
				} else {
					pipelineWatch = nil
					tasksWatch = nil
					if reconcileTimer != nil {
						reconcileTimer.Stop()
					}
				}
				isLeader = isNowLeader
			}
		case _, ok := <-reconcileTimer.C:
			if ok {
				if len(reconcileTasks) > 0 && isLeader {
					log.Debug("Notifing mesos of reconcile...")
					if err := c.Scheduler.Reconcile(reconcileTasks); err != nil {
						log.Warnf("Failed to call reconcile task: %v", err.Error())
						return err
					}
				}
			} else {
				reconcileTimer.C = nil
			}
		case pong := <-c.ping:
			pong <- Pong{
				OK:        true,
				Leader:    isLeader,
				Uptime:    time.Since(controllerStart),
				Version:   c.version,
				Pipelines: len(pipelines),
				Tasks:     len(tasks),
			}
		case <-c.quit:
			log.Debug("Tearing down framework...")
			c.Scheduler.Teardown()
			return nil
		}
	}
}

func (c *Controller) Ping() Pong {
	p := make(chan Pong, 1)
	c.ping <- p
	return <-p
}

func (c *Controller) tryShutdown() {
	if atomic.CompareAndSwapInt32(&c.hasQuit, 0, 1) {
		close(c.quit)
	}
}

func (c *Controller) Shutdown() {
	c.tryShutdown()
	c.wg.Wait()
}

func (c *Controller) OnLeadershipChange(isLeader bool) {
	select {
	case c.LeaderChange <- isLeader:
	case <-c.quit:
	}
}

func (c *Controller) OnSchedulerEvent(s mesos.Scheduler, event msg.Event) {
	select {
	case c.MesosEvents <- event:
	case <-c.quit:
	}
}
