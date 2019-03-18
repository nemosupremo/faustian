# Faustian

Faustian is an [Apache Mesos](https://mesos.apache.org) framework for launching and managing [Faust](https://github.com/robinhood/faust) workers. This service was envisioned to provide a tighter integration between Mesos, Faust, monitoring and our cloud infrastructure.

## Considerations

The functionality currently provided by this framework may be better achieved by using [Marathon](https://github.com/mesosphere/marathon) + Pods and moving the autoscaling & monitoring portions to a separate service. However, we continued developing this framework as Marathon does not yet support multiple Mesos roles, which mean we would have to run another marathon cluster as we wanted to keep our data processing and our general purpose Mesos resources separate.

## Configuration

Faustian can be configure from the command line or using environment variables.

| CLI Flag | Environment Variable | Default | Description
| -------- | -------------------- | ------- | -----------
| `--listen-addr` | `LISTEN_ADDR` | `:1424` | The listen address for the Faustian HTTP API.
| `--hostname` | `HOSTNAME` | *OS Hostname* | The hostname that Faustian will use to advertise itself.
| `--mesos-master` | `MESOS_MASTER` | *n/a* | Either a comma seperated list of hostnames leading to the mesos master, or Zookeeper URI to discover the mesos masters from zookeeper.
| `--mesos-user` | `MESOS_USER` | `root` | The OS User that the Mesos Executor will use to launch tasks.
| `--zookeeper` | `ZOOKEEPER` | *n/a* | A Zookeeper URI specifiying the hosts and path of the zookeeper cluster to store our configuration data.
| `--roles` | `ROLES` | `*` | A list of mesos roles that this framework should receive offers from.
| `--autoscaling-provider` | `AUTOSCALING_PROVIDER` | *n/a* | The cloud service this framework should use for autoscaling. Only `aws` is supported.
| `--autoscaling-default-role` | `AUTOSCALING_DEFAULT_ROLE` | *n/a*  | If a pipeline does not specify a role, for the purpose of autoscaling, it will be assumed the pipeline is under this role.
| `--vault-addr` | `VAULT_ADDR` | *n/a*  | [Vault](https://www.vaultproject.io/) address
| `--vault-token` | `VAULT_TOKEN` | *n/a* | Vault token
| `--gatekeeper-addr` | `GATEKEEPER_ADDR` | *n/a* | [Gatekeeper](https://github.com/nemosupremo/vault-gatekeeper) address
| `--aws-access-key-id` | `AWS_ACCESS_KEY_ID` | *n/a* | AWS Access Key ID for authentication to AWS services.
| `--aws-secret-access-key` | `AWS_SECRET_ACCESS_KEY` | *n/a* | AWS Secret Key for authentication to AWS services.
| `--aws-asg-map` | `AWS_ASG_MAP` | *n/a* | A list of key/value pairs (seperated by `:`) of mesos roles to autoscaling group names. Faustian will use this map to ensure each role has appropriate resources in AWS.

## Pipeline Definition

Faustian treats your consumer group of worker processes as a pipeline. Each worker in a pipeline is expected to be a docker image with an `ENTRYPOINT` directly into the faust worker. This means your dockerfile must have an `ENTRYPOINT` of either `ENTRYPOINT ["faust", "-A", "application"]` or if your python application calls `app.main()` `ENTRYPOINT ["python", "app.py"]`

```js
{
  "id": "string", // must be unique per pipeline
  "container": {
    "image": "string", // Docker image if your faust worker
    "network": "HOST" // Networking type. Either HOST or BRIDGE. If using BRIDGE, Faustian uses the `mesos-bridge` network
  },
  "environment": {
  }, // environment variables applied to all processes
  "processes": { // Faustian uses TASK_GROUPS (aka pods in marathon) to run a group of workers at the same time. This is useful if you want to run multiple agents in different processes.
    "parse": {
      "id": "parse", // id for this worker, must match the key name
      "arguments": ["worker", "-l", "info"], // command line arguments for this worker
      "environment": {
      }, // environment variables for just this work
      "resources": {
        "cpu": 0.29,
        "mem": 512
      }, // resources for this task
      "port_mappings": [{
        "host_port": 0,
        "container_port": 6066,
        "protocol": "tcp"
      }], // Port mappings, like in Marathon. This only applies when the network mode is BRIDGE.
      "kill_grace_period": 30 // kill grace period, this is how long we will wait for the application to quit before sending a KILL
    }
  },
  "executor_resources": {
    "cpu": 0.01,
    "mem": 32,
    "disk": 1
  }, // executor resourcess
  "instances": 5 // number of worker instances
}
```

## HTTP API

### `POST /pipelines`

Create a new pipeline. Returns a 204 on success.

### `PATCH /pipelines/{pipeline-id}`

Update a pipeline. If only the number of instances are changed, then new workers will be added/removed. If anything else changes all the old workers are killed and relaunched.

## License

[MIT](http://opensource.org/licenses/MIT)