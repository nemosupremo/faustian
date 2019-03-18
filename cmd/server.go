package cmd

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nemosupremo/faustian"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const DEFAULT_ZK = "zk://localhost:2181/faustian"
const DEFAULT_MESOS = "zk://localhost:2181/mesos"

type cliArgs struct {
	Name        string
	Default     interface{}
	Description string
}

func init() {

	options := []cliArgs{
		{"listen-addr", ":1424", "Listen address that faustian should listen on."},
		{"hostname", "", "Hostname"},
		{"mesos-master", DEFAULT_MESOS, "Mesos Master"},
		{"mesos-user", "root", "Mesos user for this framework."},
		{"mesos-failover-timeout", 24 * 7 * time.Hour, "Mesos failover timeout"},
		{"zookeeper", DEFAULT_ZK, "Zookeeper"},
		{"roles", []string{"*"}, "Mesos Roles"},

		{"autoscaling-provider", "", "AutoScaling Provider. Only `aws` is supported."},
		{"autoscaling-default-role", "", "AutoScaling default role. If a pipeline does not specify any roles, autoscale based on this role. If blank, such pipelines will not autoscale"},

		{"vault-addr", "", "Vault address"},
		{"vault-token", "", "Vault token"},
		{"gatekeeper-addr", "", "Vault Gatekeeper address"},

		{"aws-access-key-id", "", "AWS Access Key for AutoScaling"},
		{"aws-secret-access-key", "", "AWS Secret Key for AutoScaling"},
		{"aws-asg-map", []string{}, "Map mesos roles to autoscale groups. Format is [role]:[asg groupname]"},
	}

	for _, option := range options {
		viper.SetDefault(option.Name, option.Default)
	}

	for _, option := range options {
		switch option.Default.(type) {
		case string:
			serverCmd.PersistentFlags().String(option.Name, viper.GetString(option.Name), option.Description)
		case bool:
			serverCmd.PersistentFlags().Bool(option.Name, viper.GetBool(option.Name), option.Description)
		case time.Duration:
			serverCmd.PersistentFlags().Duration(option.Name, viper.GetDuration(option.Name), option.Description)
		case []string:
			serverCmd.PersistentFlags().StringSlice(option.Name, viper.GetStringSlice(option.Name), option.Description)
		default:
			panic("Invalid type for option default for option '" + option.Name + "'.")
		}
	}

	rootCmd.AddCommand(serverCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Starts the faustian server.",
	Run:   faustianServer,
}

func intro() {
	fmt.Println("   ___               _   _")
	fmt.Println("  / __\\_ _ _   _ ___| |_(_) __ _ _ __")
	fmt.Println(" / _\\/ _` | | | / __| __| |/ _` | '_ \\")
	fmt.Println("/ / | (_| | |_| \\__ \\ |_| | (_| | | | |")
	fmt.Println("\\/   \\__,_|\\__,_|___/\\__|_|\\__,_|_| |_|")
	fmt.Println("github.com/nemosupremo/faustian")
}

func faustianServer(*cobra.Command, []string) {
	intro()

	var conf faustian.ControllerConfig
	if viper.GetString("hostname") != "" {
		conf.Hostname = viper.GetString("hostname")
	}
	if host, port, err := net.SplitHostPort(viper.GetString("listen-addr")); err == nil {
		if conf.Hostname == "" {
			conf.Hostname = host
		}
		if n, err := strconv.Atoi(port); err == nil {
			conf.Port = n
		} else {
			log.Fatalf("Invalid port: %s", port)
			return
		}
	} else {
		log.Fatalf("Invalid listen-addr: %v", err)
		return
	}
	conf.Zk.Uri = viper.GetString("zookeeper")
	conf.Scheduler.MesosMaster = viper.GetString("mesos-master")
	conf.Scheduler.Name = "Faustian"
	conf.Scheduler.User = viper.GetString("mesos-user")
	conf.Scheduler.Roles = viper.GetStringSlice("roles")
	conf.Scheduler.FailoverTimeout = viper.GetDuration("mesos-failover-timeout")

	conf.Autoscaling.Provider = viper.GetString("autoscaling-provider")
	conf.Autoscaling.DefaultRole = viper.GetString("autoscaling-default-role")
	conf.Aws.AccessKey = viper.GetString("aws-access-key-id")
	conf.Aws.SecretKey = viper.GetString("aws-secret-access-key")
	conf.Aws.AsgMap = viper.GetStringSlice("aws-asg-map")

	if controller, err := faustian.NewController(conf); err == nil {
		running := make(chan struct{})
		go func() {
			defer close(running)
			if err := controller.Run(); err != nil {
				log.Fatalf("Failed to start server controller: %v", err.Error())
			}
		}()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select {
		case <-c:
			log.Warnln("Received Interrupt signal, shutting down server...")
			controller.Shutdown()
		case <-running:
		}
	} else {
		log.Fatalf("Failed to create server controller: %v", err)
		return
	}
}
