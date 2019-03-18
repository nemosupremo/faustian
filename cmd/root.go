package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.SetDefault("log-level", "debug")

	rootCmd.PersistentFlags().String("log-level", viper.GetString("log-level"), "Logging level.")

	viper.BindPFlag("log-level", rootCmd.PersistentFlags().Lookup("log-level"))
}

var rootCmd = &cobra.Command{
	Use:              "faustian [command]",
	Short:            "",
	PersistentPreRun: Setup,
}

var RootCmd = rootCmd

func Setup(cmd *cobra.Command, args []string) {
	if l, err := logrus.ParseLevel(viper.GetString("log-level")); err == nil {
		logrus.SetLevel(l)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}

	logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339, FullTimestamp: true})
}

func Execute(version string) {
	rootCmd.Version = version
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
