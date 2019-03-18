package main

import (
	"runtime"

	"github.com/nemosupremo/faustian/cmd"
)

var (
	Version   = "--dev--"
	BuildTime = "--dev--"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cmd.Execute()
}
