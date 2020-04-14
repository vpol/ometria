package main

import (
	"context"
	"flag"
	"github.com/vpol/ometria/internal/ometria"
	"github.com/vpol/ometria/internal/scheduler/simple"
	"github.com/vpol/ometria/internal/sources/mailchimp"
	"github.com/vpol/ometria/internal/state/disk"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// available modes: once, damon
var mode string

func init() {
	flag.StringVar(&mode, "importer_mode", "once", "run mode")
}

func main() {

	flag.Parse()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSTOP)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-stop
		cancel()
	}()

	source := mailchimp.NewClient()
	log.Printf("client initialised")

	destination := ometria.NewClient()

	log.Printf("destination initialised")

	runner := simple.New(ctx, source, disk.New(), destination)
	log.Printf("scheduler initialised")

	switch mode {

	case "once":
		runner.RunOnce()
		cancel()
		close(stop)
	case "daemon":
		runner.Run()
	}

	<-ctx.Done()

}
