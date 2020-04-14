package scheduler

import (
	"flag"
	"time"
)

var Period time.Duration

func init() {
	flag.DurationVar(&Period, "scheduler_period", 60*time.Second, "scheduler period (duration)")
}