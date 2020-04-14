package simple

import (
	"context"
	"flag"
	"fmt"
	"github.com/vpol/ometria/api"
	"github.com/vpol/ometria/internal/scheduler"
	"github.com/vpol/ometria/internal/state"
	"log"
	"os"
	"strings"
	"time"
)

var (
	idList StringArrayFlags
	logger *log.Logger
)

type StringArrayFlags []string

func (i StringArrayFlags) String() string {
	return strings.Join(i, ",")
}

func (i *StringArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func init() {
	flag.Var(&idList, "mailchimp_list_ids", "comma separated list of if's to import")
	logger = log.New(os.Stdout, "simple_scheduler ", log.LstdFlags)
}

type Simple struct {
	ctx      context.Context
	cancel   context.CancelFunc
	jobsList []string
	source   Source
	storage  state.Storage
	sink     Sink
}

type Source interface {
	Get(ctx context.Context, id string, currObject state.State) (data []api.Ometria, newObject state.State, err error)
}

type Sink interface {
	Put(ctx context.Context, data []api.Ometria) error
}

func New(ctx context.Context, source Source, storage state.Storage, ometria Sink) (s *Simple) {
	s = &Simple{
		storage:  storage,
		sink:     ometria,
		source:   source,
		jobsList: make([]string, 0),
	}

	s.jobsList = idList
	s.ctx, s.cancel = context.WithCancel(ctx)
	return
}

func (s Simple) Run() context.CancelFunc {

	ticker := time.NewTicker(scheduler.Period)

	go func() {
		defer ticker.Stop()

		go s.RunOnce()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				go s.RunOnce()
			}
		}
	}()

	return s.cancel
}

func (s Simple) runJob(ctx context.Context, id string) error {

	oldObject, err := s.storage.Get(id)
	if err != nil {
		return fmt.Errorf("[%s] failed to get storage object: %v", id, err)
	}

	logger.Printf("[%s] old object", id)

	data, newObject, err := s.source.Get(ctx, id, oldObject)

	if err != nil {
		return fmt.Errorf("[%s] failed to retrieve data: %v", id, err)
	}

	logger.Printf("[%s] data, newobject", id)

	if err := s.sink.Put(ctx, data); err != nil {
		return fmt.Errorf("[%s] failed to put data: %v", id, err)
	}

	logger.Printf("[%s] persisted", id)

	if err := s.storage.Update(id, newObject); err != nil {
		return fmt.Errorf("[%s] failed to save storage object: %v", id, err)
	}

	logger.Printf("[%s] new object", id)

	return nil
}

func (s *Simple) RunOnce() {
	for _, j := range s.jobsList {
		if err := s.runJob(s.ctx, j); err != nil {
			log.Printf("failed to execute job %v", err)
		}
	}
}
