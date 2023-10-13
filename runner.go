package instributor

import (
	"context"
	"sync"

	"github.com/michalkurzeja/go-clock"
)

type Runner struct {
	inst Instributor

	conf RunnerConfig

	exit chan struct{}
	mx   sync.RWMutex
	key  string
}

func NewRunner(inst Instributor, config RunnerConfig) *Runner {
	return &Runner{
		inst: inst,
		conf: config,
		exit: make(chan struct{}),
	}
}

func (r *Runner) Start(ctx context.Context) error {
	return r.StartNotify(ctx, make(chan struct{}))
}

func (r *Runner) StartNotify(ctx context.Context, started chan<- struct{}) error {
	ticker := clock.Ticker(r.conf.RefreshInterval)
	defer ticker.Stop()

	key, err := r.inst.Acquire(ctx)
	if err != nil {
		return err
	}

	r.setKey(key)
	defer r.setKey("")

	close(started)

	for {
		select {
		case <-r.exit:
			return r.inst.Release(ctx, key)
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, r.conf.AttemptToRefreshFor)
			err := r.inst.Refresh(ctx, key)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func (r *Runner) Stop() {
	close(r.exit)
}

func (r *Runner) Key() string {
	r.mx.RLock()
	defer r.mx.RUnlock()
	return r.key
}

func (r *Runner) setKey(key string) {
	r.mx.Lock()
	r.key = key
	r.mx.Unlock()
}
