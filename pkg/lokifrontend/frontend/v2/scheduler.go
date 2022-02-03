package v2

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"google.golang.org/grpc"

	"github.com/grafana/loki/pkg/util"
)

type scheduler struct {
	services.Service

	cfg             Config
	logger          log.Logger
	frontendAddress string

	// Channel with requests that should be forwarded to the scheduler.
	requestsCh <-chan *frontendRequest

	watcher services.Service

	mu sync.Mutex
	// Set to nil when stop is called... no more workers are created afterwards.
	workers map[string]*worker
}

func newScheduler(cfg Config, frontendAddress string, ring ring.ReadRing, requestsCh <-chan *frontendRequest, logger log.Logger) (*scheduler, error) {
	f := &scheduler{
		cfg:             cfg,
		logger:          logger,
		frontendAddress: frontendAddress,
		requestsCh:      requestsCh,
		workers:         map[string]*worker{},
	}

	switch {
	case ring != nil:
		// Use the scheduler ring and RingWatcher to find schedulers.
		w, err := util.NewRingWatcher(log.With(logger, "component", "frontend-scheduler-worker"), ring, cfg.DNSLookupPeriod, f)
		if err != nil {
			return nil, err
		}
		f.watcher = w
	default:
		// If there is no ring config fallback on using DNS for the frontend scheduler worker to find the schedulers.
		w, err := util.NewDNSWatcher(cfg.SchedulerAddress, cfg.DNSLookupPeriod, f)
		if err != nil {
			return nil, err
		}
		f.watcher = w
	}

	f.Service = services.NewIdleService(f.starting, f.stopping)
	return f, nil
}

func (f *scheduler) starting(ctx context.Context) error {
	return services.StartAndAwaitRunning(ctx, f.watcher)
}

func (f *scheduler) stopping(_ error) error {
	err := services.StopAndAwaitTerminated(context.Background(), f.watcher)

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, w := range f.workers {
		w.stop()
	}
	f.workers = nil

	return err
}

func (f *scheduler) AddressAdded(address string) {
	f.mu.Lock()
	ws := f.workers
	w := f.workers[address]

	// Already stopped or we already have worker for this address.
	if ws == nil || w != nil {
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()

	level.Info(f.logger).Log("msg", "adding connection to scheduler", "addr", address)
	conn, err := f.connectToScheduler(context.Background(), address)
	if err != nil {
		level.Error(f.logger).Log("msg", "error connecting to scheduler", "addr", address, "err", err)
		return
	}

	// No worker for this address yet, start a new one.
	w = newWorker(conn, address, f.frontendAddress, f.requestsCh, f.cfg.WorkerConcurrency, f.logger)

	f.mu.Lock()
	defer f.mu.Unlock()

	// Can be nil if stopping has been called already.
	if f.workers == nil {
		return
	}
	// We have to recheck for presence in case we got called again while we were
	// connecting and that one finished first.
	if f.workers[address] != nil {
		return
	}
	f.workers[address] = w
	w.start()
}

func (f *scheduler) AddressRemoved(address string) {
	level.Info(f.logger).Log("msg", "removing connection to scheduler", "addr", address)

	f.mu.Lock()
	// This works fine if f.workers is nil already.
	w := f.workers[address]
	delete(f.workers, address)
	f.mu.Unlock()

	if w != nil {
		w.stop()
	}
}

// Get number of workers.
func (f *scheduler) getWorkersCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.workers)
}

func (f *scheduler) connectToScheduler(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
