package v2

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/loki/pkg/lokifrontend/frontend/v2/frontendv2pb"
	"github.com/grafana/loki/pkg/scheduler/schedulerpb"
)

// Worker managing single gRPC connection to Scheduler. Each worker starts multiple goroutines for forwarding
// requests and cancellations to scheduler.
type worker struct {
	log log.Logger

	conn          *grpc.ClientConn
	concurrency   int
	schedulerAddr string
	frontendAddr  string

	// Context and cancellation used by individual goroutines.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Shared between all frontend workers.
	requestCh <-chan *frontendRequest

	// Cancellation requests for this scheduler are received via this channel. It is passed to frontend after
	// query has been enqueued to scheduler.
	cancelCh chan uint64
}

func newWorker(conn *grpc.ClientConn, schedulerAddr string, frontendAddr string, requestCh <-chan *frontendRequest, concurrency int, log log.Logger) *worker {
	w := &worker{
		log:           log,
		conn:          conn,
		concurrency:   concurrency,
		schedulerAddr: schedulerAddr,
		frontendAddr:  frontendAddr,
		requestCh:     requestCh,
		// Allow to enqueue enough cancellation requests. ~ 8MB memory size.
		cancelCh: make(chan uint64, 1000000),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w
}

func (w *worker) start() {
	client := schedulerpb.NewSchedulerForFrontendClient(w.conn)
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.runOne(w.ctx, client)
		}()
	}
}

func (w *worker) stop() {
	w.cancel()
	w.wg.Wait()
	if err := w.conn.Close(); err != nil {
		level.Error(w.log).Log("msg", "error while closing connection to scheduler", "err", err)
	}
}

func (w *worker) runOne(ctx context.Context, client schedulerpb.SchedulerForFrontendClient) {
	backoffConfig := backoff.Config{
		MinBackoff: 500 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
	}

	backoff := backoff.New(ctx, backoffConfig)
	for backoff.Ongoing() {
		loop, loopErr := client.FrontendLoop(ctx)
		if loopErr != nil {
			level.Error(w.log).Log("msg", "error contacting scheduler", "err", loopErr, "addr", w.schedulerAddr)
			backoff.Wait()
			continue
		}

		loopErr = w.schedulerLoop(loop)
		if closeErr := loop.CloseSend(); closeErr != nil {
			level.Debug(w.log).Log("msg", "failed to close frontend loop", "err", loopErr, "addr", w.schedulerAddr)
		}

		if loopErr != nil {
			level.Error(w.log).Log("msg", "error sending requests to scheduler", "err", loopErr, "addr", w.schedulerAddr)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

func (w *worker) schedulerLoop(loop schedulerpb.SchedulerForFrontend_FrontendLoopClient) error {
	if err := loop.Send(&schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.INIT,
		FrontendAddress: w.frontendAddr,
	}); err != nil {
		return err
	}

	if resp, err := loop.Recv(); err != nil || resp.Status != schedulerpb.OK {
		if err != nil {
			return err
		}
		return errors.Errorf("unexpected status received for init: %v", resp.Status)
	}

	ctx := loop.Context()

	for {
		select {
		case <-ctx.Done():
			// No need to report error if our internal context is canceled. This can happen during shutdown,
			// or when scheduler is no longer resolvable. (It would be nice if this context reported "done" also when
			// connection scheduler stops the call, but that doesn't seem to be the case).
			//
			// Reporting error here would delay reopening the stream (if the worker context is not done yet).
			level.Debug(w.log).Log("msg", "stream context finished", "err", ctx.Err())
			return nil

		case req := <-w.requestCh:
			err := loop.Send(&schedulerpb.FrontendToScheduler{
				Type:            schedulerpb.ENQUEUE,
				QueryID:         req.queryID,
				UserID:          req.userID,
				HttpRequest:     req.request,
				FrontendAddress: w.frontendAddr,
				StatsEnabled:    req.statsEnabled,
			})
			if err != nil {
				req.enqueue <- enqueueResult{status: failed}
				return err
			}

			resp, err := loop.Recv()
			if err != nil {
				req.enqueue <- enqueueResult{status: failed}
				return err
			}

			switch resp.Status {
			case schedulerpb.OK:
				req.enqueue <- enqueueResult{status: waitForResponse, cancelCh: w.cancelCh}
				// Response will come from querier.

			case schedulerpb.SHUTTING_DOWN:
				// Scheduler is shutting down, report failure to enqueue and stop this loop.
				req.enqueue <- enqueueResult{status: failed}
				return errors.New("scheduler is shutting down")

			case schedulerpb.ERROR:
				req.enqueue <- enqueueResult{status: waitForResponse}
				req.response <- &frontendv2pb.QueryResultRequest{
					HttpResponse: &httpgrpc.HTTPResponse{
						Code: http.StatusInternalServerError,
						Body: []byte(err.Error()),
					},
				}

			case schedulerpb.TOO_MANY_REQUESTS_PER_TENANT:
				req.enqueue <- enqueueResult{status: waitForResponse}
				req.response <- &frontendv2pb.QueryResultRequest{
					HttpResponse: &httpgrpc.HTTPResponse{
						Code: http.StatusTooManyRequests,
						Body: []byte("too many outstanding requests"),
					},
				}
			default:
				level.Error(w.log).Log("msg", "unknown response status from the scheduler", "status", resp.Status, "queryID", req.queryID)
				req.enqueue <- enqueueResult{status: failed}
			}

		case reqID := <-w.cancelCh:
			err := loop.Send(&schedulerpb.FrontendToScheduler{
				Type:    schedulerpb.CANCEL,
				QueryID: reqID,
			})
			if err != nil {
				return err
			}

			resp, err := loop.Recv()
			if err != nil {
				return err
			}

			// Scheduler may be shutting down, report that.
			if resp.Status != schedulerpb.OK {
				return errors.Errorf("unexpected status received for cancellation: %v", resp.Status)
			}
		}
	}
}
