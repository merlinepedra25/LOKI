package chunk

import (
	"context"
	"math"
)

// QueryCallback from an IndexQuery.
type QueryCallback func(IndexQuery, ReadBatch) bool

// DoSingleQuery is the interface for indexes that don't support batching yet.
type DoSingleQuery func(context.Context, IndexQuery, Callback) error

// QueryParallelism is the maximum number of subqueries run in
// parallel per higher-level query.
var QueryParallelism = 100

// DoParallelQueries translates between our interface for query batching,
// and indexes that don't yet support batching.
func DoParallelQueries(
	ctx context.Context, doSingleQuery DoSingleQuery, queries []IndexQuery,
	callback Callback,
) error {
	if len(queries) == 1 {
		return doSingleQuery(ctx, queries[0], callback)
	}

	queue := make(chan IndexQuery)
	incomingErrors := make(chan error)
	n := math.Min(len(queries), QueryParallelism)
	// Run n parallel goroutines fetching queries from the queue
	for i := 0; i < n; i++ {
		go func() {
			sp, ctx := ot.StartSpanFromContext(ctx, "DoParallelQueries-worker")
			defer sp.Finish()
			for {
				query, ok := <-queue
				if !ok {
					return
				}
				incomingErrors <- doSingleQuery(ctx, query, callback)
			}
		}()
	}
	// Send all the queries into the queue
	go func() {
		for _, query := range queries {
			queue <- query
		}
		close(queue)
	}()

	// Now receive all the results.
	var lastErr error
	for i := 0; i < len(queries); i++ {
		err := <-incomingErrors
		if err != nil {

			lastErr = err
		}
	}
	return lastErr
}
