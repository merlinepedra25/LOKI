package logql

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/loki/pkg/logproto"
)

var nilMetrics = NewShardingMetrics(nil)

func TestShardMappingEquivalence(t *testing.T) {
	var (
		shards   = 3
		nStreams = 60
		rounds   = 20
		streams  = randomStreams(nStreams, rounds+1, shards, []string{"a", "b", "c", "d"})
		start    = time.Unix(0, 0)
		end      = time.Unix(0, int64(time.Second*time.Duration(rounds)))
		step     = time.Second
		interval = time.Duration(0)
		limit    = 100
	)

	for _, tc := range []struct {
		query       string
		approximate bool
	}{
		{`1`, false},
		{`1 + 1`, false},
		{`{a="1"}`, false},
		{`{a="1"} |= "number: 10"`, false},
		{`rate({a=~".+"}[1s])`, false},
		{`sum by (a) (rate({a=~".+"}[1s]))`, false},
		{`sum(rate({a=~".+"}[1s]))`, false},
		{`max without (a) (rate({a=~".+"}[1s]))`, false},
		{`count(rate({a=~".+"}[1s]))`, false},
		{`avg(rate({a=~".+"}[1s]))`, true},
		{`avg(rate({a=~".+"}[1s])) by (a)`, true},
		{`1 + sum by (cluster) (rate({a=~".+"}[1s]))`, false},
		{`sum(max(rate({a=~".+"}[1s])))`, false},
		{`max(count(rate({a=~".+"}[1s])))`, false},
		{`max(sum by (cluster) (rate({a=~".+"}[1s]))) / count(rate({a=~".+"}[1s]))`, false},
		// topk prefers already-seen values in tiebreakers. Since the test data generates
		// the same log lines for each series & the resulting promql.Vectors aren't deterministically
		// sorted by labels, we don't expect this to pass.
		// We could sort them as stated, but it doesn't seem worth the performance hit.
		// {`topk(3, rate({a=~".+"}[1s]))`, false},
	} {
		q := NewMockQuerier(
			shards,
			streams,
		)

		opts := EngineOpts{}
		regular := NewEngine(opts, q, NoLimits, log.NewNopLogger())
		sharded := NewDownstreamEngine(opts, MockDownstreamer{regular}, nilMetrics, NoLimits, log.NewNopLogger())

		t.Run(tc.query, func(t *testing.T) {
			params := NewLiteralParams(
				tc.query,
				start,
				end,
				step,
				interval,
				logproto.FORWARD,
				uint32(limit),
				nil,
			)
			qry := regular.Query(params)
			ctx := user.InjectOrgID(context.Background(), "fake")

			mapper, err := NewShardMapper(shards, nilMetrics)
			require.Nil(t, err)
			_, mapped, err := mapper.Parse(tc.query)
			require.Nil(t, err)

			shardedQry := sharded.Query(params, mapped)

			res, err := qry.Exec(ctx)
			require.Nil(t, err)

			shardedRes, err := shardedQry.Exec(ctx)
			require.Nil(t, err)

			if tc.approximate {
				approximatelyEquals(t, res.Data.(promql.Matrix), shardedRes.Data.(promql.Matrix))
			} else {
				require.Equal(t, res.Data, shardedRes.Data)
			}
		})
	}
}

func TestRangeMappingEquivalence(t *testing.T) {
	var (
		shards   = 3
		nStreams = 60
		rounds   = 20
		streams  = randomStreams(nStreams, rounds+1, shards, []string{"a", "b", "c", "d"})
		start    = time.Unix(0, 0)
		end      = time.Unix(0, int64(time.Second*time.Duration(rounds)))
		step     = time.Second
		interval = time.Duration(0)
		limit    = 100
	)

	for _, tc := range []struct {
		query string
	}{
		// Range vector aggregators
		{`bytes_over_time({a=~".+"}[5s])`},
		{`count_over_time({a=~".+"}[5s])`},
		{`sum_over_time({a=~".+"} | unwrap a [5s])`},

		{`max_over_time({a=~".+"} | unwrap a [5s])`},
		{`max_over_time({a=~".+"} | unwrap a [5s]) by (a)`},
		{`max_over_time({a=~".+"} | unwrap a [5s]) without (b)`},

		{`min_over_time({a=~".+"} | unwrap a [5s])`},
		{`min_over_time({a=~".+"} | unwrap a [5s]) by (a)`},
		{`min_over_time({a=~".+"} | unwrap a [5s]) without (b)`},

		// Vector aggregator - sum
		{`sum(bytes_over_time({a=~".+"}[5s]))`},
		{`sum(bytes_over_time({a=~".+"}[5s])) by (a)`},
		{`sum(bytes_over_time({a=~".+"}[5s])) without (a)`},

		{`sum(count_over_time({a=~".+"}[5s]))`},
		{`sum(count_over_time({a=~".+"}[5s])) by (a)`},
		{`sum(count_over_time({a=~".+"}[5s])) without (a)`},

		{`sum(sum_over_time({a=~".+"} | unwrap a [5s]))`},
		{`sum(sum_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`sum(sum_over_time({a=~".+"} | unwrap a [5s])) without (a)`},

		{`sum(max_over_time({a=~".+"} | unwrap a [5s]))`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`sum(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		{`sum(min_over_time({a=~".+"} | unwrap a [5s]))`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`sum(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		// Vector aggregator - count
		{`count(bytes_over_time({a=~".+"}[5s]))`},
		{`count(bytes_over_time({a=~".+"}[5s])) by (a)`},

		{`count(count_over_time({a=~".+"}[5s]))`},
		{`count(count_over_time({a=~".+"}[5s])) by (a)`},

		{`count(sum_over_time({a=~".+"} | unwrap a [5s]))`},
		{`count(sum_over_time({a=~".+"} | unwrap a [5s])) by (a)`},

		{`count(max_over_time({a=~".+"} | unwrap a [5s]))`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`count(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		{`count(min_over_time({a=~".+"} | unwrap a [5s]))`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`count(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		// Vector aggregator - max
		{`max(bytes_over_time({a=~".+"}[5s]))`},
		{`max(bytes_over_time({a=~".+"}[5s])) by (a)`},

		{`max(count_over_time({a=~".+"}[5s]))`},
		{`max(count_over_time({a=~".+"}[5s])) by (a)`},

		{`max(sum_over_time({a=~".+"} | unwrap a [5s]))`},
		{`max(sum_over_time({a=~".+"} | unwrap a [5s])) by (a)`},

		{`max(max_over_time({a=~".+"} | unwrap a [5s]))`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`max(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		{`max(min_over_time({a=~".+"} | unwrap a [5s]))`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`max(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		// Vector aggregator - min
		{`min(bytes_over_time({a=~".+"}[5s]))`},
		{`min(bytes_over_time({a=~".+"}[5s])) by (a)`},

		{`min(count_over_time({a=~".+"}[5s]))`},
		{`min(count_over_time({a=~".+"}[5s])) by (a)`},

		{`min(sum_over_time({a=~".+"} | unwrap a [5s]))`},
		{`min(sum_over_time({a=~".+"} | unwrap a [5s])) by (a)`},

		{`min(max_over_time({a=~".+"} | unwrap a [5s]))`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`min(max_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},

		{`min(min_over_time({a=~".+"} | unwrap a [5s]))`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s])) by (a)`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s])) without (a)`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) by (a)`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) by (a)`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s]) by (a)) without (a)`},
		{`min(min_over_time({a=~".+"} | unwrap a [5s]) without (a)) without (a)`},
	} {
		q := NewMockQuerier(
			shards,
			streams,
		)

		opts := EngineOpts{}
		regularEngine := NewEngine(opts, q, NoLimits, log.NewNopLogger())
		downstreamEngine := NewDownstreamEngine(opts, MockDownstreamer{regularEngine}, nilMetrics, NoLimits, log.NewNopLogger())

		t.Run(tc.query, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "fake")

			params := NewLiteralParams(
				tc.query,
				start,
				end,
				step,
				interval,
				logproto.FORWARD,
				uint32(limit),
				nil,
			)

			// Regular engine
			qry := regularEngine.Query(params)
			res, err := qry.Exec(ctx)
			require.Nil(t, err)

			// Downstream engine - split by range
			rangeMapper, err := NewRangeVectorMapper(time.Second)
			require.Nil(t, err)
			_, rangeExpr, err := rangeMapper.Parse(tc.query)
			require.Nil(t, err)

			rangeQry := downstreamEngine.Query(params, rangeExpr)
			rangeRes, err := rangeQry.Exec(ctx)
			require.Nil(t, err)

			require.Equal(t, res.Data, rangeRes.Data)
		})
	}
}

// approximatelyEquals ensures two responses are approximately equal, up to 6 decimals precision per sample
func approximatelyEquals(t *testing.T, as, bs promql.Matrix) {
	require.Equal(t, len(as), len(bs))

	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		require.Equal(t, a.Metric, b.Metric)
		require.Equal(t, len(a.Points), len(b.Points))

		for j := 0; j < len(a.Points); j++ {
			aSample := &a.Points[j]
			aSample.V = math.Round(aSample.V*1e6) / 1e6
			bSample := &b.Points[j]
			bSample.V = math.Round(bSample.V*1e6) / 1e6
		}
		require.Equal(t, a, b)
	}
}
