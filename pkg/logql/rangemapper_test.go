package logql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_SplitRangeVectorMapping(t *testing.T) {
	rvm, err := NewRangeVectorMapper(time.Minute)
	require.NoError(t, err)

	for _, tc := range []struct {
		expr       string
		expected   string
		expectNoop bool
	}{
		{
			`count_over_time({app="foo"}[3m])`,
			`sum without(downstream<count_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m]), shard=<nil>>)`,
			false,
		},

		{
			`sum(count_over_time({app="foo"}[3m]))`,
			`sum(
				downstream<sum without(count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum without(count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum without(count_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},

		{
			`sum by (bar) (count_over_time({app="foo"}[3m]))`,
			`sum by (bar) (downstream<sum by (bar) (count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (count_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},

		{
			`min(count_over_time({app="foo"}[3m]))`,
			`min(sum without(
				downstream<count_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m]), shard=<nil>>)`,
			false,
		},

		{
			`min by (bar) (count_over_time({app="foo"}[3m]))`,
			`min by (bar)(
				downstream<sum by (bar)(count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar)(count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar)(count_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},

		{
			`sum(min(count_over_time({app="foo"}[3m])))`,
			`sum(min(
				downstream<min(count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<min(count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<min(count_over_time({app="foo"}[1m])), shard=<nil>>))`,
			false,
		},

		{
			`sum(min by (bar)(count_over_time({app="foo"}[3m])))`,
			`sum(min by (bar)(
				downstream<sum(min by (bar)(count_over_time({app="foo"}[1m] offset 2m0s))), shard=<nil>>
				++ downstream<min(count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<min(count_over_time({app="foo"}[1m])), shard=<nil>>))`,
			false,
		},

		// sum
		{
			`sum(bytes_over_time({app="foo"}[3m]))`,
			`sum(downstream<sum(bytes_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum(bytes_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum(bytes_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum by (bar) (bytes_over_time({app="foo"}[3m]))`,
			`sum by (bar) (downstream<sum by (bar) (bytes_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (bytes_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (bytes_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum(count_over_time({app="foo"}[3m]))`,
			`sum(downstream<sum(count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum(count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum(count_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum by (bar) (count_over_time({app="foo"}[3m]))`,
			`sum by (bar) (downstream<sum by (bar) (count_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (count_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (count_over_time({app="foo"}[1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum(sum_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum(downstream<sum(sum_over_time({app="foo"} | unwrap bar [1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum(sum_over_time({app="foo"} | unwrap bar [1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum(sum_over_time({app="foo"} | unwrap bar [1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum by (bar) (sum_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m])), shard=<nil>>)`,
			false,
		},

		// sum - TODO: Fix
		{
			`sum(max_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum(max_over_time({app="foo"} | unwrap bar [3m]))`,
			true,
		},
		{
			`sum by (bar) (max_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (max_over_time({app="foo"} | unwrap bar [3m]))`,
			true,
		},
		{
			`sum(max_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			`sum(max_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			true,
		},
		{
			`sum by (bar) (max_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			`sum by (bar) (max_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			true,
		},
		{
			`sum(min_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum(min_over_time({app="foo"} | unwrap bar [3m]))`,
			true,
		},
		{
			`sum by (bar) (min_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (min_over_time({app="foo"} | unwrap bar [3m]))`,
			true,
		},
		{
			`sum(min_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			`sum(min_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			true,
		},
		{
			`sum by (bar) (min_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			`sum by (bar) (min_over_time({app="foo"} | unwrap bar [3m]) by (bar))`,
			true,
		},

		// count, max, min - TODO: Fix

		// noop - TODO: Fix
		{
			`bytes_over_time({app="foo"}[3m])`,
			`bytes_over_time({app="foo"}[3m])`,
			true,
		},
		{
			`count_over_time({app="foo"}[3m])`,
			`count_over_time({app="foo"}[3m])`,
			true,
		},
		{
			`sum_over_time({app="foo"} | unwrap bar [3m])`,
			`sum_over_time({app="foo"} | unwrap bar [3m])`,
			true,
		},
		{
			`max_over_time({app="foo"} | unwrap bar [3m])`,
			`max_over_time({app="foo"} | unwrap bar [3m])`,
			true,
		},
		{
			`max_over_time({app="foo"} | unwrap bar [3m]) by (bar)`,
			`max_over_time({app="foo"} | unwrap bar [3m]) by (bar)`,
			true,
		},
		{
			`min_over_time({app="foo"} | unwrap bar [3m])`,
			`min_over_time({app="foo"} | unwrap bar [3m])`,
			true,
		},
		{
			`min_over_time({app="foo"} | unwrap bar [3m]) by (bar)`,
			`min_over_time({app="foo"} | unwrap bar [3m]) by (bar)`,
			true,
		},

		// TODO: Add binary operations
		// TODO: Add cases where the aggregation function is non-splittable, e.g., topk(2, sum(bytes_over_time({app="foo"}[3m])))
		// TODO: Add non-splittable expressions
		// TODO: Add noop if range interval is slower or equal to split interval
	} {
		tc := tc
		t.Run(tc.expr, func(t *testing.T) {
			//t.Parallel()
			noop, mappedExpr, err := rvm.Parse(tc.expr)
			require.NoError(t, err)
			require.Equal(t, removeWhiteSpace(tc.expected), removeWhiteSpace(mappedExpr.String()))
			require.Equal(t, tc.expectNoop, noop)
		})
	}
}

func Test_FailQuery(t *testing.T) {
	rvm, err := NewRangeVectorMapper(time.Minute)
	require.NoError(t, err)
	_, _, err = rvm.Parse(`{app="foo"} |= "err"`)
	require.Error(t, err)
}
