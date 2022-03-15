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
			`bytes_over_time({app="foo"}[3m])`,
			`sum without(downstream<bytes_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<bytes_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<bytes_over_time({app="foo"}[1m]), shard=<nil>>)`,
			false,
		},
		{
			`count_over_time({app="foo"}[3m])`,
			`sum without(downstream<count_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m]), shard=<nil>>)`,
			false,
		},
		{
			`sum_over_time({app="foo"} | unwrap bar [3m])`,
			`sum without(downstream<sum_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<sum_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<sum_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>)`,
			false,
		},
		{
			`max_over_time({app="foo"} | unwrap bar [3m])`,
			`max without(downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>)`,
			false,
		},
		{
			`max_over_time ({app="foo"} | unwrap bar [3m]) by (baz)`,
			`max by (baz) (downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 2m0s) by (baz), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 1m0s) by (baz), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m]) by (baz), shard=<nil>>)`,
			false,
		},
		{
			`min_over_time({app="foo"} | unwrap bar [3m])`,
			`min without(downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>)`,
			false,
		},
		{
			`min_over_time ({app="foo"} | unwrap bar [3m]) by (baz)`,
			`min by (baz) (downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 2m0s) by (baz), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 1m0s) by (baz), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m]) by (baz), shard=<nil>>)`,
			false,
		},

		// Vector aggregator
		// sum
		{
			`sum(bytes_over_time({app="foo"}[3m]))`,
			`sum(sum without (downstream<bytes_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<bytes_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<bytes_over_time({app="foo"}[1m]), shard=<nil>>))`,
			false,
		},
		{
			`sum(count_over_time({app="foo"}[3m]))`,
			`sum(sum without (downstream<count_over_time({app="foo"}[1m] offset 2m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m] offset 1m0s), shard=<nil>>
				++ downstream<count_over_time({app="foo"}[1m]), shard=<nil>>))`,
			false,
		},
		{
			`sum(sum_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum(sum without (downstream<sum_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<sum_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<sum_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>))`,
			false,
		},
		{
			`sum(max_over_time({app="foo"}  | unwrap bar [3m]))`,
			`sum(max without (downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<max_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>))`,
			false,
		},
		{
			`sum(min_over_time({app="foo"}  | unwrap bar [3m]))`,
			`sum(min without (downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 2m0s), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m] offset 1m0s), shard=<nil>>
				++ downstream<min_over_time({app="foo"} | unwrap bar [1m]), shard=<nil>>))`,
			false,
		},

		// sum by
		{
			`sum by (bar) (bytes_over_time({app="foo"}[3m]))`,
			`sum by (bar) (downstream<sum by (bar) (bytes_over_time({app="foo"}[1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (bytes_over_time({app="foo"}[1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (bytes_over_time({app="foo"}[1m])), shard=<nil>>)`,
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
			`sum by (bar) (sum_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (sum_over_time({app="foo"} | unwrap bar [1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum by (bar) (max_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (downstream<sum by (bar) (max_over_time({app="foo"} | unwrap bar [1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (max_over_time({app="foo"} | unwrap bar [1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (max_over_time({app="foo"} | unwrap bar [1m])), shard=<nil>>)`,
			false,
		},
		{
			`sum by (bar) (min_over_time({app="foo"} | unwrap bar [3m]))`,
			`sum by (bar) (downstream<sum by (bar) (min_over_time({app="foo"} | unwrap bar [1m] offset 2m0s)), shard=<nil>>
				++ downstream<sum by (bar) (min_over_time({app="foo"} | unwrap bar [1m] offset 1m0s)), shard=<nil>>
				++ downstream<sum by (bar) (min_over_time({app="foo"} | unwrap bar [1m])), shard=<nil>>)`,
			false,
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
