package logql

import (
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	util_log "github.com/grafana/loki/pkg/util/log"
)

type RangeVectorMapper struct {
	splitByInterval time.Duration
}

func NewRangeVectorMapper(interval time.Duration) (RangeVectorMapper, error) {
	if interval <= 0 {
		return RangeVectorMapper{}, fmt.Errorf("cannot create RangeVectorMapper with <=0 splitByInterval. Received %s", interval)
	}
	return RangeVectorMapper{
		splitByInterval: interval,
	}, nil
}

// Parse returns (noop, parsed expression, error)
func (m RangeVectorMapper) Parse(query string) (bool, Expr, error) {
	origExpr, err := ParseExpr(query)
	if err != nil {
		return true, nil, err
	}

	modExpr, err := m.Map(origExpr, nil)
	if err != nil {
		return true, nil, err
	}

	return origExpr.String() == modExpr.String(), modExpr, err
}

func (m RangeVectorMapper) Map(expr Expr, grouping *Grouping) (Expr, error) {
	// immediately clone the passed expr to avoid mutating the original
	expr, err := Clone(expr)
	if err != nil {
		return nil, err
	}

	switch e := expr.(type) {
	case *VectorAggregationExpr:
		return m.mapVectorAggregationExpr(e)
	case *RangeAggregationExpr:
		// noop - TODO: the aggregation of range aggregation expressions needs to preserve the labels
		return m.mapRangeAggregationExpr(e, grouping), nil
	case *BinOpExpr:
		lhsMapped, err := m.Map(e.SampleExpr, nil)
		if err != nil {
			return nil, err
		}
		rhsMapped, err := m.Map(e.RHS, nil)
		if err != nil {
			return nil, err
		}
		lhsSampleExpr, ok := lhsMapped.(SampleExpr)
		if !ok {
			return nil, badASTMapping(lhsMapped)
		}
		rhsSampleExpr, ok := rhsMapped.(SampleExpr)
		if !ok {
			return nil, badASTMapping(rhsMapped)
		}
		e.SampleExpr = lhsSampleExpr
		e.RHS = rhsSampleExpr
		return e, nil
	default:
		return nil, errors.Errorf("unexpected expr type (%T) for ASTMapper type (%T) ", expr, m)
	}
}

// getRangeInterval returns the interval in the range vector
// Example: expression `count_over_time({app="foo"}[10m])` returns 10m
func getRangeInterval(expr SampleExpr) time.Duration {
	var rangeInterval time.Duration
	expr.Walk(func(e interface{}) {
		switch concrete := e.(type) {
		case *RangeAggregationExpr:
			rangeInterval = concrete.Left.Interval
		}
	})
	return rangeInterval
}

// mapSampleExpr transform expr in multiple subexpressions split by offset range interval
// rangeInterval should be greater than m.splitByInterval, otherwise the resultant expression
// will have an unnecessary aggregation operation
func (m RangeVectorMapper) mapSampleExpr(expr SampleExpr, rangeInterval time.Duration, grouping *Grouping) SampleExpr {
	var head *ConcatSampleExpr

	splitCount := int(rangeInterval / m.splitByInterval)
	for i := 0; i < splitCount; i++ {
		subExpr, _ := Clone(expr)
		subSampleExpr := subExpr.(SampleExpr)
		offset := time.Duration(i) * m.splitByInterval
		subSampleExpr.Walk(func(e interface{}) {
			switch concrete := e.(type) {
			case *RangeAggregationExpr:
				concrete.Left.Interval = m.splitByInterval
				if offset != 0 {
					concrete.Left.Offset = offset
				}
			}

		})
		head = &ConcatSampleExpr{
			DownstreamSampleExpr: DownstreamSampleExpr{
				SampleExpr: subSampleExpr,
			},
			next: head,
		}
	}

	if head == nil {
		return expr
	}
	return head
}

func (m RangeVectorMapper) mapVectorAggregationExpr(expr *VectorAggregationExpr) (SampleExpr, error) {
	rangeInterval := getRangeInterval(expr)

	// in case the interval is smaller than the configured split interval,
	// don't split it.
	// TODO: what if there is another internal expr with an interval that can be split?
	if rangeInterval <= m.splitByInterval {
		return expr, nil
	}

	grouping := expr.Grouping
	if grouping.Groups == nil || expr.Operation == OpTypeCount {
		grouping = nil
	}

	// Split the vector aggregation child node
	subMapped, err := m.Map(expr.Left, nil)
	if err != nil {
		return nil, err
	}
	sampleExpr, ok := subMapped.(SampleExpr)
	if !ok {
		return nil, badASTMapping(subMapped)
	}

	return &VectorAggregationExpr{
		Left:      sampleExpr,
		Grouping:  expr.Grouping,
		Params:    expr.Params,
		Operation: expr.Operation,
	}, nil
}

func (m RangeVectorMapper) mapRangeAggregationExpr(expr *RangeAggregationExpr, grouping *Grouping) SampleExpr {
	// TODO: In case expr is non-splittable, can we attempt to shard a child node?

	rangeInterval := getRangeInterval(expr)

	// in case the interval is smaller than the configured split interval,
	// don't split it.
	// TODO: what if there is another internal expr with an interval that can be split?
	if rangeInterval <= m.splitByInterval {
		return expr
	}

	if isSplittableByRange(expr) {
		switch expr.Operation {
		case OpRangeTypeBytes, OpRangeTypeCount, OpRangeTypeSum:
			if grouping == nil {
				return &VectorAggregationExpr{
					Left: m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping: &Grouping{
						Without: true,
					},
					Operation: OpTypeSum,
				}
			} else {
				return &VectorAggregationExpr{
					Left:      m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping:  &Grouping{Without: true},
					Operation: OpTypeSum,
				}
			}
		case OpRangeTypeMax:
			if grouping == nil {
				subGrouping := expr.Grouping
				if subGrouping == nil {
					subGrouping = &Grouping{
						Without: true,
					}
				}
				return &VectorAggregationExpr{
					Left:      m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping:  subGrouping,
					Operation: OpTypeMax,
				}
			} else {
				return &VectorAggregationExpr{
					Left:      m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping:  &Grouping{Without: true},
					Operation: OpTypeMax,
				}
			}

		case OpRangeTypeMin:
			if grouping == nil {
				subGrouping := expr.Grouping
				if subGrouping == nil {
					subGrouping = &Grouping{
						Without: true,
					}
				}
				return &VectorAggregationExpr{
					Left:      m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping:  subGrouping,
					Operation: OpTypeMin,
				}
			} else {
				return &VectorAggregationExpr{
					Left:      m.mapSampleExpr(expr, rangeInterval, nil),
					Grouping:  &Grouping{Without: true},
					Operation: OpTypeMin,
				}
			}
		default:
			// this should not be reachable. If an operation is splittable it should
			// have an optimization listed
			level.Warn(util_log.Logger).Log(
				"msg", "unexpected range aggregation expression which appears shardable, ignoring",
				"operation", expr.Operation,
			)
			return expr
		}
	}

	return expr
}

func isSplittableByRange(expr SampleExpr) bool {
	switch e := expr.(type) {
	case *VectorAggregationExpr:
		_, ok := SplittableVectorOp[e.Operation]
		return ok && isSplittableByRange(e.Left)
	case *BinOpExpr:
		return true
	case *LabelReplaceExpr:
		return true
	case *RangeAggregationExpr:
		_, ok := SplittableRangeVectorOp[e.Operation]
		return ok
	default:
		return false
	}
}

var SplittableVectorOp = map[string]struct{}{
	OpTypeSum:   {},
	OpTypeCount: {},
	OpTypeMax:   {},
	OpTypeMin:   {},
}

var SplittableRangeVectorOp = map[string]struct{}{
	OpRangeTypeBytes: {},
	OpRangeTypeCount: {},
	OpRangeTypeSum:   {},
	OpRangeTypeMax:   {},
	OpRangeTypeMin:   {},
}