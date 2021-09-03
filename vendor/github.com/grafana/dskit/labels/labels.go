package labels

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
)

// DeleteMatchingLabels removes metric with labels matching the filter.
func DeleteMatchingLabels(c CollectorVec, filter map[string]string) error {
	lbls, err := GetLabels(c, filter)
	if err != nil {
		return err
	}

	for _, ls := range lbls {
		c.Delete(ls.Map())
	}

	return nil
}

// GetLabels returns list of label combinations used by this collector at the time of call.
// This can be used to find and delete unused metrics.
func GetLabels(c prometheus.Collector, filter map[string]string) ([]labels.Labels, error) {
	ch := make(chan prometheus.Metric, 16)

	go func() {
		defer close(ch)
		c.Collect(ch)
	}()

	errs := tsdberrors.NewMulti()
	var result []labels.Labels
	dtoMetric := &dto.Metric{}
	lbls := labels.NewBuilder(nil)

nextMetric:
	for m := range ch {
		err := m.Write(dtoMetric)
		if err != nil {
			errs.Add(err)
			// We cannot return here, to avoid blocking goroutine calling c.Collect()
			continue
		}

		lbls.Reset(nil)
		for _, lp := range dtoMetric.Label {
			n := lp.GetName()
			v := lp.GetValue()

			filterValue, ok := filter[n]
			if ok && filterValue != v {
				continue nextMetric
			}

			lbls.Set(lp.GetName(), lp.GetValue())
		}
		result = append(result, lbls.Labels())
	}

	return result, errs.Err()
}
