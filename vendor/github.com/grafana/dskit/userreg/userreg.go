package userreg

import (
	"bytes"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// UserRegistry holds a Prometheus registry associated with a specific user.
type UserRegistry struct {
	user string               // Set to "" when registry is soft-removed.
	reg  *prometheus.Registry // Set to nil, when registry is soft-removed.

	// Set to last result of Gather() call when removing registry.
	lastGather MetricFamilyMap
}

// UserRegistries holds Prometheus registries for multiple users, guaranteeing
// multi-thread safety and stable ordering.
type UserRegistries struct {
	logger log.Logger
	regsMu sync.Mutex
	regs   []UserRegistry
}

// NewUserRegistries makes new UserRegistries.
func NewUserRegistries(logger log.Logger) *UserRegistries {
	return &UserRegistries{logger: logger}
}

// AddUserRegistry adds an user registry. If user already has a registry,
// previous registry is removed, but latest metric values are preserved
// in order to avoid counter resets.
func (r *UserRegistries) AddUserRegistry(user string, reg *prometheus.Registry) {
	r.regsMu.Lock()
	defer r.regsMu.Unlock()

	// Soft-remove user registry, if user has one already.
	for idx := 0; idx < len(r.regs); {
		if r.regs[idx].user != user {
			idx++
			continue
		}

		if r.softRemoveUserRegistry(&r.regs[idx]) {
			// Keep it.
			idx++
		} else {
			// Remove it.
			r.regs = append(r.regs[:idx], r.regs[idx+1:]...)
		}
	}

	// New registries must be added to the end of the list, to guarantee stability.
	r.regs = append(r.regs, UserRegistry{
		user: user,
		reg:  reg,
	})
}

// RemoveUserRegistry removes all Prometheus registries for a given user.
// If hard is true, registry is removed completely.
// If hard is false, latest registry values are preserved for future aggregations.
func (r *UserRegistries) RemoveUserRegistry(user string, hard bool) {
	r.regsMu.Lock()
	defer r.regsMu.Unlock()

	for idx := 0; idx < len(r.regs); {
		if user != r.regs[idx].user {
			idx++
			continue
		}

		if !hard && r.softRemoveUserRegistry(&r.regs[idx]) {
			idx++ // keep it
		} else {
			r.regs = append(r.regs[:idx], r.regs[idx+1:]...) // remove it.
		}
	}
}

// Returns true, if we should keep latest metrics. Returns false if we failed to gather latest metrics,
// and this can be removed completely.
func (r *UserRegistries) softRemoveUserRegistry(ur *UserRegistry) bool {
	last, err := ur.reg.Gather()
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to gather metrics from registry", "user", ur.user, "err", err)
		return false
	}

	for ix := 0; ix < len(last); {
		// Only keep metrics for which we don't want to go down, since that indicates reset (counter, summary, histogram).
		switch last[ix].GetType() {
		case dto.MetricType_COUNTER, dto.MetricType_SUMMARY, dto.MetricType_HISTOGRAM:
			ix++
		default:
			// Remove gauges and unknowns.
			last = append(last[:ix], last[ix+1:]...)
		}
	}

	// No metrics left.
	if len(last) == 0 {
		return false
	}

	ur.lastGather, err = NewMetricFamilyMap(last)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to gather metrics from registry", "user", ur.user, "err", err)
		return false
	}

	ur.user = ""
	ur.reg = nil
	return true
}

// Registries returns a copy of the user registries list.
func (r *UserRegistries) Registries() []UserRegistry {
	r.regsMu.Lock()
	defer r.regsMu.Unlock()

	out := make([]UserRegistry, 0, len(r.regs))
	out = append(out, r.regs...)

	return out
}

func (r *UserRegistries) BuildMetricFamiliesPerUser() MetricFamiliesPerUser {
	data := MetricFamiliesPerUser{}
	for _, entry := range r.Registries() {
		// Set for removed users.
		if entry.reg == nil {
			if entry.lastGather != nil {
				data = append(data, struct {
					user    string
					metrics MetricFamilyMap
				}{user: "", metrics: entry.lastGather})
			}

			continue
		}

		m, err := entry.reg.Gather()
		if err == nil {
			var mfm MetricFamilyMap // := would shadow err from outer block, and single err check will not work
			mfm, err = NewMetricFamilyMap(m)
			if err == nil {
				data = append(data, struct {
					user    string
					metrics MetricFamilyMap
				}{
					user:    entry.user,
					metrics: mfm,
				})
			}
		}

		if err != nil {
			level.Warn(r.logger).Log("msg", "failed to gather metrics from registry", "user", entry.user, "err", err)
			continue
		}
	}
	return data
}

// struct for holding metrics with same label values
type metricsWithLabels struct {
	labelValues []string
	metrics     []*dto.Metric
}

func getMetricsWithLabelNames(mf *dto.MetricFamily, labelNames []string) map[string]metricsWithLabels {
	result := map[string]metricsWithLabels{}

	for _, m := range mf.GetMetric() {
		lbls, include := getLabelValues(m, labelNames)
		if !include {
			continue
		}

		key := getLabelsString(lbls)
		r := result[key]
		if r.labelValues == nil {
			r.labelValues = lbls
		}
		r.metrics = append(r.metrics, m)
		result[key] = r
	}
	return result
}

func getLabelValues(m *dto.Metric, labelNames []string) ([]string, bool) {
	result := make([]string, 0, len(labelNames))

	for _, ln := range labelNames {
		found := false

		// Look for the label among the metric ones. We re-iterate on each metric label
		// which is algorithmically bad, but the main assumption is that the labelNames
		// in input are typically very few units.
		for _, lp := range m.GetLabel() {
			if ln != lp.GetName() {
				continue
			}

			result = append(result, lp.GetValue())
			found = true
			break
		}

		if !found {
			// required labels not found
			return nil, false
		}
	}

	return result, true
}

func getLabelsString(labelValues []string) string {
	// Get a buffer from the pool, reset it and release it at the
	// end of the function.
	buf := bytesBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bytesBufferPool.Put(buf)

	for _, v := range labelValues {
		buf.WriteString(v)
		buf.WriteByte(0) // separator, not used in prometheus labels
	}
	return buf.String()
}
