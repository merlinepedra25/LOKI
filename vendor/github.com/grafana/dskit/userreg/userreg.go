package userreg

import (
	"sync"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
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
	regsMu sync.Mutex
	regs   []UserRegistry
}

// NewUserRegistries makes new UserRegistries.
func NewUserRegistries() *UserRegistries {
	return &UserRegistries{}
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
		level.Warn(util_log.Logger).Log("msg", "failed to gather metrics from registry", "user", ur.user, "err", err)
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
		level.Warn(util_log.Logger).Log("msg", "failed to gather metrics from registry", "user", ur.user, "err", err)
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
			level.Warn(util_log.Logger).Log("msg", "failed to gather metrics from registry", "user", entry.user, "err", err)
			continue
		}
	}
	return data
}
