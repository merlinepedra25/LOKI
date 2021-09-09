package dslog

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

var experimentalFeaturesInUse = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "experimental_features_in_use_total",
		Help:      "The number of experimental features in use.",
	},
)

// WarnExperimentalUse logs a warning and increments the experimental features metric.
func WarnExperimentalUse(feature string, logger log.Logger, reg prometheus.Registerer) {
	if reg != nil {
		reg.MustRegister(experimentalFeaturesInUse)
	}
	level.Warn(logger).Log("msg", "experimental feature in use", "feature", feature)
	experimentalFeaturesInUse.Inc()
}
