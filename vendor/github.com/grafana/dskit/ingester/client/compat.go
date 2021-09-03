package client

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

// Fingerprint runs the same algorithm as Prometheus labelSetToFingerprint()
func Fingerprint(labels labels.Labels) model.Fingerprint {
	sum := hashNew()
	for _, label := range labels {
		sum = hashAddString(sum, label.Name)
		sum = hashAddByte(sum, model.SeparatorByte)
		sum = hashAddString(sum, label.Value)
		sum = hashAddByte(sum, model.SeparatorByte)
	}
	return model.Fingerprint(sum)
}
