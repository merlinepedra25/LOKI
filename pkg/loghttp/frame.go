package loghttp

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func newLogLineField() *data.Frame {
	labelsField := data.NewFieldFromFieldType(data.FieldTypeString, 1) // labels
	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, 1)     // time
	lineField := data.NewFieldFromFieldType(data.FieldTypeString, 1)   // line

	f := data.NewFrame("", labelsField, timeField, lineField)
	f.SetMeta(&data.FrameMeta{
		// TODO -- types
	})
	return f
}
