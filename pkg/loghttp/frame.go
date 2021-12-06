package loghttp

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

type logsFrame struct {
	labels *data.Field
	time   *data.Field
	line   *data.Field
	frame  *data.Frame
}

// :grimmice: this really should exist in the SDK directly
func (f *logsFrame) clear() {
	for {
		if f.labels.Len() < 1 {
			return
		}
		f.frame.DeleteRow(0)
	}
}

func (f *logsFrame) append(stream Stream) {
	str := stream.Labels.String()
	for _, v := range stream.Entries {
		f.frame.AppendRow(str, v.Timestamp, v.Line)
	}
}

func newLogsFrame(size int) logsFrame {
	wrap := logsFrame{
		labels: data.NewFieldFromFieldType(data.FieldTypeString, size),
		time:   data.NewFieldFromFieldType(data.FieldTypeTime, size),
		line:   data.NewFieldFromFieldType(data.FieldTypeString, size),
	}

	wrap.labels.Name = "Labels"
	wrap.time.Name = "Time"
	wrap.line.Name = "Line"

	wrap.frame = data.NewFrame("", wrap.labels, wrap.time, wrap.line)
	wrap.frame.SetMeta(&data.FrameMeta{
		// TODO -- types
	})
	return wrap
}
