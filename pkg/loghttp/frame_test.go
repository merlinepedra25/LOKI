package loghttp

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/stretchr/testify/require"
)

func TestFrameSamples(t *testing.T) {
	f := newLogsFrame(0)
	f.append(Stream{
		Labels: LabelSet{
			"a": "AAA",
			"b": "BBB",
		},
		Entries: []Entry{
			{Timestamp: time.Now(), Line: "111"},
			{Timestamp: time.Now(), Line: "222"},
		},
	})
	f.append(Stream{
		Labels: LabelSet{
			"c": "CCC",
		},
		Entries: []Entry{
			{Timestamp: time.Now(), Line: "333"},
		},
	})

	v, err := data.FrameToJSON(f.frame, data.IncludeAll)
	require.NoError(t, err)
	fmt.Printf("%s\n", v)

	f.clear()
	f.append(Stream{
		Labels: LabelSet{
			"x": "XXX",
		},
		Entries: []Entry{
			{Timestamp: time.Now(), Line: "444"},
		},
	})

	v, err = data.FrameToJSON(f.frame, data.IncludeDataOnly)
	require.NoError(t, err)
	fmt.Printf("%s\n", v)

	require.Equal(t, len(f.frame.Fields), 4)
}
