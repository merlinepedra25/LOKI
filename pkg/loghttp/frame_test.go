package loghttp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrameSamples(t *testing.T) {
	f := newLogLineField()

	require.Equal(t, len(f.Fields), 3)
}
