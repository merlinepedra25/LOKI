package dshttp

import (
	"encoding/json"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"gopkg.in/yaml.v2"
)

// WriteJSONResponse writes some JSON as an HTTP response.
func WriteJSONResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}

// WriteHTMLResponse sends message as text/html response with 200 status code.
func WriteHTMLResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html")

	// Ignore inactionable errors.
	_, _ = w.Write([]byte(message))
}

// StreamWriteYAMLResponse stream writes data as http response
func StreamWriteYAMLResponse(w http.ResponseWriter, iter chan interface{}, logger log.Logger) {
	w.Header().Set("Content-Type", "application/yaml")
	for v := range iter {
		data, err := yaml.Marshal(v)
		if err != nil {
			level.Error(logger).Log("msg", "yaml marshal failed", "err", err)
			continue
		}
		_, err = w.Write(data)
		if err != nil {
			level.Error(logger).Log("msg", "write http response failed", "err", err)
			return
		}
	}
}
