package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"net/http"
	"runtime/debug"

	"github.com/go-kit/log/level"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/loki/pkg/util"

	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/loki/pkg/logqlmodel"
	"github.com/grafana/loki/pkg/storage/chunk"
)

// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
const StatusClientClosedRequest = 499

const (
	ErrClientCanceled   = "The request was cancelled by the client."
	ErrDeadlineExceeded = "Request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed."
)

type ErrorResponseBody struct {
	Code    int    `json:"code"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	JSONError(w, 404, "not found")
}

func JSONError(w http.ResponseWriter, code int, message string, args ...interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponseBody{
		Code:    code,
		Status:  "error",
		Message: fmt.Sprintf(message, args...),
	})
}

// WriteError write a go error with the correct status code.
func WriteError(err error, w http.ResponseWriter) {
	WriteErrorWithContext(context.Background(), err, w)
}

func WriteErrorWithContext(ctx context.Context, err error, w http.ResponseWriter) {
	var (
		queryErr chunk.QueryError
		promErr  promql.ErrStorage
	)

	level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren", "err", err)
	me, ok := err.(util.MultiError)
	if ok && me.IsCancel() {
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusClientClosedRequest multi")
		JSONError(w, StatusClientClosedRequest, ErrClientCanceled)
		return
	}
	if ok && me.IsDeadlineExceeded() {
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "ErrDeadlineExceeded multi")
		JSONError(w, http.StatusGatewayTimeout, ErrDeadlineExceeded)
		return
	}

	s, isRPC := status.FromError(err)
	if isRPC && s.Code() == codes.Canceled {
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren stacktrace", "stacktrace", string(debug.Stack()))
	}
	switch {
	case errors.Is(err, context.Canceled) ||
		(errors.As(err, &promErr) && errors.Is(promErr.Err, context.Canceled)):
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusClientClosedRequest")
		JSONError(w, StatusClientClosedRequest, ErrClientCanceled)
	case errors.Is(err, context.DeadlineExceeded) ||
		(isRPC && s.Code() == codes.DeadlineExceeded):
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusGatewayTimeout")
		JSONError(w, http.StatusGatewayTimeout, ErrDeadlineExceeded)
	case errors.As(err, &queryErr),
		errors.Is(err, logqlmodel.ErrLimit) || errors.Is(err, logqlmodel.ErrParse) || errors.Is(err, logqlmodel.ErrPipeline),
		errors.Is(err, user.ErrNoOrgID):
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusGatewayTimeout")
		JSONError(w, http.StatusBadRequest, err.Error())
	case isRPC && s.Code() == codes.Canceled:
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusInternalServerError")
		JSONError(w, http.StatusInternalServerError, err.Error())
	default:
		if grpcErr, ok := httpgrpc.HTTPResponseFromError(err); ok {
			level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "HTTPResponseFromError")
			JSONError(w, int(grpcErr.Code), string(grpcErr.Body))
			return
		}
		level.Error(util_log.WithContext(ctx, util_log.Logger)).Log("msg", "supra89kren + res", "errorresult", "StatusInternalServerError default")
		JSONError(w, http.StatusInternalServerError, err.Error())
	}
}
