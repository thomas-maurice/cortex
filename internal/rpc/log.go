package rpc

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
)

// ServerLogInterceptor logs every inbound RPC: the procedure, how long it took,
// and the outcome (ok, or the connect error code + message). It is registered
// outermost so it also records calls rejected by the auth interceptor.
func ServerLogInterceptor(log *slog.Logger) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			start := time.Now()
			resp, err := next(ctx, req)
			dur := time.Since(start)
			if err != nil {
				log.Warn("rpc",
					"procedure", req.Spec().Procedure,
					"peer", req.Peer().Addr,
					"dur", dur.String(),
					"code", connect.CodeOf(err).String(),
					"err", err)
			} else {
				log.Info("rpc",
					"procedure", req.Spec().Procedure,
					"peer", req.Peer().Addr,
					"dur", dur.String())
			}
			return resp, err
		}
	})
}
