package rpc

import (
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
)

// NewClient builds a MemoryService client pointed at baseURL (e.g.
// "http://localhost:8080"), attaching the bearer token to every call. token may
// be empty when the server runs without auth.
func NewClient(baseURL, token string) cortexv1connect.MemoryServiceClient {
	httpClient := &http.Client{Timeout: 60 * time.Second}
	return cortexv1connect.NewMemoryServiceClient(
		httpClient,
		baseURL,
		connect.WithInterceptors(clientAuthInterceptor(token)),
	)
}
