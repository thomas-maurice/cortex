package rpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/obs"
)

// After a real call through the interceptor, /metrics must show the request
// counted under its procedure and result code — and must not echo anything from
// the request itself (content/namespace/tenant would be unbounded cardinality
// and a data leak on an unauthenticated endpoint).
func TestMetricsInterceptor(t *testing.T) {
	reg := obs.NewRegistry()
	mux := http.NewServeMux()
	path, h := cortexv1connect.NewMemoryServiceHandler(cortexv1connect.UnimplementedMemoryServiceHandler{},
		connect.WithInterceptors(MetricsInterceptor(reg)))
	mux.Handle(path, h)
	obs.Mount(mux, reg, nil)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := cortexv1connect.NewMemoryServiceClient(srv.Client(), srv.URL)
	_, err := client.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{
		Text:      "TOP-SECRET-MEMORY-TEXT",
		Namespace: "private-namespace",
	}))
	require.Error(t, err) // Unimplemented handler: the interceptor must still count it.

	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	body := string(raw)

	assert.Contains(t, body, `cortex_rpc_requests_total{code="unimplemented",procedure="/cortex.v1.MemoryService/Save"} 1`)
	assert.Contains(t, body, `cortex_rpc_duration_seconds_count{procedure="/cortex.v1.MemoryService/Save"} 1`)
	assert.NotContains(t, body, "TOP-SECRET-MEMORY-TEXT")
	assert.NotContains(t, body, "private-namespace")
}
