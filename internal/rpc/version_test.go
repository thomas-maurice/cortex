package rpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/identity"
)

// denyAllAuth rejects every request — the harshest possible authenticator, so
// anything that gets through can only be an explicit interceptor exemption.
type denyAllAuth struct{}

func (denyAllAuth) Authenticate(context.Context, http.Header) (identity.Identity, error) {
	return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("deny all"))
}

// TestGetVersionIsPublicEverythingElseIsNot pins the auth exemption's exact
// scope. GetVersion must work without credentials — that is what lets a
// stale/unauthenticated client discover it needs an upgrade. Equally
// load-bearing: the exemption must not widen — every other procedure has to
// keep hitting the authenticator, otherwise the version check becomes an
// accidental auth bypass.
func TestGetVersionIsPublicEverythingElseIsNot(t *testing.T) {
	svc := &Service{cfg: Config{Version: "9.9.9"}}
	mux := http.NewServeMux()
	mux.Handle(cortexv1connect.NewMemoryServiceHandler(svc,
		connect.WithInterceptors(ServerAuthInterceptor(denyAllAuth{}))))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := cortexv1connect.NewMemoryServiceClient(http.DefaultClient, srv.URL)
	ctx := context.Background()

	res, err := client.GetVersion(ctx, connect.NewRequest(&cortexv1.GetVersionRequest{}))
	require.NoError(t, err, "GetVersion must be reachable without credentials")
	assert.Equal(t, "9.9.9", res.Msg.GetVersion())

	_, err = client.Status(ctx, connect.NewRequest(&cortexv1.StatusRequest{}))
	require.Error(t, err, "Status must still require auth")
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())
}
