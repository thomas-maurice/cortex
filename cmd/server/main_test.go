package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"connectrpc.com/connect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/obs"
	"github.com/thomas-maurice/cortex/internal/rpc"
)

// TestInitialBackupDelay pins the restart-resilient scheduling rule: the first
// periodic backup after boot must be scheduled from the newest on-disk backup's
// age, NOT from process start. Without this, a server that restarts more often
// than the interval would keep resetting a ticker that never fires.
func TestInitialBackupDelay(t *testing.T) {
	const interval = 24 * time.Hour
	const startup = time.Minute
	now := time.Unix(1_000_000, 0)

	t.Run("no prior backup -> back up soon", func(t *testing.T) {
		assert.Equal(t, startup, initialBackupDelay(time.Time{}, false, interval, startup, now))
	})

	t.Run("overdue backup -> back up soon", func(t *testing.T) {
		last := now.Add(-25 * time.Hour) // older than interval
		assert.Equal(t, startup, initialBackupDelay(last, true, interval, startup, now))
	})

	t.Run("exactly at interval -> overdue, back up soon", func(t *testing.T) {
		last := now.Add(-interval)
		assert.Equal(t, startup, initialBackupDelay(last, true, interval, startup, now))
	})

	t.Run("recent backup -> wait only the remaining interval", func(t *testing.T) {
		last := now.Add(-6 * time.Hour) // 18h remaining
		assert.Equal(t, 18*time.Hour, initialBackupDelay(last, true, interval, startup, now))
	})
}

// TestS3ConfigFromEnv pins the env-var -> S3-config mapping. Credentials are held
// in memory only (never stored), so the mapping is the whole contract. Rules that
// matter and could silently regress: uploads only enable with BOTH credentials
// AND a bucket, CORTEX_S3_ENABLED can force them off, and region falls back
// AWS_DEFAULT_REGION -> AWS_REGION -> "us-east-1".
func TestS3ConfigFromEnv(t *testing.T) {
	// Clear every var the function reads so each subtest starts from a known
	// blank environment regardless of the host's real AWS_* vars.
	clearEnv := func(t *testing.T) {
		for _, k := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
			"AWS_REGION", "CORTEX_S3_ENDPOINT", "CORTEX_S3_BUCKET",
			"CORTEX_S3_PREFIX", "CORTEX_S3_USE_SSL", "CORTEX_S3_ENABLED",
		} {
			t.Setenv(k, "")
		}
	}

	t.Run("no credentials is disabled", func(t *testing.T) {
		clearEnv(t)
		assert.False(t, s3ConfigFromEnv().Enabled, "must not enable without credentials")
	})

	t.Run("one credential alone is not enough", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		assert.False(t, s3ConfigFromEnv().Enabled, "both access and secret key are required")
	})

	t.Run("credentials plus bucket enable an upload config with defaults", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		t.Setenv("CORTEX_S3_BUCKET", "cortex-backups")
		cfg := s3ConfigFromEnv()
		assert.True(t, cfg.Enabled, "creds + bucket present + default enabled")
		assert.Equal(t, "s3.amazonaws.com", cfg.Endpoint)
		assert.Equal(t, "us-east-1", cfg.Region)
		assert.Equal(t, "cortex-backups", cfg.Bucket)
		assert.Equal(t, "AKIA", cfg.AccessKey)
		assert.Equal(t, "secret", cfg.SecretKey)
		assert.True(t, cfg.UseSsl)
	})

	t.Run("no bucket forces uploads off even with credentials", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		assert.False(t, s3ConfigFromEnv().Enabled, "nowhere to upload -> not enabled")
	})

	t.Run("region falls back to AWS_REGION and overrides apply", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		t.Setenv("AWS_REGION", "eu-west-3")
		t.Setenv("CORTEX_S3_ENDPOINT", "minio.lan:9000")
		t.Setenv("CORTEX_S3_BUCKET", "b")
		t.Setenv("CORTEX_S3_USE_SSL", "false")
		t.Setenv("CORTEX_S3_ENABLED", "false")
		cfg := s3ConfigFromEnv()
		assert.Equal(t, "eu-west-3", cfg.Region)
		assert.Equal(t, "minio.lan:9000", cfg.Endpoint)
		assert.False(t, cfg.UseSsl)
		assert.False(t, cfg.Enabled, "explicitly disabled despite a bucket")
	})
}

// The compose healthcheck runs this subcommand because the distroless image has
// no wget/curl; it must turn /readyz's status into the exit result.
func TestHealthcheck(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		wantErr bool
	}{{"ready", http.StatusOK, false}, {"not ready", http.StatusServiceUnavailable, true}} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/readyz", r.URL.Path)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			err := healthcheck(strings.TrimPrefix(srv.URL, "http://"))
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
	assert.Error(t, healthcheck("not-an-addr"))
}

// /metrics is unauthenticated, so it must never share the (publicly routed)
// API port: on the API mux it falls through to the UI catch-all even though
// /healthz and /readyz stay there.
func TestAPIMuxNeverServesMetrics(t *testing.T) {
	mux := http.NewServeMux()
	obs.Mount(mux, nil, nil)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	for path, want := range map[string]int{"/metrics": http.StatusTeapot, "/healthz": http.StatusOK, "/readyz": http.StatusOK} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, want, rec.Code, path)
	}
}

// The dedicated listener serves /metrics plus the probes, on a port that is not
// the API's, and stops with the context.
func TestMetricsListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := http.NewServeMux()
	obs.Mount(mux, obs.NewRegistry(), nil)
	addr, err := obs.Serve(ctx, slog.Default(), "metrics", "127.0.0.1:0", mux)
	require.NoError(t, err)

	for _, path := range []string{"/metrics", "/healthz", "/readyz"} {
		resp, err := http.Get("http://" + addr.String() + path)
		require.NoError(t, err, path)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, path)
	}

	cancel()
	require.Eventually(t, func() bool {
		_, err := http.Get("http://" + addr.String() + "/healthz")
		return err != nil
	}, 5*time.Second, 20*time.Millisecond, "listener must shut down with ctx")
}

// Disabling metrics is an explicit empty value; unset keeps the default.
func TestMetricsAddrFromEnv(t *testing.T) {
	// t.Setenv registers restore; Unsetenv then makes it genuinely unset.
	t.Setenv("API_METRICS_ADDR", "")
	require.NoError(t, os.Unsetenv("API_METRICS_ADDR"))
	assert.Equal(t, ":9090", metricsAddrFromEnv())
	t.Setenv("API_METRICS_ADDR", "")
	assert.Equal(t, "", metricsAddrFromEnv(), "empty disables the listener")
	t.Setenv("API_METRICS_ADDR", ":9191")
	assert.Equal(t, ":9191", metricsAddrFromEnv())
}

// TestRequestSizeLimit pins the server-wide cap: without it an authenticated
// tenant could send an arbitrarily large restore payload and exhaust server
// memory for everyone. Uses the production option builder with a tiny limit.
func TestRequestSizeLimit(t *testing.T) {
	svc := rpc.NewService(nil, nil, nil, nil, rpc.Config{}, slog.Default())
	mux := http.NewServeMux()
	mux.Handle(cortexv1connect.NewMemoryServiceHandler(svc, rpcHandlerOptions(1024, nil)...))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := cortexv1connect.NewMemoryServiceClient(http.DefaultClient, srv.URL)

	call := func(n int) error {
		_, err := client.RestoreSelf(context.Background(),
			connect.NewRequest(&cortexv1.RestoreSelfRequest{Data: []byte(strings.Repeat("x", n))}))
		return err
	}

	// Under the limit the request reaches the handler (which rejects the
	// garbage payload as a bad backup format, not as too large).
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(call(100)))
	// Over the limit it is refused before the handler runs.
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(call(4096)))
}

func TestMaxRequestBytesFromEnv(t *testing.T) {
	t.Setenv("CORTEX_MAX_REQUEST_BYTES", "")
	assert.Equal(t, 256<<20, maxRequestBytesFromEnv())
	t.Setenv("CORTEX_MAX_REQUEST_BYTES", "1048576")
	assert.Equal(t, 1048576, maxRequestBytesFromEnv())
	for _, bad := range []string{"0", "-5", "abc"} {
		t.Setenv("CORTEX_MAX_REQUEST_BYTES", bad)
		assert.Equal(t, 256<<20, maxRequestBytesFromEnv(), bad)
	}
}
