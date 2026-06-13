# Single image containing all Cortex binaries. Compose selects which one to run
# per service via `entrypoint`. Build it with `make image`.

# Stage 1: build the embedded web UI. cortex-server go:embeds ui/dist.
FROM node:22-alpine AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
# VERSION is stamped into every binary's `main.version` so the image reports the
# same version it is tagged with (passed by CI / `make image`; "dev" otherwise).
ARG VERSION=dev
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Overlay the freshly built UI so the embed picks up real assets, not the
# committed .gitkeep placeholder.
COPY --from=ui /ui/dist ./ui/dist
RUN LDFLAGS="-s -w -X main.version=${VERSION}" \
 && CGO_ENABLED=0 go build -ldflags="$LDFLAGS" -o /out/cortex-server ./cmd/server \
 && CGO_ENABLED=0 go build -ldflags="$LDFLAGS" -o /out/cortex-worker ./cmd/worker \
 && CGO_ENABLED=0 go build -ldflags="$LDFLAGS" -o /out/cortex-mcp ./cmd/mcp \
 && CGO_ENABLED=0 go build -ldflags="$LDFLAGS" -o /out/cortex ./cmd/cli

# nonroot variant runs as UID 65532 — required for non-root app platforms like
# TrueNAS Scale. The binaries are stateless (all state lives in nats/weaviate/
# ollama), so they need no writable paths and no host-mount ownership.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
EXPOSE 8080
# Default to the server; compose/k8s overrides the entrypoint for the worker.
ENTRYPOINT ["/usr/local/bin/cortex-server"]
