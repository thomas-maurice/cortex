.DEFAULT_GOAL := help
BIN := bin
IMAGE ?= ghcr.io/thomas-maurice/cortex:latest
VERSION ?= dev
OLLAMA_MODEL ?= qwen3-embedding:0.6b

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: proto
proto: ## Regenerate Go code from the protobuf definitions (needs buf)
	buf generate
	@echo "generated gen/ from proto/"

.PHONY: proto-ui
proto-ui: ## Regenerate the TS Connect clients for the web UI (needs ui deps)
	buf generate --template buf.gen.ui.yaml
	@echo "generated ui/src/gen from proto/"

.PHONY: ui
ui: ## Build the embedded web UI into ui/dist (npm install + vite build)
	cd ui && npm install && npm run build
	@echo "built ui/dist"

.PHONY: build
build: ui ## Build the server (with embedded UI), mcp, worker, and cli into ./bin
	@mkdir -p $(BIN)
	go build -o $(BIN)/cortex-server ./cmd/server
	go build -o $(BIN)/cortex-mcp ./cmd/mcp
	go build -o $(BIN)/cortex-worker ./cmd/worker
	go build -o $(BIN)/cortex ./cmd/cli
	@echo "built $(BIN)/cortex-server, $(BIN)/cortex-mcp, $(BIN)/cortex-worker and $(BIN)/cortex"

.PHONY: image
image: ## Build the cortex docker image (all binaries) tagged $(IMAGE)
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .
	@echo "built image $(IMAGE) (version $(VERSION))"

.PHONY: up
up: image ## Build the image, then start the stack (nats, weaviate, ollama, worker, server)
	docker compose up -d

.PHONY: down
down: ## Stop the stack (keeps volumes)
	docker compose down

.PHONY: nuke
nuke: ## Stop the stack and delete all data volumes
	docker compose down -v

.PHONY: model
model: ## Pull the embedding model into the running ollama container
	docker compose exec ollama ollama pull $(OLLAMA_MODEL)

.PHONY: bootstrap
bootstrap: up ## Bring up the stack and pull the embedding model
	@echo "waiting for ollama..."; sleep 5
	$(MAKE) model
	@echo "bootstrap complete"

.PHONY: logs
logs: ## Tail worker + server logs
	docker compose logs -f worker server

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: test
test: ## Run unit tests
	go test ./...
