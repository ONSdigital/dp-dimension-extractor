BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=.

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

VAULT_ADDR?='http://127.0.0.1:8200'

BUILD_TIME=$(shell date +%s)
GIT_COMMIT=$(shell git rev-parse HEAD)
VERSION ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)
LDFLAGS=-ldflags "-w -s -X 'main.Version=${VERSION}' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'"

# The following variables are used to generate a vault token for the app. The reason for declaring variables, is that
# its difficult to move the token code in a Makefile action. Doing so makes the Makefile more difficult to
# read and starts introduction if/else statements.
VAULT_POLICY:="$(shell vault policy write -address=$(VAULT_ADDR) read-psk policy.hcl)"
TOKEN_INFO:="$(shell vault token create -address=$(VAULT_ADDR) -policy=read-psk -period=24h -display-name=dp-dimension-extractor)"
APP_TOKEN:="$(shell echo $(TOKEN_INFO) | awk '{print $$6}')"

.PHONY: all
all: audit test build

.PHONY: audit
audit:
	go list -m all | nancy sleuth

.PHONY: build
build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build $(LDFLAGS) -o $(BUILD_ARCH)/$(BIN_DIR)/dp-dimension-extractor main.go

.PHONY: debug
debug:
	VAULT_TOKEN=$(APP_TOKEN) VAULT_ADDR=$(VAULT_ADDR) HUMAN_LOG=1 go run $(LDFLAGS) main.go

.PHONY: lint
lint:
	golangci-lint run --timeout=5m ./...

.PHONY: acceptance
acceptance:
	VAULT_TOKEN=$(APP_TOKEN) VAULT_ADDR=$(VAULT_ADDR) ENCRYPTION_DISABLED=false HUMAN_LOG=1 go run $(LDFLAGS) main.go

.PHONY: test
test:
	go test -cover -race ./...

.PHONY: test-component
test-component:
	go test -cover -coverpkg=github.com/ONSdigital/dp-dimension-extractor/... -component

.PHONY: vault
vault:
	@echo "$(VAULT_POLICY)"
	@echo "$(TOKEN_INFO)"
	@echo "$(APP_TOKEN)"

.PHONEY: build debug test
