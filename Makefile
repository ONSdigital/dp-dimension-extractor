DIRECTORIES?=dimension service config

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=.

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build -o $(BUILD_ARCH)/$(BIN_DIR)/dp-dimension-extractor main.go

debug:
	HUMAN_LOG=1 go run main.go

test: ; $(foreach dir,$(DIRECTORIES),(echo Running tests for $(dir) package:) && (go test -cover $(dir)/*.go) &&) :

.PHONEY: build debug test
