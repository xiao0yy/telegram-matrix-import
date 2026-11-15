GO ?= go
VERSION ?= dev
COMMIT ?= unknown
TAGS := goolm,netgo,osusergo,sqlite_omit_load_extension
IDENTITY := -X main.version=$(VERSION) -X main.commit=$(COMMIT)
# The generated Telegram schema is large. Disabling its compiler optimizations
# reduces build memory; transfer buffers and other packages remain optimized.
TGFLAGS := -gcflags=github.com/gotd/td/tg=-N\ -l

.PHONY: build test integration dist
build:
	mkdir -p bin
	CGO_ENABLED=1 $(GO) build -p 1 $(TGFLAGS) -tags $(TAGS) -ldflags '$(IDENTITY)' -o bin/tg-import ./cmd/tg-import

test:
	$(GO) test -p 1 $(TGFLAGS) -tags $(TAGS) ./...
	$(GO) vet -p 1 $(TGFLAGS) -tags $(TAGS) ./...

integration:
	python3 tests/tuwunel_fixture.py $(GO) test -p 1 $(TGFLAGS) -tags $(TAGS) ./internal/matrix -count=1

# musl avoids depending on the build runner's glibc version. Production uses
# Debian 12; the CI runner's newer glibc must not become a runtime requirement.
dist:
	mkdir -p bin dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 CC=musl-gcc $(GO) build -p 1 $(TGFLAGS) -tags $(TAGS) -ldflags '$(IDENTITY) -linkmode external -extldflags=-static' -o bin/tg-import ./cmd/tg-import
	GO='$(GO)' python3 scripts/package.py
