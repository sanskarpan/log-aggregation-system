GO ?= go
SERVICES := gateway distributor parser ingester indexer query-frontend querier compactor control-plane

.PHONY: fmt test build clean

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

build:
	mkdir -p bin
	for service in $(SERVICES); do \
		$(GO) build -o bin/$$service ./cmd/$$service || exit 1; \
	done

clean:
	rm -rf bin

