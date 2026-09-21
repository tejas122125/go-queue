.PHONY: all build test clean generate dashboard-dev dashboard-build

all: build

generate:
	/Users/tejaswee/go/bin/sqlc generate

build:
	CGO_ENABLED=0 go build -v ./...

test:
	CGO_ENABLED=0 go test -v ./...

clean:
	go clean

dashboard-dev:
	pnpm --filter @queue-forge/dashboard dev

dashboard-build:
	pnpm --filter @queue-forge/dashboard build
