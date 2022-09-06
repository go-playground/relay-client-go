GOCMD=GO111MODULE=on go

deps:
	go mod download
	go mod tidy

linters-install:
	@golangci-lint --version >/dev/null 2>&1 || { \
		echo "installing linting tools..."; \
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s v1.41.1; \
	}

lint: linters-install deps
	golangci-lint run

test: lint
	RELAY_URL="http://localhost:8080" $(GOCMD) test -cover -race ./...

test.ci: deps
	$(GOCMD) test -cover -race ./...

services.up:
	@docker-compose up -d
	@dockerize -wait tcp://:5432 -wait tcp://:8080

services.down:
	@docker-compose stop --timeout 0

test.all: services.up test services.down

bench:
	$(GOCMD) test -bench=. -benchmem ./...

.PHONY: deps test test.ci test.all lint linters-install services.up services.down