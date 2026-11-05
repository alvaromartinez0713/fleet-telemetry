BINS := ingest consumer alerts agent gateway

.PHONY: build run-up run-down run-status test

build:
	go build ./...

run-up:
	docker compose -f deploy/docker-compose.yml up -d

run-down:
	docker compose -f deploy/docker-compose.yml down

run-status:
	docker compose -f deploy/docker-compose.yml ps

test:
	go test ./...
