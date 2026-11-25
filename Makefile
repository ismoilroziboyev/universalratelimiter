.PHONY: test race vet benchmark integration

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

benchmark:
	go test -run '^$$' -bench=. -benchmem ./...

integration:
	@REDIS_SERVER="$${REDIS_SERVER:-$$(command -v redis-server)}"; \
	test -n "$$REDIS_SERVER" || { echo 'Install redis-server or set REDIS_SERVER to its path'; exit 1; }; \
	REDIS_SERVER="$$REDIS_SERVER" go test -count=1 -v ./tests/integration
