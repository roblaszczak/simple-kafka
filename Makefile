run-container:
	docker-compose run --rm app "bash"

test:
	docker-compose run --rm app "go test"

qa:
	gometalinter --line-length=120 --deadline 30s --enable=misspell --disable=gotype
