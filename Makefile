SOURCE = basic concurrency

.PHONY: build
build:
	@for dir in $(SOURCE); do \
		go build -o cmd/$$dir/main cmd/$$dir/main.go; \
	done

.PHONY: run
run:
ifndef TARGET
	@echo "Error: Please specify TARGET (e.g., make run TARGET=foo)"
	exit 1
endif
	time ./cmd/$(TARGET)/main