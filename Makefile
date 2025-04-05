.PHONY: run

GO_FILES := $(shell find . -name '*.go' ! -name '*_test.go')

run:
	go run $(GO_FILES)
