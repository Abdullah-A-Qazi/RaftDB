# protoc plugins (protoc-gen-go, protoc-gen-go-grpc) are installed via
# `make tools` into $(go env GOPATH)/bin; PATH is extended so protoc finds
# them regardless of the user's shell setup.
GOBIN := $(shell go env GOPATH)/bin

.PHONY: all build test vet proto tools clean

all: build test

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

# Regenerate Go stubs from the .proto definitions.
# --go_opt=module strips the module prefix so generated files land in
# rpc/raftpb/ and rpc/kvpb/ (matching the go_package options).
proto:
	PATH="$(PATH):$(GOBIN)" protoc \
		--go_out=. --go_opt=module=github.com/Abdullah-A-Qazi/RaftDB \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Abdullah-A-Qazi/RaftDB \
		rpc/raft.proto rpc/kv.proto

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

clean:
	rm -f raftd
	go clean ./...
