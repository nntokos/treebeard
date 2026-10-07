# obistream-adapter additions on top of the upstream Treebeard repo.
# Builds the gRPC adapter (treebeard_grpc) that exposes the obistream.proto
# BackendIngress/Capability/BatchUnionIngress services over the running
# Treebeard cluster (router + shardnode + oramnode + Redis).

PROTO_DIR  := api
OBISTREAM_PROTO := $(PROTO_DIR)/obistream.proto
OBISTREAM_OUT   := $(PROTO_DIR)/obistream

.PHONY: all proto treebeard_grpc grpc_server clean_grpc

all: treebeard_grpc

# Generate obistream gRPC stubs (requires protoc + protoc-gen-go + protoc-gen-go-grpc).
proto: $(OBISTREAM_OUT)/obistream.pb.go

$(OBISTREAM_OUT)/obistream.pb.go: $(OBISTREAM_PROTO)
	mkdir -p $(OBISTREAM_OUT)
	protoc --proto_path=$(PROTO_DIR) \
	       --go_out=$(OBISTREAM_OUT) --go_opt=paths=source_relative \
	       --go-grpc_out=$(OBISTREAM_OUT) --go-grpc_opt=paths=source_relative \
	       obistream.proto

# Build the gRPC adapter binary.
treebeard_grpc: proto
	go build -o treebeard_grpc ./cmd/grpc_server/

# Build ALL upstream Treebeard binaries (for deploying the full cluster).
cluster_binaries:
	go build -o router     ./cmd/router/
	go build -o shardnode  ./cmd/shardnode/
	go build -o oramnode   ./cmd/oramnode/

clean_grpc:
	rm -f treebeard_grpc $(OBISTREAM_OUT)/obistream.pb.go $(OBISTREAM_OUT)/obistream_grpc.pb.go

# Regenerate upstream protos (unchanged from scripts/generate_protos.sh).
proto_upstream:
	cd scripts && bash generate_protos.sh
