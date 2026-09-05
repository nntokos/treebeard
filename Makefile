# daos-xr-grpc additions on top of the upstream Treebeard repo.
# Builds the gRPC adapter (treebeard_grpc) that exposes the daos_xr.proto
# BackendIngress/Capability/BatchUnionIngress services over the running
# Treebeard cluster (router + shardnode + oramnode + Redis).

PROTO_DIR  := api
DAOS_PROTO := $(PROTO_DIR)/daos_xr.proto
DAOS_OUT   := $(PROTO_DIR)/daos_xr

.PHONY: all proto treebeard_grpc grpc_server clean_grpc

all: treebeard_grpc

# Generate daos_xr gRPC stubs (requires protoc + protoc-gen-go + protoc-gen-go-grpc).
proto: $(DAOS_OUT)/daos_xr.pb.go

$(DAOS_OUT)/daos_xr.pb.go: $(DAOS_PROTO)
	mkdir -p $(DAOS_OUT)
	protoc --proto_path=$(PROTO_DIR) \
	       --go_out=$(DAOS_OUT) --go_opt=paths=source_relative \
	       --go-grpc_out=$(DAOS_OUT) --go-grpc_opt=paths=source_relative \
	       daos_xr.proto

# Build the gRPC adapter binary.
treebeard_grpc: proto
	go build -o treebeard_grpc ./cmd/grpc_server/

# Build ALL upstream Treebeard binaries (for deploying the full cluster).
cluster_binaries:
	go build -o router     ./cmd/router/
	go build -o shardnode  ./cmd/shardnode/
	go build -o oramnode   ./cmd/oramnode/

clean_grpc:
	rm -f treebeard_grpc $(DAOS_OUT)/daos_xr.pb.go $(DAOS_OUT)/daos_xr_grpc.pb.go

# Regenerate upstream protos (unchanged from scripts/generate_protos.sh).
proto_upstream:
	cd scripts && bash generate_protos.sh
