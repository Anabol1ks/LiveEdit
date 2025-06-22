PROTO_DIR = proto/
PROTO_FILE = $(PROTO_DIR)document/document.proto
# PROTO_FILE = $(PROTO_DIR)user/user.proto
GEN_DIR = gen

# Путь до validate.proto (можно через buf или вручную)
PROTO_INCLUDE = third_party

.PHONY: all generate clean

all: generate

generate:
	protoc $(PROTO_FILE) \
		--proto_path=. \
		--proto_path=$(PROTO_INCLUDE) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--validate_out=lang=go:$(GEN_DIR)

clean:
	rm -rf $(GEN_DIR)
