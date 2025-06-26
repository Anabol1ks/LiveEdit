PROTO_DIR = proto/
PROTO_FILES = $(wildcard $(PROTO_DIR)*/*.proto)
GEN_DIR = gen

# Путь до validate.proto (можно через buf или вручную)
PROTO_INCLUDE = third_party

.PHONY: all generate clean

all: generate

generate:
	protoc $(PROTO_FILES) \
		--proto_path=. \
		--proto_path=$(PROTO_INCLUDE) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--validate_out=lang=go:$(GEN_DIR) \
		--grpc-gateway_out=$(GEN_DIR) --grpc-gateway_opt=paths=source_relative \
		--openapiv2_out=$(GEN_DIR) --openapiv2_opt=logtostderr=true

clean:
	rm -rf $(GEN_DIR)


