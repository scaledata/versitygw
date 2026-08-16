#!/usr/bin/env bash
# Regenerate the OtterControlService Go gRPC stubs from otter_control.proto.
# Requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH
# (present in polaris/.buildenv/bin). Run from the versity module root.
set -euo pipefail
cd "$(dirname "$0")/../.."   # -> otter-v2/versity (module root: github.com/versity/versitygw)
protoc --proto_path=otter/controlgrpc \
  --go_out=. --go_opt=module=github.com/versity/versitygw \
  --go-grpc_out=. --go-grpc_opt=module=github.com/versity/versitygw \
  otter/controlgrpc/otter_control.proto
echo "generated otter/controlgrpc/ottercontrolpb/"
