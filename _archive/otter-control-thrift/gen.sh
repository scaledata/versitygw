#!/usr/bin/env bash
# Regenerate the OtterControl Go stubs from otter_control.thrift.
# Requires thrift 0.23.0 (must match the github.com/apache/thrift runtime in go.mod).
set -euo pipefail
cd "$(dirname "$0")"
thrift -r --gen go -out . otter_control.thrift
# The generated *-remote CLI is `package main` and breaks `go build ./...`; drop it.
rm -rf ottercontrol/otter_control-remote
echo "generated ottercontrol/ (removed -remote CLI)"
