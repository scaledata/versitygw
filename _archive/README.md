# `_archive/` — preserved, not built

Work that should stay reachable but is not part of the gateway build. The
leading underscore makes the Go tool skip this directory, so nothing here is
compiled, vetted or tested.

## `otter-control-thrift/`

The Thrift implementation of the OtterControl service — the original JFL grant
push endpoint, and the transport proven end to end against a real Scala
SafeThrift client over cluster mutual TLS (PoC0).

**It is not being ported.** CDM pins `apache/thrift v0.13.0`; this code is
generated against 0.23.0 and uses seven symbols absent from the vendored tree:
`WrapTException`, `ProcessorError`, `SlogTStructWrapper`, `ErrAbandonRequest`,
`ResponseMeta`, `ServerConnectivityCheckInterval`, and
`NewTBinaryProtocolFactoryConf`. Six of those appear in generated code, so
regenerating against 0.13.0 is not a small edit. Thrift cannot be vendored, and
the gRPC server in `otter/controlgrpc` replaces it.

Kept because the handler semantics — epoch guarding, typed rejects, the resolver
bridge — carried over to gRPC unchanged, and because this is the artifact behind
the mTLS interop result.

## `plans/grant-driven-write-path.md`

The design for grant-driven write-path reconfiguration, written against the
otter-v2 working tree. Describes the problem it solves: the control plane
received grants and created bucket directories, while S3 writes still went to
`gwroot` regardless of the `ChannelPath` the grant named.

Implemented in `otter: drive the write path from pushed grants`. Retained for
the rationale, in particular why `os.Chdir` had to go before two `posix.Backend`
instances could coexist across a reconfigure.
