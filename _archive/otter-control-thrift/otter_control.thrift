// OtterControl — the JFL -> Otter control-plane RPC (the "grant push").
//
// This is the standalone (laptop-buildable) contract: it extends the PoC0 wire
// (poc0-jfl-thrift/, proven over cluster mTLS with a real Scala SafeThrift client)
// with current_epoch on the upsert response and a revoke RPC. The production-shape
// additions — a common.RequestContext envelope and a TMultiplexedProtocol — are the
// deliberate on-cluster follow-up (they need CDM includes + the build box and are
// exercised only by the real Scala client), so they are intentionally absent here.
//
// Regenerate the Go stubs with ./gen.sh (thrift 0.23.0, matching the runtime dep).
namespace go ottercontrol

struct PingResponse {
  1: string message
  2: i64 server_unix_ms
}

enum PlacementPolicy { KEY_HASH = 0, ORDINAL_ROUND_ROBIN = 1 }

struct ChannelSlot {
  1: i32 slot
  2: string node_id
  3: string endpoint
  4: string channel_path
}

// The grant: "which client (by access key), which bucket, which channels."
struct OtterChannelGrant {
  1: string client_id            // logical client (IAM principal) — provenance only
  2: string access_key_id        // SigV4 key the client signs with -> binds requests to this grant
  3: string bucket               // the bucket this grant authorizes
  4: i64 epoch                   // monotonic version, frozen per (access_key_id,bucket); higher wins
  5: i32 n                       // channel count (== slots.size; validated on receipt)
  6: list<ChannelSlot> slots     // ordered slot -> owner: the channel-set
  7: PlacementPolicy policy       // KEY_HASH | ORDINAL_ROUND_ROBIN
  8: i64 lease_expiry_unix_s     // optional TTL (revoke-by-expiry); 0 = none
  9: bool active                 // false = soft-disable
}

struct UpsertGrantResponse {
  1: bool accepted               // false if an equal-or-newer epoch already won
  2: i64 current_epoch           // the epoch now in effect (== grant.epoch on accept; the winner on reject)
  3: string detail
}

// Revoke is keyed by Otter's join key (access_key_id), not client_id: that is what
// the cache and the routing path key on. client_id is provenance. The durable CRDB
// delete is authoritative; this just drops the hot-cache entry.
struct RevokeRequest {
  1: string access_key_id
  2: string bucket
  3: i64 epoch                   // guard: only revoke if >= the cached epoch
  4: string client_id            // provenance (optional)
}

struct RevokeGrantResponse {
  1: bool revoked
  2: string detail
}

service OtterControl {
  // liveness / the mTLS-handshake probe used by PoC0
  PingResponse ping(1: string caller)

  // JFL pushes a grant; idempotent by (access_key_id, bucket); epoch guards ordering
  UpsertGrantResponse upsertChannelGrant(1: OtterChannelGrant grant)

  // offboard / rotate / retire a grant from the hot cache
  RevokeGrantResponse revokeChannelGrant(1: RevokeRequest req)
}
