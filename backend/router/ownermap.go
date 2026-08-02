// Package router implements Otter multi-node write distribution: it routes each
// object verb to the slot/owner computed from the object key, writing locally
// when this node owns the object and forwarding to the owner's gateway
// otherwise. See the Router type doc for the invariants.
package router

import (
	"encoding/json"
	"fmt"
	"os"
)

// Slot is one node's place in the ordered owner map: slot index i owns the
// objects that hash to i. NodeId/ChannelPath identify the AF2 channel; Endpoint
// is that slot's gateway, used to forward writes/reads for objects it owns.
// This matches the JSON emitted by deploy_multichannel.sh / af2_expose_n.scala.
type Slot struct {
	Slot        int    `json:"slot"`
	NodeId      string `json:"nodeId"`
	Endpoint    string `json:"endpoint"`
	ChannelPath string `json:"channelPath"`
}

// OwnerMap is the authoritative, ordered slot table — byte-identical on every
// gateway. selfIdx is supplied per-node as a flag (not in the JSON), and the
// forward-leg credentials come from the environment, so the same file ships to
// every node. N and the ordering are frozen for the data's retention life
// (a changed modulus repoints every object); Epoch lets a reader detect drift.
type OwnerMap struct {
	Epoch int64  `json:"epoch"`
	N     int    `json:"n"`
	Slots []Slot `json:"slots"`
}

// LoadOwnerMap reads and validates the owner map JSON.
func LoadOwnerMap(path string) (*OwnerMap, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read owner map %q: %w", path, err)
	}
	var m OwnerMap
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse owner map %q: %w", path, err)
	}
	if m.N <= 0 {
		return nil, fmt.Errorf("owner map: n must be > 0, got %d", m.N)
	}
	if len(m.Slots) != m.N {
		return nil, fmt.Errorf("owner map: have %d slots but n=%d", len(m.Slots), m.N)
	}
	seenEndpoint := make(map[string]bool, m.N)
	for i, s := range m.Slots {
		// Routing uses array position as the slot index (place() returns an
		// index into Slots), so a per-slot "slot" field that disagrees with its
		// position would silently misroute every object that hashes there. Catch
		// a misordered/misnumbered owner-map file at load instead.
		if s.Slot != i {
			return nil, fmt.Errorf("owner map: slot at position %d declares slot=%d — slots must be in ascending order matching their index", i, s.Slot)
		}
		if s.Endpoint == "" {
			return nil, fmt.Errorf("owner map: slot %d (%q) has an empty endpoint", i, s.NodeId)
		}
		if seenEndpoint[s.Endpoint] {
			return nil, fmt.Errorf("owner map: duplicate endpoint %q at slot %d — each slot needs its own gateway", s.Endpoint, i)
		}
		seenEndpoint[s.Endpoint] = true
	}
	return &m, nil
}
