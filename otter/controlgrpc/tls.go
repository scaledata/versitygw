// Copyright 2023 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package controlgrpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerTLSConfig builds a mutual-TLS config from cluster cert/key/CA files.
//
// This is the self-contained fallback for callers that have nothing better.
// A caller that can reach richer TLS policy (cluster cipher suites, version
// floors, rotation) should build its own *tls.Config and hand it to NewServer
// instead: this package cannot import that machinery, because a vendored
// module cannot import the module that vendors it without creating a cycle
// and breaking standalone builds of the fork.
func ServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("otter control: load keypair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("otter control: read CA %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("otter control: no certs parsed from %q", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
