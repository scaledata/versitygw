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

// Package providers holds the pluggable IAM backend implementations
// (LDAP, Vault, S3-object) and the factory that selects among them, plus
// the lean built-in backends (internal, single, IPA). It is a separate
// package from auth so that embedders which only need auth's types and
// ACL/policy logic - and never call New - don't compile in the LDAP,
// Vault, and S3-transfer-manager client libraries these backends pull in.
package providers

import (
	"fmt"
	"time"

	"github.com/versity/versitygw/auth"
)

// New builds an IAMService from the given options, selecting the backend
// implied by which options are set. See auth.Opts for the fields.
func New(o *auth.Opts) (auth.IAMService, error) {
	var svc auth.IAMService
	var err error

	switch {
	case o.Dir != "":
		svc, err = auth.NewInternal(o.RootAccount, o.Dir)
		fmt.Printf("initializing internal IAM with %q\n", o.Dir)
	case o.LDAPServerURL != "":
		svc, err = NewLDAPService(o.RootAccount, o.LDAPServerURL, o.LDAPBindDN, o.LDAPPassword,
			o.LDAPQueryBase, o.LDAPAccessAtr, o.LDAPSecretAtr, o.LDAPRoleAtr, o.LDAPUserIdAtr,
			o.LDAPGroupIdAtr, o.LDAPProjectIdAtr, o.LDAPObjClasses, o.LDAPTLSSkipVerify)
		fmt.Printf("initializing LDAP IAM with %q\n", o.LDAPServerURL)
	case o.S3Endpoint != "":
		svc, err = NewS3(o.RootAccount, o.S3Access, o.S3Secret, o.S3Region, o.S3Bucket,
			o.S3Endpoint, o.S3DisableSSlVerfiy)
		fmt.Printf("initializing S3 IAM with '%v/%v'\n",
			o.S3Endpoint, o.S3Bucket)
	case o.VaultEndpointURL != "":
		svc, err = NewVaultIAMService(o.RootAccount, o.VaultEndpointURL, o.VaultNamespace, o.VaultSecretStoragePath, o.VaultSecretStorageNamespace,
			o.VaultAuthMethod, o.VaultAuthNamespace, o.VaultMountPath, o.VaultRootToken, o.VaultRoleId, o.VaultRoleSecret,
			o.VaultServerCert, o.VaultClientCert, o.VaultClientCertKey)
		fmt.Printf("initializing Vault IAM with %q\n", o.VaultEndpointURL)
	case o.IpaHost != "":
		svc, err = auth.NewIpaIAMService(o.RootAccount, o.IpaHost, o.IpaVaultName, o.IpaUser, o.IpaPassword, o.IpaInsecure)
		fmt.Printf("initializing IPA IAM with %q\n", o.IpaHost)
	default:
		// if no iam options selected, default to the single user mode
		fmt.Println("No IAM service configured, enabling single account mode")
		return auth.NewIAMServiceSingle(o.RootAccount), nil
	}

	if err != nil {
		return nil, err
	}

	if o.CacheDisable {
		return svc, nil
	}

	return auth.NewCache(svc,
		time.Duration(o.CacheTTL)*time.Second,
		time.Duration(o.CachePrune)*time.Second), nil
}
