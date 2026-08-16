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

package auth

import (
	"errors"

	"github.com/versity/versitygw/s3err"
)

type Role string

const (
	RoleUser     Role = "user"
	RoleAdmin    Role = "admin"
	RoleUserPlus Role = "userplus"
)

func (r Role) IsValid() bool {
	switch r {
	case RoleAdmin:
		return true
	case RoleUser:
		return true
	case RoleUserPlus:
		return true
	default:
		return false
	}
}

// Account is a gateway IAM account
type Account struct {
	Access    string `json:"access"`
	Secret    string `json:"secret"`
	Role      Role   `json:"role"`
	UserID    int    `json:"userID"`
	GroupID   int    `json:"groupID"`
	ProjectID int    `json:"projectID"`
}

type ListUserAccountsResult struct {
	Accounts []Account
}

// Mutable props, which could be changed when updating an IAM account
type MutableProps struct {
	Secret    *string `json:"secret"`
	Role      Role    `json:"role"`
	UserID    *int    `json:"userID"`
	GroupID   *int    `json:"groupID"`
	ProjectID *int    `json:"projectID"`
}

func (m MutableProps) Validate() error {
	if m.Role != "" && !m.Role.IsValid() {
		return s3err.GetAPIError(s3err.ErrAdminInvalidUserRole)
	}

	return nil
}

// UpdateAcc applies the given mutable properties onto acc. Exported so
// IAM backends that live outside this package (see auth/providers) can
// share the same update semantics as the backends in this package.
func UpdateAcc(acc *Account, props MutableProps) {
	if props.Secret != nil {
		acc.Secret = *props.Secret
	}
	if props.GroupID != nil {
		acc.GroupID = *props.GroupID
	}
	if props.UserID != nil {
		acc.UserID = *props.UserID
	}
	if props.ProjectID != nil {
		acc.ProjectID = *props.ProjectID
	}
	if props.Role != "" {
		acc.Role = props.Role
	}
}

// IAMService is the interface for all IAM service implementations
//
//go:generate moq -out ../s3api/controllers/iam_moq_test.go -pkg controllers . IAMService
type IAMService interface {
	CreateAccount(account Account) error
	GetUserAccount(access string) (Account, error)
	UpdateUserAccount(access string, props MutableProps) error
	DeleteUserAccount(access string) error
	ListUserAccounts() ([]Account, error)
	Shutdown() error
}

var (
	// ErrUserExists is returned when the user already exists
	ErrUserExists = errors.New("user already exists")
	// ErrNoSuchUser is returned when the user does not exist
	ErrNoSuchUser = errors.New("user not found")
)

type Opts struct {
	RootAccount                 Account
	Dir                         string
	LDAPServerURL               string
	LDAPBindDN                  string
	LDAPPassword                string
	LDAPQueryBase               string
	LDAPObjClasses              string
	LDAPAccessAtr               string
	LDAPSecretAtr               string
	LDAPRoleAtr                 string
	LDAPUserIdAtr               string
	LDAPGroupIdAtr              string
	LDAPProjectIdAtr            string
	LDAPTLSSkipVerify           bool
	VaultEndpointURL            string
	VaultNamespace              string
	VaultSecretStoragePath      string
	VaultSecretStorageNamespace string
	VaultAuthMethod             string
	VaultAuthNamespace          string
	VaultMountPath              string
	VaultRootToken              string
	VaultRoleId                 string
	VaultRoleSecret             string
	VaultServerCert             string
	VaultClientCert             string
	VaultClientCertKey          string
	S3Access                    string
	S3Secret                    string
	S3Region                    string
	S3Bucket                    string
	S3Endpoint                  string
	S3DisableSSlVerfiy          bool
	CacheDisable                bool
	CacheTTL                    int
	CachePrune                  int
	IpaHost                     string
	IpaVaultName                string
	IpaUser                     string
	IpaPassword                 string
	IpaInsecure                 bool
}

// New is implemented in auth/providers, which holds the pluggable backend
// implementations (LDAP, Vault, S3-object) so that embedders who never call
// New don't compile in their client libraries. See auth/providers.New.
