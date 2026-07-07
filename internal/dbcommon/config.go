// Package dbcommon holds the connection-config helpers shared by the
// postgres-source and postgres-sink components within this connector repo.
//
// Source and sink each embed DSNConfig for the shared connection fields and add
// their own component-specific fields (source: BatchRows; sink: Columns). This
// stops the two packages from drifting — buildDSN, defaults, and the common
// required-field checks live in one place. This is an INTRA-repo helper (same
// status as internal/dbbatch); it does not cross the standalone-repo boundary.
package dbcommon

import (
	"encoding/json"
	"fmt"
	"net/url"
)

// Defaults shared by source and sink (design §4 / §7).
const (
	DefaultPort    = 5432
	DefaultSSLMode = "disable"
)

// DSNConfig carries the connection fields common to both components. Each
// component's Config embeds this and may add its own fields.
type DSNConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	Table    string `json:"table"`
	SSLMode  string `json:"sslmode"`
}

// Parse unmarshals raw JSON into dst, wrapping errors with "<prefix>: config:".
func Parse(raw string, prefix string, dst any) error {
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		return fmt.Errorf("%s: config: %w", prefix, err)
	}
	return nil
}

// ValidateCommon enforces the required connection fields shared by both
// components (design §4 / §7). Component-specific required fields (e.g. the
// source's columns) are checked by the component itself.
func ValidateCommon(c DSNConfig, prefix string) error {
	if c.Host == "" {
		return fmt.Errorf("%s: host is required", prefix)
	}
	if c.Database == "" {
		return fmt.Errorf("%s: database is required", prefix)
	}
	if c.User == "" {
		return fmt.Errorf("%s: user is required", prefix)
	}
	if c.Password == "" {
		return fmt.Errorf("%s: password is required", prefix)
	}
	if c.Table == "" {
		return fmt.Errorf("%s: table is required", prefix)
	}
	return nil
}

// ApplyDefaults fills Port and SSLMode when unset (design §4 / §7).
func ApplyDefaults(c *DSNConfig) {
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.SSLMode == "" {
		c.SSLMode = DefaultSSLMode
	}
}

// BuildDSN constructs the pgx connection URL via net/url so the password is
// URL-encoded safely (url.UserPassword handles special chars). The password is
// never logged by this package.
func BuildDSN(c DSNConfig) string {
	u := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}
	if c.User != "" {
		u.User = url.UserPassword(c.User, c.Password)
	}
	q := u.Query()
	q.Set("sslmode", c.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}
