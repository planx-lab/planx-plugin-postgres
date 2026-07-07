package dbcommon

import (
	"strings"
	"testing"
)

// dbcommon is the shared intra-repo helper extracted from source/sink. These
// tests pin the contract both components depend on: the error-prefix convention
// ("postgres <component>: ..."), the URL-encoding of the password, and defaults.

func TestParse_InvalidJSON_WrappedWithPrefix(t *testing.T) {
	var got DSNConfig
	err := Parse(`{not json`, "postgres source", &got)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "postgres source: config:") {
		t.Fatalf("error prefix missing: %q", err.Error())
	}
}

func TestParse_PopulatesEmbeddedStruct(t *testing.T) {
	type wrapper struct {
		DSNConfig
		Columns string `json:"columns"`
	}
	var got wrapper
	raw := `{"host":"h","port":5433,"database":"d","user":"u","password":"p","table":"t","sslmode":"require","columns":"a,b"}`
	if err := Parse(raw, "postgres source", &got); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Host != "h" || got.Port != 5433 || got.Database != "d" ||
		got.User != "u" || got.Password != "p" || got.Table != "t" ||
		got.SSLMode != "require" || got.Columns != "a,b" {
		t.Fatalf("fields not populated: %+v", got)
	}
}

func TestValidateCommon_RequiresEveryConnectionField(t *testing.T) {
	cases := []struct {
		missing string
		cfg     DSNConfig
	}{
		{"host", DSNConfig{Database: "d", User: "u", Password: "p", Table: "t"}},
		{"database", DSNConfig{Host: "h", User: "u", Password: "p", Table: "t"}},
		{"user", DSNConfig{Host: "h", Database: "d", Password: "p", Table: "t"}},
		{"password", DSNConfig{Host: "h", Database: "d", User: "u", Table: "t"}},
		{"table", DSNConfig{Host: "h", Database: "d", User: "u", Password: "p"}},
	}
	for _, c := range cases {
		t.Run(c.missing, func(t *testing.T) {
			err := ValidateCommon(c.cfg, "postgres sink")
			if err == nil {
				t.Fatalf("expected error for missing %s", c.missing)
			}
			if !strings.Contains(err.Error(), c.missing) {
				t.Fatalf("error should name %s: got %q", c.missing, err.Error())
			}
		})
	}
}

func TestValidateCommon_PassesWhenComplete(t *testing.T) {
	cfg := DSNConfig{Host: "h", Database: "d", User: "u", Password: "p", Table: "t"}
	if err := ValidateCommon(cfg, "postgres sink"); err != nil {
		t.Fatalf("expected nil for complete config: %v", err)
	}
}

func TestApplyDefaults_FillsPortAndSSLMode(t *testing.T) {
	cfg := DSNConfig{}
	ApplyDefaults(&cfg)
	if cfg.Port != DefaultPort {
		t.Errorf("Port = %d, want %d", cfg.Port, DefaultPort)
	}
	if cfg.SSLMode != DefaultSSLMode {
		t.Errorf("SSLMode = %q, want %q", cfg.SSLMode, DefaultSSLMode)
	}
}

func TestApplyDefaults_DoesNotOverwriteSetValues(t *testing.T) {
	cfg := DSNConfig{Port: 9999, SSLMode: "require"}
	ApplyDefaults(&cfg)
	if cfg.Port != 9999 {
		t.Errorf("Port overwritten: got %d, want 9999", cfg.Port)
	}
	if cfg.SSLMode != "require" {
		t.Errorf("SSLMode overwritten: got %q, want require", cfg.SSLMode)
	}
}

func TestBuildDSN_URLEncodesPassword(t *testing.T) {
	cfg := DSNConfig{
		Host:     "h",
		Port:     5432,
		Database: "d",
		User:     "u",
		Password: "p@ss/w0rd",
		Table:    "t",
		SSLMode:  "disable",
	}
	dsn := BuildDSN(cfg)
	// The password must be percent-encoded so special chars don't break the URL.
	if !strings.Contains(dsn, "p%40ss%2Fw0rd") {
		t.Fatalf("password not URL-encoded in DSN: %s", dsn)
	}
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Fatalf("expected postgres:// scheme: %s", dsn)
	}
	if !strings.Contains(dsn, "sslmode=disable") {
		t.Fatalf("sslmode not set: %s", dsn)
	}
}

func TestBuildDSN_OmitsUserInfoWhenUserEmpty(t *testing.T) {
	cfg := DSNConfig{Host: "h", Port: 5432, Database: "d", SSLMode: "disable"}
	dsn := BuildDSN(cfg)
	if strings.Contains(dsn, "@") {
		t.Fatalf("empty user should not produce userinfo: %s", dsn)
	}
}
