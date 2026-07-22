package config

import (
	"fmt"
	"time"
)

// OPAConfig holds Open Policy Agent configuration for external authorization.
// When enabled, the OPA middleware sends policy input to OPA on every request
// and enforces the allow/deny decision. Fail-closed: denies if OPA is unreachable.
type OPAConfig struct {
	URL     string        `mapstructure:"url" json:"url"`
	Timeout time.Duration `mapstructure:"timeout" json:"timeout"`
	Enabled bool          `mapstructure:"enabled" json:"enabled"`
}

func NewOPAConfig() *OPAConfig {
	return &OPAConfig{
		Enabled: false,
		URL:     "http://localhost:8181/v1/data/hyperfleet/authz/allow",
		Timeout: 5 * time.Second,
	}
}

func (c *OPAConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.URL == "" {
		return fmt.Errorf("opa.url is required when OPA is enabled")
	}
	if c.Timeout < 100*time.Millisecond {
		return fmt.Errorf("opa.timeout must be at least 100ms, got %v", c.Timeout)
	}
	return nil
}
