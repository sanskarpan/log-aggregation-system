package model

import (
	"testing"
	"time"
)

func TestTenantConfigValidate(t *testing.T) {
	tenant := TenantConfig{
		ID:   "default",
		Name: "Default",
		Limits: TenantLimits{
			MaxLabelsPerStream: 12,
			MaxBodyBytes:       1024,
			MaxParsedFields:    10,
			MaxFieldValueBytes: 128,
			Retention:          time.Hour,
		},
	}
	if err := tenant.Validate(); err != nil {
		t.Fatalf("expected valid tenant, got %v", err)
	}
}
