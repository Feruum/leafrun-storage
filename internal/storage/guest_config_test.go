package storage

import (
	"github.com/leafrun/leafrun-platform/authn"
	"testing"
)

func TestLoadConfigCarriesDistinctGuestCookieNames(t *testing.T) {
	values := map[string]string{"LEAFRUN_STORAGE_AUTH_MODE": "access", "LEAFRUN_STORAGE_DATABASE_URL": "postgres://db/storage", "LEAFRUN_STORAGE_S3_BUCKET": "private", "LEAFRUN_STORAGE_OWNER_URL": "http://owner.example", "LEAFRUN_STORAGE_OWNER_TOKEN": "owner-credential-123456", "LEAFRUN_STORAGE_ACCESS_URL": "http://access.example", "LEAFRUN_STORAGE_ACCESS_TOKEN": "access-credential-123456", "LEAFRUN_STORAGE_BROWSER_ORIGINS_JSON": `["https://app.example"]`, "LEAFRUN_STORAGE_SERVICE_CREDENTIALS": `[{"token":"service-credential-123456","subject":"generation","domain":"service","actions":["snapshot.get"]}]`, "LEAFRUN_STORAGE_SESSION_COOKIE": "custom_human", "LEAFRUN_GUEST_COOKIE_NAME": "custom_guest"}
	for key, value := range values {
		t.Setenv(key, value)
	}
	t.Setenv("LEAFRUN_STORAGE_DEV_FIXTURES", "")
	t.Setenv("LEAFRUN_STORAGE_IMPORT_VERIFIER_TOKEN", "")
	c, err := LoadConfig()
	if err != nil || c.GuestCookieName != "custom_guest" || c.CookieName != "custom_human" {
		t.Fatalf("configured cookies not carried: %v", err)
	}
	t.Setenv("LEAFRUN_GUEST_COOKIE_NAME", "")
	c, err = LoadConfig()
	if err != nil || c.GuestCookieName != authn.DefaultGuestCookieName {
		t.Fatalf("guest default not carried: %v", err)
	}
	for _, guest := range []string{"custom_human", "bad name"} {
		t.Setenv("LEAFRUN_GUEST_COOKIE_NAME", guest)
		if _, err := LoadConfig(); err == nil {
			t.Fatal("invalid/colliding cookie names accepted")
		}
	}
}
