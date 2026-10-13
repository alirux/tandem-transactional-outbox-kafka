package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alirux/tandem-transactional-outbox-kafka/apps/tandem-cli/internal/exitcode"
)

func TestMain(m *testing.M) {
	// Tests opt into connection settings with t.Setenv, independently of the shell.
	for _, name := range []string{envBaseURL, envToken, envAPIKey, envCACert} {
		if err := os.Unsetenv(name); err != nil {
			fmt.Fprintf(os.Stderr, "unsetting %s: %v\n", name, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func TestResolve_missingBaseURLIsAUsageError(t *testing.T) {
	_, err := Resolve(Options{}, nil)
	if err == nil {
		t.Fatal("Resolve(no base URL) = nil error, want a usage error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UsageError {
		t.Errorf("CodeOf(err) = %d, want UsageError (%d)", got, exitcode.UsageError)
	}
}

func TestResolve_baseURLFlagBeatsEnvVar(t *testing.T) {
	t.Setenv(envBaseURL, "https://from-env.example")
	resolved, err := Resolve(Options{BaseURL: "https://from-flag.example"}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.BaseURL != "https://from-flag.example" {
		t.Errorf("BaseURL = %q, want the flag value", resolved.BaseURL)
	}
}

func TestResolve_baseURLFallsBackToEnvVar(t *testing.T) {
	t.Setenv(envBaseURL, "https://from-env.example")
	resolved, err := Resolve(Options{}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.BaseURL != "https://from-env.example" {
		t.Errorf("BaseURL = %q, want the env value", resolved.BaseURL)
	}
}

func TestResolve_credentialsAreOptional(t *testing.T) {
	resolved, err := Resolve(Options{BaseURL: "https://admin.example"}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "https://admin.example/outbox/summary", nil)
	if applyErr := resolved.RequestEditor(context.Background(), req); applyErr != nil {
		t.Fatalf("RequestEditor() error = %v", applyErr)
	}
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("Authorization header = %q, want none", got)
	}
	if got := req.Header.Get("X-API-Key"); got != "" {
		t.Errorf("X-API-Key header = %q, want none", got)
	}
}

func TestResolve_requestEditorSetsBearerTokenAndAPIKeyIndependently(t *testing.T) {
	resolved, err := Resolve(Options{
		BaseURL: "https://admin.example",
		Token:   "tok-123",
		APIKey:  "key-456",
	}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "https://admin.example/outbox/summary", nil)
	if applyErr := resolved.RequestEditor(context.Background(), req); applyErr != nil {
		t.Fatalf("RequestEditor() error = %v", applyErr)
	}
	if got, want := req.Header.Get("Authorization"), "Bearer tok-123"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
	if got, want := req.Header.Get("X-API-Key"), "key-456"; got != want {
		t.Errorf("X-API-Key = %q, want %q", got, want)
	}
}

func TestResolve_credentialsFallBackToEnvVarsAndFlagsWin(t *testing.T) {
	const (
		envTokenValue = "env-token"
		flagToken     = "flag-token"
		envKeyValue   = "env-key"
		flagKey       = "flag-key"
	)
	for _, tc := range []struct {
		name     string
		envVar   string
		envValue string
		options  Options
		header   string
		want     string
	}{
		{"tokenFromEnv", envToken, envTokenValue, Options{}, "Authorization", "Bearer " + envTokenValue},
		{"tokenFlagBeatsEnv", envToken, envTokenValue, Options{Token: flagToken}, "Authorization", "Bearer " + flagToken},
		{"apiKeyFromEnv", envAPIKey, envKeyValue, Options{}, "X-API-Key", envKeyValue},
		{"apiKeyFlagBeatsEnv", envAPIKey, envKeyValue, Options{APIKey: flagKey}, "X-API-Key", flagKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envVar, tc.envValue)
			tc.options.BaseURL = "https://admin.example"
			resolved, err := Resolve(tc.options, nil)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			req := httptest.NewRequest(http.MethodGet, resolved.BaseURL+"/outbox/summary", nil)
			if applyErr := resolved.RequestEditor(context.Background(), req); applyErr != nil {
				t.Fatalf("RequestEditor() error = %v", applyErr)
			}
			if got := req.Header.Get(tc.header); got != tc.want {
				t.Errorf("%s = %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}

func TestResolve_extraHeaderOverridesAScheme(t *testing.T) {
	resolved, err := Resolve(Options{
		BaseURL: "https://admin.example",
		Token:   "tok-123",
		Headers: []string{"Authorization: Basic dXNlcjpwYXNz"},
	}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "https://admin.example/outbox/summary", nil)
	if applyErr := resolved.RequestEditor(context.Background(), req); applyErr != nil {
		t.Fatalf("RequestEditor() error = %v", applyErr)
	}
	if got, want := req.Header.Get("Authorization"), "Basic dXNlcjpwYXNz"; got != want {
		t.Errorf("Authorization = %q, want the --header override %q", got, want)
	}
}

func TestResolve_invalidHeaderFormatIsAUsageError(t *testing.T) {
	_, err := Resolve(Options{
		BaseURL: "https://admin.example",
		Headers: []string{"not-a-valid-header"},
	}, nil)
	if err == nil {
		t.Fatal("Resolve(bad --header) = nil error, want a usage error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UsageError {
		t.Errorf("CodeOf(err) = %d, want UsageError (%d)", got, exitcode.UsageError)
	}
}

func TestResolve_emptyHeaderNameAfterTrimIsAUsageError(t *testing.T) {
	_, err := Resolve(Options{
		BaseURL: "https://admin.example",
		Headers: []string{"   : value"},
	}, nil)
	if err == nil {
		t.Fatal("Resolve(blank --header name) = nil error, want a usage error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UsageError {
		t.Errorf("CodeOf(err) = %d, want UsageError (%d)", got, exitcode.UsageError)
	}
}

func TestResolve_caCertFileNotFoundIsAUsageError(t *testing.T) {
	_, err := Resolve(Options{
		BaseURL: "https://admin.example",
		CACert:  filepath.Join(t.TempDir(), "does-not-exist.pem"),
	}, nil)
	if err == nil {
		t.Fatal("Resolve(missing --ca-cert) = nil error, want a usage error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UsageError {
		t.Errorf("CodeOf(err) = %d, want UsageError (%d)", got, exitcode.UsageError)
	}
}

func TestResolve_caCertWithNoCertificatesIsAUsageError(t *testing.T) {
	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	writeFile(t, garbage, "not a pem file")

	_, err := Resolve(Options{BaseURL: "https://admin.example", CACert: garbage}, nil)
	if err == nil {
		t.Fatal("Resolve(garbage --ca-cert) = nil error, want a usage error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UsageError {
		t.Errorf("CodeOf(err) = %d, want UsageError (%d)", got, exitcode.UsageError)
	}
}

func TestResolve_validCaCertIsLoadedIntoTheTLSConfig(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "ca.pem")
	writeFile(t, certFile, selfSignedCAPEM(t))

	resolved, err := Resolve(Options{BaseURL: "https://admin.example", CACert: certFile}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	transport, ok := resolved.HTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("HTTPClient.Transport is not *http.Transport")
	}
	if transport.TLSClientConfig.RootCAs == nil {
		t.Error("TLSClientConfig.RootCAs is nil, want the loaded CA pool")
	}
}

func TestResolve_caCertFallsBackToEnvVarAndFlagWins(t *testing.T) {
	certDir := t.TempDir()
	envCertFile := filepath.Join(certDir, "env-ca.pem")
	flagCertFile := filepath.Join(certDir, "flag-ca.pem")
	envCert := selfSignedCAPEM(t)
	flagCert := selfSignedCAPEM(t)
	writeFile(t, envCertFile, envCert)
	writeFile(t, flagCertFile, flagCert)

	for _, tc := range []struct {
		name    string
		flag    string
		wantPEM string
	}{
		{"caCertFromEnv", "", envCert},
		{"caCertFlagBeatsEnv", flagCertFile, flagCert},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envCACert, envCertFile)
			resolved, err := Resolve(Options{BaseURL: "https://admin.example", CACert: tc.flag}, nil)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			transport, ok := resolved.HTTPClient.Transport.(*http.Transport)
			if !ok {
				t.Fatal("HTTPClient.Transport is not *http.Transport")
			}
			if transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil {
				t.Fatal("TLSClientConfig.RootCAs is nil, want the selected CA pool")
			}
			wantPool := x509.NewCertPool()
			if !wantPool.AppendCertsFromPEM([]byte(tc.wantPEM)) {
				t.Fatal("loading the expected CA certificate")
			}
			if !transport.TLSClientConfig.RootCAs.Equal(wantPool) {
				t.Error("TLSClientConfig.RootCAs does not match the selected CA certificate")
			}
		})
	}
}

func TestResolve_insecureSkipsVerificationAndWarnsEveryTime(t *testing.T) {
	var warnings []string
	resolved, err := Resolve(Options{BaseURL: "https://admin.example", Insecure: true},
		func(msg string) { warnings = append(warnings, msg) })
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	transport := resolved.HTTPClient.Transport.(*http.Transport)
	if !transport.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = false, want true")
	}
	if len(warnings) != 1 {
		t.Fatalf("warn called %d times, want exactly 1", len(warnings))
	}
}

func TestResolve_timeoutDefaultsWhenFlagNotPassed(t *testing.T) {
	resolved, err := Resolve(Options{BaseURL: "https://admin.example"}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.HTTPClient.Timeout != DefaultTimeout {
		t.Errorf("Timeout = %v, want default %v", resolved.HTTPClient.Timeout, DefaultTimeout)
	}
}

func TestResolve_timeoutZeroDisablesIt(t *testing.T) {
	resolved, err := Resolve(Options{
		BaseURL: "https://admin.example", Timeout: 0, TimeoutSet: true,
	}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.HTTPClient.Timeout != 0 {
		t.Errorf("Timeout = %v, want 0 (disabled)", resolved.HTTPClient.Timeout)
	}
}

func TestResolve_explicitTimeoutIsApplied(t *testing.T) {
	resolved, err := Resolve(Options{
		BaseURL: "https://admin.example", Timeout: 5 * time.Second, TimeoutSet: true,
	}, nil)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.HTTPClient.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", resolved.HTTPClient.Timeout)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func selfSignedCAPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tandem-cli test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
