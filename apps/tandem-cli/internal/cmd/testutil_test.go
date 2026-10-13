package cmd

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/alirux/tandem-transactional-outbox-kafka/apps/tandem-cli/internal/exitcode"
)

func TestMain(m *testing.M) {
	// Tests opt into connection settings with t.Setenv, independently of the shell.
	for _, name := range []string{"TANDEM_ADMIN_URL", "TANDEM_ADMIN_TOKEN", "TANDEM_ADMIN_API_KEY", "TANDEM_ADMIN_CA_CERT"} {
		if err := os.Unsetenv(name); err != nil {
			fmt.Fprintf(os.Stderr, "unsetting %s: %v\n", name, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// execute runs a fresh root command end to end (LLD-cli.md §10): real cobra parsing, real
// flag/env resolution, a real HTTP round trip to server, no mocks. Every case gets its own
// NewRootCmd(), so nothing leaks between them even when run in parallel.
func execute(t *testing.T, baseURL string, args ...string) (stdout, stderr string, code exitcode.Code) {
	t.Helper()
	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"--base-url", baseURL}, args...))

	err := root.Execute()
	return out.String(), errOut.String(), exitcode.CodeOf(err)
}

// appWithColor is the minimal App a pairs/row rendering helper needs: nothing but the
// color decision, since those helpers never touch the client or the streams.
func appWithColor(color bool) *App {
	return &App{IO: IOStreams{Color: color}}
}

// executeNoServer is execute without prepending --base-url, for cases exercising base-URL
// resolution itself (missing, env-var fallback) or pure usage-error paths that never reach
// the network.
func executeNoServer(t *testing.T, args ...string) (stdout, stderr string, code exitcode.Code) {
	t.Helper()
	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)

	err := root.Execute()
	return out.String(), errOut.String(), exitcode.CodeOf(err)
}
