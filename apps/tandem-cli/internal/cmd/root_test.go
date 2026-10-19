package cmd

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alirux/tandem-transactional-outbox-kafka/apps/tandem-cli/internal/output"
)

func TestSetupApp_invalidOutputFlagIsAUsageError(t *testing.T) {
	_, _, code := executeNoServer(t, "--base-url", "http://unused.invalid", "--output", "xml", "outbox", "summary")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
}

func TestSetupApp_insecureWarnsOnStderrThroughARealInvocation(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})

	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"--base-url", server.URL, "--insecure", "outbox", "summary"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(errOut.String(), "--insecure") {
		t.Errorf("stderr = %q, want the --insecure warning", errOut.String())
	}
}

func TestPrepareForDocGeneration_disablesTheAutoGenTagOnTheWholeTree(t *testing.T) {
	root := NewRootCmd()
	PrepareForDocGeneration(root)

	if !root.DisableAutoGenTag {
		t.Error("root.DisableAutoGenTag = false, want true")
	}
	for _, c := range root.Commands() {
		if !c.DisableAutoGenTag {
			t.Errorf("command %q: DisableAutoGenTag = false, want true", c.Name())
		}
	}
}

func TestPrepareForDocGeneration_includesHelpAndCompletionInTheTree(t *testing.T) {
	root := NewRootCmd()
	PrepareForDocGeneration(root)

	var hasHelp, hasCompletion bool
	for _, c := range root.Commands() {
		switch c.Name() {
		case "help":
			hasHelp = true
		case "completion":
			hasCompletion = true
		}
	}
	if !hasHelp {
		t.Error("root.Commands() missing help - PrepareForDocGeneration must materialize it")
	}
	if !hasCompletion {
		t.Error("root.Commands() missing completion - PrepareForDocGeneration must materialize it")
	}
}

func TestHelp_completionSortsLastAfterHelpItself(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	text := out.String()
	helpIdx := strings.Index(text, "\n  help ")
	completionIdx := strings.Index(text, "\n  completion ")
	if helpIdx == -1 || completionIdx == -1 {
		t.Fatalf("expected both help and completion listed, got:\n%s", text)
	}
	if completionIdx < helpIdx {
		t.Errorf("completion (at %d) should sort after help (at %d):\n%s", completionIdx, helpIdx, text)
	}
}

func TestHelp_doesNotRequireABaseURL(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("`tandem-cli help` must not require --base-url, got error: %v", err)
	}
	if !strings.Contains(out.String(), "Available Commands:") {
		t.Errorf("expected the command list, got:\n%s", out.String())
	}
}

func TestCompletion_doesNotRequireABaseURL(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"completion", "bash"})
	if err := root.Execute(); err != nil {
		t.Fatalf("`tandem-cli completion bash` must not require --base-url, got error: %v", err)
	}
	if !strings.Contains(out.String(), "bash completion") {
		t.Errorf("expected a bash completion script, got %d bytes", out.Len())
	}
}

func TestHelp_rootOpensWithThePlainLogoWhenNotATerminal(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	text := out.String()
	if !strings.HasPrefix(text, banner(false)) {
		t.Errorf("root help should open with the logo, got:\n%s", text)
	}
	if strings.Contains(text, "\x1b") {
		t.Errorf("help written to a non-terminal must carry no escape sequence, got %q", text)
	}
}

func TestHelp_subcommandHelpCarriesNoLogo(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"outbox", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	firstLine, _, _ := strings.Cut(banner(false), "\n")
	if strings.Contains(out.String(), firstLine) {
		t.Errorf("only the root help shows the logo, got:\n%s", out.String())
	}
}

func TestBanner_colorsTheArrowsOnlyWhenEnabled(t *testing.T) {
	colored := banner(true)
	for _, c := range []output.Color{logoBlue, logoGreen, logoTeal, logoOrange, logoPink} {
		if !strings.Contains(colored, string(c)) {
			t.Errorf("colored banner misses %q", c)
		}
	}
	sgr := regexp.MustCompile("\x1b\\[[0-9;]*m")
	if stripped := sgr.ReplaceAllString(colored, ""); stripped != banner(false) {
		t.Errorf("colored banner should differ from the plain one by escapes only:\n%s\nvs\n%s", stripped, banner(false))
	}
}

// A cell draws two stacked pixels in one foreground color, so two colored pixels sharing
// a cell would lose one of their colors.
func TestBanner_neverStacksTwoColorsInOneCell(t *testing.T) {
	grid := logoPixels()
	for y := 0; y < len(grid); y += 2 {
		for x, top := range grid[y] {
			bottom := grid[y+1][x]
			if top != '.' && bottom != '.' && top != bottom {
				t.Errorf("cell row:%d column:%d stacks %q over %q", y/2, x, top, bottom)
			}
		}
	}
}

func TestBanner_withoutColorPrintsTheLogoAtRestWithoutWaiting(t *testing.T) {
	var out bytes.Buffer
	writeBanner(&out, false, func(time.Duration) { t.Error("must not wait without color") })
	if out.String() != banner(false) {
		t.Errorf("got:\n%s\nwant:\n%s", out.String(), banner(false))
	}
}

func TestBanner_withColorDrawsTheLogoFromTheLeftThenLeavesItAtRest(t *testing.T) {
	var out bytes.Buffer
	var waited time.Duration
	writeBanner(&out, true, func(d time.Duration) { waited += d })

	frames := strings.Split(out.String(), cursorUp)
	if len(frames) != logoDrawFrames+1 {
		t.Fatalf("got %d drawings, want %d frames plus the logo at rest", len(frames), logoDrawFrames+1)
	}
	if last := frames[len(frames)-1]; last != banner(true) {
		t.Errorf("the animation must end on the logo at rest, got:\n%s", last)
	}
	if waited != logoDrawTime {
		t.Errorf("waited %v, want %v", waited, logoDrawTime)
	}

	// Each frame is written over the previous one without clearing it, so it may only add
	// pixels, never remove or move one.
	sgr := regexp.MustCompile("\x1b\\[[0-9;]*m")
	previous := ""
	for i, frame := range frames {
		current := sgr.ReplaceAllString(frame, "")
		prevLines, curLines := strings.Split(previous, "\n"), strings.Split(current, "\n")
		for y, prevLine := range prevLines {
			prevCells, curCells := []rune(prevLine), []rune(curLines[y])
			for x, cell := range prevCells {
				if cell != ' ' && (x >= len(curCells) || curCells[x] != cell) {
					t.Fatalf("frame %d loses the pixel at line:%d column:%d", i, y, x)
				}
			}
		}
		previous = current
	}
	drawn := func(s string) int { return len([]rune(strings.Join(strings.Fields(s), ""))) }
	if first := sgr.ReplaceAllString(frames[0], ""); drawn(first) >= drawn(previous) {
		t.Errorf("the first frame should draw only the logo's left edge, got:\n%s", first)
	}
}
