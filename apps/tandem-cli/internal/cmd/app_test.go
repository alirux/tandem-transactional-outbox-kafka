package cmd

import "testing"

func TestColorEnabled_onlyOnATerminalThatTakesColor(t *testing.T) {
	cases := []struct {
		name          string
		tty           bool
		noColor, term string
		want          bool
	}{
		{"terminal", true, "", "xterm-256color", true},
		{"terminal with TERM unset", true, "", "", true},
		{"pipe or file", false, "", "xterm-256color", false},
		{"NO_COLOR set", true, "1", "xterm-256color", false},
		{"dumb terminal", true, "", "dumb", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("NO_COLOR", c.noColor)
			t.Setenv("TERM", c.term)
			if got := colorEnabled(c.tty); got != c.want {
				t.Errorf("colorEnabled(%v) = %v, want %v", c.tty, got, c.want)
			}
		})
	}
}
