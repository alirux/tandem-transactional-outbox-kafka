package cmd

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/alirux/tandem-transactional-outbox-kafka/apps/tandem-cli/internal/output"
)

// The logo's colors, from the 256-color palette rather than 24-bit: every terminal that
// passes the color gate renders these, while truecolor is still missing from some.
const (
	logoBlue   output.Color = "\033[38;5;33m"
	logoGreen  output.Color = "\033[38;5;76m"
	logoTeal   output.Color = "\033[38;5;43m"
	logoOrange output.Color = "\033[38;5;214m"
	logoPink   output.Color = "\033[38;5;204m"
)

// logoArrows is the graphic logo on an 8-pixel-high grid: two bars running into a pair of
// chevrons. Each pixel is the letter of its color in logoColors, '.' an empty one.
var logoArrows = [8]string{
	"......BB....OO...",
	".......BB....OO..",
	"BBBBBB..BB....OO.",
	".........BB....OO",
	".........TT....PP",
	"GGGGGG..TT....PP.",
	".......TT....PP..",
	"......TT....PP...",
}

var logoColors = map[byte]output.Color{
	'B': logoBlue, 'G': logoGreen, 'T': logoTeal, 'O': logoOrange, 'P': logoPink,
}

// logoGap is the empty pixel columns between the arrows and the name.
const logoGap = 3

// logoName spells "Tandem" in 8x8 bitmap letters, one byte per pixel row, leftmost pixel
// in the most significant bit. advance is how far the next letter starts: less than 8
// tucks it in, as the a does under the T's bar.
var logoName = []struct {
	rows    [8]byte
	advance int
}{
	{[8]byte{0xFC, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x00}, 5}, // T
	{[8]byte{0x00, 0x00, 0x3C, 0x06, 0x1E, 0x66, 0x3B, 0x00}, 8}, // a
	{[8]byte{0x00, 0x00, 0x7C, 0x66, 0x66, 0x66, 0x66, 0x00}, 8}, // n
	{[8]byte{0x0E, 0x06, 0x36, 0x6E, 0x66, 0x66, 0x3B, 0x00}, 8}, // d
	{[8]byte{0x00, 0x00, 0x3C, 0x66, 0x7E, 0x60, 0x3C, 0x00}, 8}, // e
	{[8]byte{0x00, 0x00, 0x7E, 0x6B, 0x6B, 0x6B, 0x6B, 0x00}, 8}, // m
}

// logoPixels lays the arrows and the name side by side on one grid. The name's pixels are
// 'N', which has no entry in logoColors: it keeps the terminal's own foreground, readable
// on a light theme and a dark one alike.
func logoPixels() [8][]byte {
	var grid [8][]byte
	for y := range grid {
		grid[y] = []byte(logoArrows[y] + strings.Repeat(".", logoGap))
	}
	x := len(grid[0])
	for _, letter := range logoName {
		for y := range grid {
			for len(grid[y]) < x+8 {
				grid[y] = append(grid[y], '.')
			}
			for b := range 8 {
				if letter.rows[y]>>(7-b)&1 == 1 {
					grid[y][x+b] = 'N'
				}
			}
		}
		x += letter.advance
	}
	return grid
}

// banner renders the Tandem logo as it rests, shown on top of the root command's help
// only. Each character cell holds two pixels stacked (▀ ▄ █), which keeps them square. It
// stays out of the command's Long text on purpose: that text also feeds the generated
// Markdown reference, where neither the art nor its color codes belong. color is the
// caller's decision, the same gate every other colored output passes (LLD-cli.md §6).
func banner(color bool) string {
	return renderLogo(func(_ int, pixel byte) (output.Color, bool) {
		if !color {
			return "", true
		}
		return logoColors[pixel], true
	}) + "\n"
}

// The draw-in animation: short enough not to hold up someone who only wants a flag's name.
const (
	logoDrawTime   = 700 * time.Millisecond
	logoDrawFrames = 20
)

// logoSweep is the run of colors the name takes just behind the drawing front, the logo's
// own from blue to pink.
var logoSweep = []output.Color{
	"\033[38;5;33m", "\033[38;5;39m", "\033[38;5;38m", "\033[38;5;44m",
	"\033[38;5;43m", "\033[38;5;42m", "\033[38;5;76m", "\033[38;5;112m",
	"\033[38;5;178m", "\033[38;5;214m", "\033[38;5;209m", "\033[38;5;204m",
}

// cursorUp moves the cursor to the start of the logo's first line, to draw the next frame
// over the previous one.
const cursorUp = "\033[4A\r"

// writeBanner prints the logo. With color, it first draws it from left to right in
// logoDrawTime: a frame only ever adds pixels, so each one is written over the previous
// without clearing it. Without color the output may not be a terminal at all, so the logo
// prints at rest at once. pause waits between frames (time.Sleep outside tests).
func writeBanner(w io.Writer, color bool, pause func(time.Duration)) {
	if color {
		for i := range logoDrawFrames {
			if i > 0 {
				_, _ = fmt.Fprint(w, cursorUp)
			}
			_, _ = fmt.Fprint(w, logoFrame(i))
			pause(logoDrawTime / logoDrawFrames)
		}
		_, _ = fmt.Fprint(w, cursorUp)
	}
	_, _ = fmt.Fprint(w, banner(color))
}

// logoFrame renders frame i of the draw-in: nothing right of the drawing front, the
// arrows in their colors behind it, and the name lit by logoSweep for a few columns
// before it settles into the terminal's foreground. The front leaves the last column
// together with the sweep, so the last frame shows the logo at rest.
func logoFrame(i int) string {
	width := len(logoPixels()[0])
	front := (i + 1) * (width + len(logoSweep)) / logoDrawFrames
	return renderLogo(func(x int, pixel byte) (output.Color, bool) {
		if x > front {
			return "", false
		}
		if c, ok := logoColors[pixel]; ok {
			return c, true
		}
		if behind := front - x; behind < len(logoSweep) {
			return logoSweep[len(logoSweep)-1-behind], true
		}
		return "", true
	})
}

// renderLogo draws the pixel grid two rows per line. paint gives a pixel's color ("" for
// the terminal's foreground) and whether it is drawn at all. A cell takes one color, its
// top pixel's or else its bottom one's: the drawing never stacks two colors in one cell,
// which would need a background color as well.
func renderLogo(paint func(x int, pixel byte) (output.Color, bool)) string {
	grid := logoPixels()
	var b strings.Builder
	for y := 0; y < len(grid); y += 2 {
		var line strings.Builder
		for x, top := range grid[y] {
			bottom := grid[y+1][x]
			pixel := top
			if pixel == '.' {
				pixel = bottom
			}
			c, drawn := paint(x, pixel)
			if pixel == '.' || !drawn {
				line.WriteByte(' ')
				continue
			}
			cell := "█"
			switch {
			case bottom == '.':
				cell = "▀"
			case top == '.':
				cell = "▄"
			}
			line.WriteString(output.Colorize(cell, c, c != ""))
		}
		b.WriteString(strings.TrimRight(line.String(), " "))
		b.WriteByte('\n')
	}
	return b.String()
}
