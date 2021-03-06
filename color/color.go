// Package color TextColor coloring functionality for TTY output.
package color

import "fmt"

// number value constant of color.
const (
	Black = iota + 30
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	White
)

// TextColor coloring of message tag.
func TextColor(color int, str string) string {
	return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", color, str)
}
