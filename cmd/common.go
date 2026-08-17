package cmd

import (
	"fmt"
)

const (
	red    = "\033[0;31m"
	green  = "\033[0;32m"
	yellow = "\033[0;33m"
	plain  = "\033[0m"
)

func Err(msg ...any) string {
	return red + fmt.Sprint(msg...) + plain
}

func Ok(msg ...any) string {
	return green + fmt.Sprint(msg...) + plain
}

func Warn(msg ...any) string {
	return yellow + fmt.Sprint(msg...) + plain
}
