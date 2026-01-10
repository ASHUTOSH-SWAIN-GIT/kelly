package log

import "fmt"

func formatOffset(offset int64) string {
	return fmt.Sprintf("%020d", offset)
}
