package util

import (
	"context"
	"time"
)

func SleepContext(ctx context.Context, seconds int) {
	select {
	case <-time.After(seconds * time.Second):
	case <-ctx.Done():
	}
}
