package util

import (
	"context"
	"time"
)

func SleepContext(ctx context.Context, seconds int) {
	select {
	// it's actually very counterintuitive that we have to cast second to time.Duration here, see
	// https://stackoverflow.com/questions/17573190/how-to-multiply-duration-by-integer#comment83798994_17573390
	case <-time.After(time.Duration(seconds) * time.Second):
	case <-ctx.Done():
	}
}
