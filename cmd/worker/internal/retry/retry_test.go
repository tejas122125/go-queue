package retry

import (
	"testing"
	"time"
)

func TestCalculateDelay(t *testing.T) {
	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{0, 5000 * time.Millisecond},
		{1, 10000 * time.Millisecond},
		{2, 20000 * time.Millisecond},
		{3, 40000 * time.Millisecond},
		{4, 60000 * time.Millisecond}, // capped at 60s
		{5, 60000 * time.Millisecond}, // capped at 60s
		{10, 60000 * time.Millisecond},
	}

	for _, tt := range tests {
		delay := CalculateDelay(tt.attempts)
		if delay != tt.expected {
			t.Errorf("CalculateDelay(%d) = %v, expected %v", tt.attempts, delay, tt.expected)
		}
	}
}

