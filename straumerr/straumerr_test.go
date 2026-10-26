package straumerr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/straum-io/straum-go-sdk/straumerr"
)

// The table of docs/protocol.md section 5: names and retry classes.
func TestCodes(t *testing.T) {
	want := []struct {
		name  string
		retry straumerr.Retry
	}{
		{"UnknownTopic", straumerr.NoRetry},
		{"UnknownPartition", straumerr.NoRetry},
		{"TopicExists", straumerr.NoRetry},
		{"MessageTooLarge", straumerr.NoRetry},
		{"FrameTooLarge", straumerr.NoRetry},
		{"Overloaded", straumerr.RetryAfterBackoff},
		{"UnsupportedAckLevel", straumerr.NoRetry},
		{"UnsupportedVersion", straumerr.NoRetry},
		{"Unsupported", straumerr.NoRetry},
		{"Unauthenticated", straumerr.NoRetry},
		{"Forbidden", straumerr.NoRetry},
		{"MalformedRequest", straumerr.NoRetry},
		{"NotLeader", straumerr.RetryAfterMetadata},
		{"FencedLeaderEpoch", straumerr.RetryAfterMetadata},
		{"StorageError", straumerr.NoRetry},
	}
	for i, w := range want {
		c := straumerr.Code(i + 1)
		if c.String() != w.name || c.Retry() != w.retry || !c.Known() {
			t.Errorf("code %d: %s %v %v, want %s %v", i+1, c, c.Retry(), c.Known(), w.name, w.retry)
		}
		if got, ok := straumerr.CodeByName(w.name); !ok || got != c {
			t.Errorf("CodeByName(%s) = %d %v", w.name, got, ok)
		}
	}
	for _, c := range []straumerr.Code{0, 16, 999} {
		if c.Known() || c.Retry() != straumerr.NoRetry || c.String() != fmt.Sprint(uint16(c)) {
			t.Errorf("unknown code %d: known %v retry %v name %s", c, c.Known(), c.Retry(), c)
		}
	}
	if _, ok := straumerr.CodeByName("Nope"); ok {
		t.Error("CodeByName accepted an unknown name")
	}
}

func TestErrorMatchesItsCode(t *testing.T) {
	err := fmt.Errorf("produce: %w", &straumerr.Error{Code: straumerr.Overloaded, Message: "busy", RetryAfterMs: 5})
	if !errors.Is(err, straumerr.Overloaded) || errors.Is(err, straumerr.NotLeader) {
		t.Fatal("errors.Is does not match the code")
	}
	var se *straumerr.Error
	if !errors.As(err, &se) || se.RetryAfterMs != 5 {
		t.Fatal("errors.As failed")
	}
	if se.Error() != "straum: Overloaded: busy" || (&straumerr.Error{Code: 999}).Error() != "straum: 999" {
		t.Errorf("messages: %q", se.Error())
	}
}
