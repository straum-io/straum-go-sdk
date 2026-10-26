// Package straumerr defines the error codes of the Straum protocol, version 0
// (docs/protocol.md section 5), with their retry classes, and the error a
// broker returns.
//
// A Code is itself an error, so a caller tests for one with errors.Is:
//
//	if errors.Is(err, straumerr.UnknownTopic) { ... }
package straumerr

import (
	"fmt"
	"strconv"
)

// Code is a protocol error code. Codes this version does not define are
// kept as received and are not retryable.
type Code uint16

// The codes of protocol version 0. Code 0 is never sent.
const (
	UnknownTopic        Code = 1
	UnknownPartition    Code = 2
	TopicExists         Code = 3
	MessageTooLarge     Code = 4
	FrameTooLarge       Code = 5
	Overloaded          Code = 6
	UnsupportedAckLevel Code = 7
	UnsupportedVersion  Code = 8
	Unsupported         Code = 9
	Unauthenticated     Code = 10
	Forbidden           Code = 11
	MalformedRequest    Code = 12
	NotLeader           Code = 13
	FencedLeaderEpoch   Code = 14
	StorageError        Code = 15
)

var names = [...]string{
	UnknownTopic:        "UnknownTopic",
	UnknownPartition:    "UnknownPartition",
	TopicExists:         "TopicExists",
	MessageTooLarge:     "MessageTooLarge",
	FrameTooLarge:       "FrameTooLarge",
	Overloaded:          "Overloaded",
	UnsupportedAckLevel: "UnsupportedAckLevel",
	UnsupportedVersion:  "UnsupportedVersion",
	Unsupported:         "Unsupported",
	Unauthenticated:     "Unauthenticated",
	Forbidden:           "Forbidden",
	MalformedRequest:    "MalformedRequest",
	NotLeader:           "NotLeader",
	FencedLeaderEpoch:   "FencedLeaderEpoch",
	StorageError:        "StorageError",
}

// Known reports whether c is a code of protocol version 0.
func (c Code) Known() bool { return int(c) < len(names) && names[c] != "" }

// String returns the code's name, or its number for an unknown code.
func (c Code) String() string {
	if c.Known() {
		return names[c]
	}
	return strconv.Itoa(int(c))
}

func (c Code) Error() string { return "straum: " + c.String() }

// CodeByName returns the code with the given name.
func CodeByName(name string) (Code, bool) {
	for i, n := range names {
		if n != "" && n == name {
			return Code(i), true
		}
	}
	return 0, false
}

// Retry is what the protocol allows a client to do after an error.
type Retry int

const (
	// NoRetry: the error goes back to the caller. Every unknown code is
	// in this class.
	NoRetry Retry = iota
	// RetryAfterMetadata: refresh Metadata, go to the partition's leader
	// and retry the same request (NotLeader, FencedLeaderEpoch).
	RetryAfterMetadata
	// RetryAfterBackoff: wait, then retry the same request (Overloaded).
	RetryAfterBackoff
)

// Retry returns the code's retry class (docs/protocol.md sections 5 and 6).
func (c Code) Retry() Retry {
	switch c {
	case NotLeader, FencedLeaderEpoch:
		return RetryAfterMetadata
	case Overloaded:
		return RetryAfterBackoff
	}
	return NoRetry
}

// Error is an error answered by a broker. errors.Is(err, code) matches its
// Code.
type Error struct {
	Code    Code
	Message string // for people; not to be parsed
	// LeaderNodeID and LeaderEpoch are set for NotLeader and
	// FencedLeaderEpoch.
	LeaderNodeID uint32
	LeaderEpoch  uint32
	// RetryAfterMs is the broker's suggested wait for Overloaded, 0 for none.
	RetryAfterMs uint32
	// MinVersion and MaxVersion are the broker's range for UnsupportedVersion.
	MinVersion uint16
	MaxVersion uint16
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code.Error()
	}
	return fmt.Sprintf("%s: %s", e.Code.Error(), e.Message)
}

// Unwrap returns the code, so errors.Is(err, straumerr.UnknownTopic) works.
func (e *Error) Unwrap() error { return e.Code }
