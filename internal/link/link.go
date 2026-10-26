// Package link gives the producer, consumer and admin packages, and the
// tests, the request path of a *client.Client. It takes internal/wire
// types, so package client does not export it; client sets these
// functions in its init. c must be a *client.Client.
package link

import (
	"context"

	"github.com/straum-io/straum-go-sdk/internal/wire"
)

var (
	// Do sends req to any broker, with the client's retries, and returns
	// its success response. An Error response is returned as a
	// *straumerr.Error.
	Do func(c any, ctx context.Context, req wire.Request) (wire.Response, error)

	// DoPartition sends the request build returns to the leader of a
	// partition, with the client's retries. build gets the
	// expected_leader_epoch to send: 0 on the first attempt, the epoch
	// Metadata reported after a leader error. Retries rebuild the request
	// with the same producer id and seq (build must return them unchanged).
	DoPartition func(c any, ctx context.Context, topic string, partition uint32, build func(epoch uint32) wire.Request) (wire.Response, error)
)
