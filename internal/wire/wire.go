// Package wire encodes and decodes the frames of the Straum protocol,
// version 0 (docs/protocol.md in the Straum repository, at the commit named
// in this module's CONTRACT file). It does no I/O. DecodeResponseVersion
// also decodes responses under the extensibility rules of version 1, which
// the client does not offer yet.
//
// Decoding never panics: every input gives a value or an error. Byte
// fields of a decoded value (keys, header names and values, bodies, the
// auth payload) alias the body passed to the decoder; copy them to keep
// them past the body's reuse.
//
// A nil Message.Key is an absent key; a non-nil empty key is a present,
// empty one. The two encode differently.
package wire

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Version is the protocol version this package speaks.
const Version uint16 = 0

const (
	// HeaderSize is the fixed part of every frame:
	// length u32 | type u16 | correlation_id u64.
	HeaderSize = 14
	// MinFrameLength is the smallest valid length field: type and
	// correlation id, no body.
	MinFrameLength = 10
	// ResponseBit is set in the type of every success response.
	ResponseBit uint16 = 0x8000
)

// Message types (docs/protocol.md section 4). A success response's type is
// its request's type with ResponseBit set.
const (
	TypeHello         uint16 = 0x0001
	TypePing          uint16 = 0x0002
	TypeMetadata      uint16 = 0x0003
	TypeCreateTopic   uint16 = 0x0004
	TypeDeleteTopic   uint16 = 0x0005
	TypeProduce       uint16 = 0x0006
	TypeFetch         uint16 = 0x0007
	TypeListTopics    uint16 = 0x0008
	TypeDescribeTopic uint16 = 0x0009
	TypeBrokerStats   uint16 = 0x000a
	TypeError         uint16 = 0xffff
)

// Produce.AckLevel values. Version 0 brokers support only AckDurable.
const (
	AckWritten    uint8 = 0
	AckDurable    uint8 = 1
	AckReplicated uint8 = 2
)

const (
	flagKey     = 1
	flagHeaders = 2
	// Smallest encoded message: flags and an empty body.
	minMessage = 5
)

// FrameHeader is the fixed part of a frame.
type FrameHeader struct {
	// Length counts the bytes after the length field: 10 + body length.
	Length        uint32
	Type          uint16
	CorrelationID uint64
}

// ParseHeader reads a frame header from the first HeaderSize bytes of b.
func ParseHeader(b []byte) (FrameHeader, error) {
	if len(b) < HeaderSize {
		return FrameHeader{}, ErrTruncated
	}
	h := FrameHeader{
		Length:        binary.LittleEndian.Uint32(b),
		Type:          binary.LittleEndian.Uint16(b[4:]),
		CorrelationID: binary.LittleEndian.Uint64(b[6:]),
	}
	if h.Length < MinFrameLength {
		return h, fmt.Errorf("%w: frame length %d below %d", ErrInvalidValue, h.Length, MinFrameLength)
	}
	return h, nil
}

// BodyLen is the number of body bytes after the header.
func (h FrameHeader) BodyLen() int { return int(h.Length - MinFrameLength) }

// Header is one message header. Names need not be unique; order is kept.
type Header struct {
	Name  []byte
	Value []byte
}

// Message is the unit of Produce and Fetch (section 4.6).
type Message struct {
	Key     []byte // nil: no key; non-nil, possibly empty: a key
	Headers []Header
	Body    []byte
}

func (m *Message) encode(w *writer) {
	var flags uint8
	if m.Key != nil {
		flags |= flagKey
	}
	if len(m.Headers) > 0 {
		flags |= flagHeaders
	}
	w.u8(flags)
	if m.Key != nil {
		w.shortBytes("key", m.Key)
	}
	if len(m.Headers) > 0 {
		if len(m.Headers) > math.MaxUint16 {
			w.tooLong("headers", len(m.Headers))
		}
		w.u16(uint16(len(m.Headers)))
		for _, h := range m.Headers {
			w.shortBytes("header name", h.Name)
			w.bytes("header value", h.Value)
		}
	}
	w.bytes("body", m.Body)
}

func decodeMessage(r *reader) Message {
	var m Message
	flags := r.u8()
	if flags&^(flagKey|flagHeaders) != 0 {
		r.invalid("reserved message flags")
		return m
	}
	if flags&flagKey != 0 {
		m.Key = r.shortBytes()
	}
	if flags&flagHeaders != 0 {
		n := int(r.u16())
		if r.err == nil && n == 0 {
			r.invalid("headers flag with no headers")
			return m
		}
		// Each header takes at least 6 bytes; check before allocating.
		if n*6 > len(r.b) {
			r.fail(ErrTruncated)
			return m
		}
		m.Headers = make([]Header, 0, n)
		for range n {
			name := r.shortBytes()
			value := r.bytes()
			if r.err != nil {
				return m
			}
			m.Headers = append(m.Headers, Header{Name: name, Value: value})
		}
	}
	m.Body = r.bytes()
	return m
}

// Request is a request body. The types of this package that implement it
// are pointers: *Hello, *Ping, *Metadata, *CreateTopic, *DeleteTopic,
// *Produce, *Fetch, *ListTopics, *DescribeTopic, *BrokerStats.
type Request interface {
	Type() uint16
	encode(w *writer)
	isRequest()
}

// Response is a response body: *HelloOk, *Pong, *MetadataOk,
// *CreateTopicOk, *DeleteTopicOk, *ProduceOk, *FetchOk, *ListTopicsOk,
// *DescribeTopicOk, *BrokerStatsOk or *Error.
type Response interface {
	Type() uint16
	encode(w *writer)
	isResponse()
}

// AppendRequest appends one complete frame holding req to dst.
func AppendRequest(dst []byte, correlationID uint64, req Request) ([]byte, error) {
	return appendFrame(dst, req.Type(), correlationID, req.encode)
}

// AppendResponse appends one complete frame holding resp to dst.
func AppendResponse(dst []byte, correlationID uint64, resp Response) ([]byte, error) {
	return appendFrame(dst, resp.Type(), correlationID, resp.encode)
}

func appendFrame(dst []byte, typ uint16, corr uint64, body func(*writer)) ([]byte, error) {
	start := len(dst)
	w := writer{b: dst}
	w.u32(0)
	w.u16(typ)
	w.u64(corr)
	body(&w)
	if w.err != nil {
		return dst, w.err
	}
	length := len(w.b) - start - 4
	if uint64(length) > math.MaxUint32 {
		return dst, fmt.Errorf("%w: frame has %d bytes", ErrTooLong, length)
	}
	binary.LittleEndian.PutUint32(w.b[start:], uint32(length))
	return w.b, nil
}

// SplitFrame splits a buffer holding exactly one frame into its header and
// body.
func SplitFrame(frame []byte) (FrameHeader, []byte, error) {
	h, err := ParseHeader(frame)
	if err != nil {
		return h, nil, err
	}
	body := frame[HeaderSize:]
	if len(body) < h.BodyLen() {
		return h, nil, ErrTruncated
	}
	if len(body) > h.BodyLen() {
		return h, nil, ErrTrailingBytes
	}
	return h, body, nil
}

// Partition is the default partitioner of docs/protocol.md section 7:
// crc32c(key) mod partitionCount. A partitionCount of 0 gives 0.
func Partition(key []byte, partitionCount uint32) uint32 {
	if partitionCount == 0 {
		return 0
	}
	return crc32.Checksum(key, castagnoli) % partitionCount
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrorFromResponse converts an Error response to the error returned to
// callers.
func ErrorFromResponse(e *Error) *straumerr.Error {
	return &straumerr.Error{
		Code:         e.Code,
		Message:      e.Message,
		LeaderNodeID: e.LeaderNodeID,
		LeaderEpoch:  e.LeaderEpoch,
		RetryAfterMs: e.RetryAfterMs,
		MinVersion:   e.MinVersion,
		MaxVersion:   e.MaxVersion,
	}
}
