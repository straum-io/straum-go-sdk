package wire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Hand-built version 1 response bodies (Straum docs/protocol.md section
// 10.2).

func le16(v uint16) []byte { return binary.LittleEndian.AppendUint16(nil, v) }
func le32(v uint32) []byte { return binary.LittleEndian.AppendUint32(nil, v) }
func le64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }
func str(s string) []byte  { return append(le16(uint16(len(s))), s...) }
func cat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

// item prefixes the concatenated fields with their length as a uvarint.
func item(fields ...[]byte) []byte {
	b := cat(fields...)
	return append(binary.AppendUvarint(nil, uint64(len(b))), b...)
}

func TestV1SkipsUnknownBytes(t *testing.T) {
	pad := bytes.Repeat([]byte{0xee}, 200) // a 2-byte length for the second item
	body := cat(le32(2),
		item(str("a"), le32(1), []byte{9, 9}),
		item(str("b"), le32(3), pad),
		str("b"),
		[]byte{1, 2, 3})
	resp, err := wire.DecodeResponseVersion(1, wire.TypeListTopics|wire.ResponseBit, body)
	if err != nil {
		t.Fatal(err)
	}
	l := resp.(*wire.ListTopicsOk)
	if len(l.Topics) != 2 || l.Topics[0] != (wire.TopicSummary{Name: "a", Partitions: 1}) ||
		l.Topics[1] != (wire.TopicSummary{Name: "b", Partitions: 3}) || l.NextCursor != "b" {
		t.Errorf("got %+v", l)
	}
	if _, err := wire.DecodeResponse(wire.TypeListTopics|wire.ResponseBit, body); err == nil {
		t.Error("version 0 decoded a version 1 body")
	}
}

func TestV1FetchedMessage(t *testing.T) {
	body := cat(le32(7), le64(0), le64(2), le32(1),
		item(le64(1), le64(1000), []byte{1}, str("k"), le32(1), []byte("x"), []byte{0xaa}))
	resp, err := wire.DecodeResponseVersion(1, wire.TypeFetch|wire.ResponseBit, body)
	if err != nil {
		t.Fatal(err)
	}
	f := resp.(*wire.FetchOk)
	if f.LeaderEpoch != 7 || f.NextSeq != 2 || len(f.Messages) != 1 {
		t.Fatalf("got %+v", f)
	}
	m := f.Messages[0]
	if m.Seq != 1 || m.TimestampMs != 1000 || string(m.Key) != "k" || string(m.Body) != "x" {
		t.Errorf("got %+v", m)
	}
}

func TestV1Metadata(t *testing.T) {
	partition := item(le32(0), le32(1), le32(4), le32(1), le32(1), le32(1), le32(1), le64(0), le64(5), []byte{0})
	body := cat(str("c"), le32(1),
		le32(1), item(le32(1), str("h"), le16(7171), str(""), []byte{0}),
		le32(1), item(str("t"), le16(0), le32(1), partition, []byte{0}))
	resp, err := wire.DecodeResponseVersion(1, wire.TypeMetadata|wire.ResponseBit, body)
	if err != nil {
		t.Fatal(err)
	}
	m := resp.(*wire.MetadataOk)
	if len(m.Nodes) != 1 || m.Nodes[0].Port != 7171 || len(m.Topics) != 1 ||
		len(m.Topics[0].Partitions) != 1 || m.Topics[0].Partitions[0].NextSeq != 5 ||
		m.Topics[0].Partitions[0].LeaderEpoch != 4 {
		t.Errorf("got %+v", m)
	}
}

func TestV1DecodeErrors(t *testing.T) {
	topics := wire.TypeListTopics | wire.ResponseBit
	// Bytes after a bad length, so the count check passes and the length
	// itself is what fails.
	rest := make([]byte, 8)
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"non-minimal varint", cat(le32(1), []byte{0x80, 0x00}, rest), wire.ErrInvalidValue},
		{"varint over 5 bytes", cat(le32(1), []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, rest), wire.ErrInvalidValue},
		{"varint above 2^32-1", cat(le32(1), []byte{0xff, 0xff, 0xff, 0xff, 0x10}, rest), wire.ErrInvalidValue},
		{"length overrun", cat(le32(1), []byte{20}, str("a"), le32(1), str("")), wire.ErrTruncated},
		{"field cut by its item", cat(le32(1), []byte{5}, str("a"), []byte{1, 0}, str("")), wire.ErrTruncated},
		{"body ends in a field", cat(le32(1), item(str("a"), le32(1)), []byte{1}), wire.ErrTruncated},
	}
	for _, c := range cases {
		if _, err := wire.DecodeResponseVersion(1, topics, c.body); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestV1Error(t *testing.T) {
	body := cat(le16(uint16(straumerr.Overloaded)), str("busy"), le32(0), le32(0), le32(50), le16(0), le16(0))
	extended := cat(body, []byte{1, 2})
	resp, err := wire.DecodeResponseVersion(1, wire.TypeError, extended)
	if err != nil {
		t.Fatal(err)
	}
	if e := resp.(*wire.Error); e.Code != straumerr.Overloaded || e.RetryAfterMs != 50 {
		t.Errorf("got %+v", e)
	}
	// Before HelloOk, and on a version 0 connection, an error is strict.
	if _, err := wire.DecodeResponse(wire.TypeError, extended); !errors.Is(err, wire.ErrTrailingBytes) {
		t.Errorf("version 0 error with trailing bytes: %v", err)
	}
}

func TestHelloOkLeadingFields(t *testing.T) {
	fields := func(version uint16) []byte {
		return cat(le16(version), str("c"), le32(1), le32(8<<20), le32(1<<20))
	}
	// A version 1 HelloOk appends fields after the five leading ones.
	v1 := cat(fields(1), le32(0), []byte{1})
	resp, err := wire.DecodeResponse(wire.TypeHello|wire.ResponseBit, v1)
	if err != nil {
		t.Fatal(err)
	}
	if h := resp.(*wire.HelloOk); h.Version != 1 || h.ClusterID != "c" || h.MaxMessageBytes != 1<<20 {
		t.Errorf("got %+v", h)
	}
	if _, err := wire.DecodeResponse(wire.TypeHello|wire.ResponseBit, cat(fields(0), []byte{0})); !errors.Is(err, wire.ErrTrailingBytes) {
		t.Errorf("version 0 HelloOk with trailing bytes: %v", err)
	}
	if _, err := wire.DecodeResponse(wire.TypeHello|wire.ResponseBit, fields(1)[:10]); !errors.Is(err, wire.ErrTruncated) {
		t.Errorf("cut HelloOk: %v", err)
	}
}
