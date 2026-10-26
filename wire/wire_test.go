package wire_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/straum-io/straum-go-sdk/straumerr"
	"github.com/straum-io/straum-go-sdk/wire"
)

func frameOf(t testing.TB, req wire.Request) []byte {
	t.Helper()
	b, err := wire.AppendRequest(nil, 1, req)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeErrors(t *testing.T) {
	produce := frameOf(t, &wire.Produce{Topic: "t", AckLevel: wire.AckDurable,
		Messages: []wire.Message{{Key: []byte("k"), Headers: []wire.Header{{Name: []byte("h"), Value: []byte("v")}}, Body: []byte("b")}}})
	body := produce[wire.HeaderSize:]
	// Offset of the message flags: topic (2+1), partition 4, ids 16, ack 1,
	// epoch 4, timeout 4, count 4.
	flags := 3 + 4 + 16 + 1 + 4 + 4 + 4
	mutate := func(f func(b []byte) []byte) []byte { return f(bytes.Clone(body)) }

	cases := []struct {
		name string
		typ  uint16
		body []byte
		want error
	}{
		{"empty produce", wire.TypeProduce, nil, wire.ErrTruncated},
		{"cut short", wire.TypeProduce, body[:len(body)-1], wire.ErrTruncated},
		{"trailing byte", wire.TypeProduce, append(bytes.Clone(body), 0), wire.ErrTrailingBytes},
		{"reserved flag", wire.TypeProduce, mutate(func(b []byte) []byte { b[flags] |= 4; return b }), wire.ErrInvalidValue},
		{"headers flag, zero headers", wire.TypeProduce, mutate(func(b []byte) []byte {
			// flags | key len 1 | 'k' | header count u16
			b[flags+4], b[flags+5] = 0, 0
			return b
		}), wire.ErrInvalidValue},
		{"huge count", wire.TypeMetadata, []byte{0xff, 0xff, 0xff, 0xff}, wire.ErrTruncated},
		{"invalid utf-8", wire.TypeDescribeTopic, []byte{2, 0, 0xc3, 0x28}, wire.ErrInvalidUTF8},
		{"unknown type", 0x0042, nil, wire.ErrUnknownType},
		{"response type as request", wire.TypePing | wire.ResponseBit, make([]byte, 8), wire.ErrUnknownType},
	}
	for _, c := range cases {
		if _, err := wire.DecodeRequest(c.typ, c.body); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
	if _, err := wire.DecodeResponse(wire.TypePing, make([]byte, 8)); !errors.Is(err, wire.ErrUnknownType) {
		t.Errorf("request type as response: %v", err)
	}
	if _, err := wire.DecodeResponse(wire.TypeFetch|wire.ResponseBit, append(make([]byte, 20), 0xff, 0xff, 0xff, 0xff)); !errors.Is(err, wire.ErrTruncated) {
		t.Errorf("fetch with a huge count: %v", err)
	}
}

func TestKeyPresence(t *testing.T) {
	for _, key := range [][]byte{nil, {}, []byte("k")} {
		b := frameOf(t, &wire.Produce{Topic: "t", Messages: []wire.Message{{Key: key, Body: []byte("x")}}})
		h, body, err := wire.SplitFrame(b)
		if err != nil {
			t.Fatal(err)
		}
		req, err := wire.DecodeRequest(h.Type, body)
		if err != nil {
			t.Fatal(err)
		}
		got := req.(*wire.Produce).Messages[0].Key
		if (got == nil) != (key == nil) || !bytes.Equal(got, key) {
			t.Errorf("key %q (nil %v) came back as %q (nil %v)", key, key == nil, got, got == nil)
		}
	}
}

func TestEncodeTooLong(t *testing.T) {
	long := strings.Repeat("x", 1<<16)
	for name, req := range map[string]wire.Request{
		"topic":   &wire.DescribeTopic{Name: long},
		"key":     &wire.Produce{Messages: []wire.Message{{Key: []byte(long)}}},
		"headers": &wire.Produce{Messages: []wire.Message{{Headers: make([]wire.Header, 1<<16)}}},
	} {
		if _, err := wire.AppendRequest(nil, 1, req); !errors.Is(err, wire.ErrTooLong) {
			t.Errorf("%s: got %v, want ErrTooLong", name, err)
		}
	}
}

func TestParseHeader(t *testing.T) {
	if _, err := wire.ParseHeader(make([]byte, 13)); !errors.Is(err, wire.ErrTruncated) {
		t.Errorf("short header: %v", err)
	}
	if _, err := wire.ParseHeader([]byte{9, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0}); !errors.Is(err, wire.ErrInvalidValue) {
		t.Errorf("length 9: %v", err)
	}
	b := frameOf(t, &wire.Ping{Nonce: 7})
	if _, _, err := wire.SplitFrame(b[:len(b)-1]); !errors.Is(err, wire.ErrTruncated) {
		t.Errorf("short frame: %v", err)
	}
	if _, _, err := wire.SplitFrame(append(b, 0)); !errors.Is(err, wire.ErrTrailingBytes) {
		t.Errorf("long frame: %v", err)
	}
}

func TestErrorFromResponse(t *testing.T) {
	err := wire.ErrorFromResponse(&wire.Error{Code: straumerr.NotLeader, LeaderNodeID: 2, LeaderEpoch: 3})
	if !errors.Is(err, straumerr.NotLeader) || err.LeaderNodeID != 2 || err.LeaderEpoch != 3 {
		t.Errorf("got %+v", err)
	}
}

func TestPartitionZeroCount(t *testing.T) {
	if p := wire.Partition([]byte("k"), 0); p != 0 {
		t.Errorf("count 0 gave %d", p)
	}
	if p := wire.Partition([]byte{}, 7); p != 0 {
		t.Errorf("empty key gave %d", p)
	}
}

// Seeds every fuzz target with the golden frames.
func seed(f *testing.F) {
	for _, fx := range fixtures(f) {
		h, body, err := wire.SplitFrame(fx.bin)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(h.Type, body)
	}
}

// Decoding any input never panics; whatever decodes re-encodes to the same
// bytes (the encoding is canonical).
func FuzzDecodeRequest(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, typ uint16, body []byte) {
		req, err := wire.DecodeRequest(typ, body)
		if err != nil {
			return
		}
		b, err := wire.AppendRequest(nil, 0, req)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b[wire.HeaderSize:], body) {
			t.Fatalf("re-encoding differs: %x vs %x", b[wire.HeaderSize:], body)
		}
	})
}

func FuzzDecodeResponse(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, typ uint16, body []byte) {
		resp, err := wire.DecodeResponse(typ, body)
		if err != nil {
			return
		}
		b, err := wire.AppendResponse(nil, 0, resp)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b[wire.HeaderSize:], body) {
			t.Fatalf("re-encoding differs: %x vs %x", b[wire.HeaderSize:], body)
		}
	})
}

// A whole frame of any bytes: header parsing and decoding never panic.
func FuzzFrame(f *testing.F) {
	for _, fx := range fixtures(f) {
		f.Add(fx.bin)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		h, body, err := wire.SplitFrame(frame)
		if err != nil {
			return
		}
		_, _ = wire.DecodeRequest(h.Type, body)
		_, _ = wire.DecodeResponse(h.Type, body)
	})
}
