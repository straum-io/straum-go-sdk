package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

// Decoding errors. Every error returned by a Decode function wraps one of
// them.
var (
	// ErrTruncated: the body ends before a field, or a length or count
	// exceeds the bytes left.
	ErrTruncated = errors.New("wire: body ends early")
	// ErrInvalidUTF8: a string is not UTF-8.
	ErrInvalidUTF8 = errors.New("wire: string is not UTF-8")
	// ErrTrailingBytes: bytes follow the last field.
	ErrTrailingBytes = errors.New("wire: bytes after the last field")
	// ErrInvalidValue: a field holds a value this version does not define.
	ErrInvalidValue = errors.New("wire: invalid value")
	// ErrUnknownType: the message type is not one of this version.
	ErrUnknownType = errors.New("wire: unknown message type")
)

// ErrTooLong is returned by the encoders when a value does not fit its
// length or count field (a string over 65535 bytes, a key over 65535
// bytes, more than 65535 headers, a frame over 4 GiB).
var ErrTooLong = errors.New("wire: value too long for its length field")

// reader decodes little-endian fields from a body. The first error sticks:
// later reads return zero values, and err reports the first failure.
//
// ext selects the response rules of protocol version 1 (Straum
// docs/protocol.md section 10.2 in the draft of straum-io/straum#129, not
// yet at the CONTRACT commit): an array element of struct type starts
// with its length, and bytes after the fields a decoder knows, at the end
// of a body or of an element, are skipped. Version 0 decodes with ext
// false and rejects such bytes.
type reader struct {
	b   []byte
	err error
	ext bool
}

func (r *reader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
	r.b = nil
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n > len(r.b) {
		r.fail(ErrTruncated)
		return nil
	}
	// The full slice expression keeps an append by the caller from
	// overwriting the next field.
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// shortBytes reads `len u16, bytes`. The result is never nil when the read
// succeeds, so a present empty key stays distinct from an absent one.
func (r *reader) shortBytes() []byte {
	n := int(r.u16())
	b := r.take(n)
	if b == nil && r.err == nil {
		b = []byte{}
	}
	return b
}

// bytes reads `len u32, bytes`; never nil on success.
func (r *reader) bytes() []byte {
	n := r.u32()
	if uint64(n) > uint64(len(r.b)) {
		r.fail(ErrTruncated)
		return nil
	}
	b := r.take(int(n))
	if b == nil && r.err == nil {
		b = []byte{}
	}
	return b
}

func (r *reader) string() string {
	b := r.shortBytes()
	if r.err != nil {
		return ""
	}
	if !utf8.Valid(b) {
		r.fail(ErrInvalidUTF8)
		return ""
	}
	return string(b)
}

// count reads an array count and checks that count elements of at least
// minSize bytes each fit in what is left, so a count is never trusted to
// allocate.
func (r *reader) count(minSize int) int {
	n := r.u32()
	if r.err != nil {
		return 0
	}
	if uint64(n)*uint64(max(minSize, 1)) > uint64(len(r.b)) {
		r.fail(ErrTruncated)
		return 0
	}
	return int(n)
}

// uvarint reads a length of version 1: 7 bits per byte, low group first,
// the high bit set on every byte but the last; at most 5 bytes, in its
// shortest form, at most 2^32−1.
func (r *reader) uvarint() uint32 {
	if r.err != nil {
		return 0
	}
	var v uint64
	for i := range 5 {
		if i == len(r.b) {
			r.fail(ErrTruncated)
			return 0
		}
		c := r.b[i]
		v |= uint64(c&0x7f) << (7 * i)
		if c&0x80 == 0 {
			switch {
			case i > 0 && c == 0:
				r.invalid("varint not in its shortest form")
			case v > math.MaxUint32:
				r.invalid("varint above 2^32-1")
			default:
				r.b = r.b[i+1:]
				return uint32(v)
			}
			return 0
		}
	}
	r.invalid("varint longer than 5 bytes")
	return 0
}

// item decodes one array element with fn. Without ext, fn reads the
// element in place. With ext, the element starts with its length as a
// uvarint; fn reads only within that length, a field cut by it is
// ErrTruncated, and the bytes fn leaves are skipped.
func (r *reader) item(fn func()) {
	if !r.ext {
		fn()
		return
	}
	n := r.uvarint()
	if r.err != nil {
		return
	}
	if uint64(n) > uint64(len(r.b)) {
		r.fail(ErrTruncated)
		return
	}
	rest := r.b[n:]
	r.b = r.b[:n:n]
	fn()
	if r.err == nil {
		r.b = rest
	}
}

func (r *reader) invalid(what string) {
	r.fail(fmt.Errorf("%w: %s", ErrInvalidValue, what))
}

// finish returns the first error, or ErrTrailingBytes if bytes are left
// and ext is false.
func (r *reader) finish() error {
	if r.err != nil {
		return r.err
	}
	if len(r.b) != 0 && !r.ext {
		return ErrTrailingBytes
	}
	return nil
}

// writer appends fields; the first value that does not fit its length
// field sticks as err.
type writer struct {
	b   []byte
	err error
}

func (w *writer) u8(v uint8)   { w.b = append(w.b, v) }
func (w *writer) u16(v uint16) { w.b = binary.LittleEndian.AppendUint16(w.b, v) }
func (w *writer) u32(v uint32) { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *writer) u64(v uint64) { w.b = binary.LittleEndian.AppendUint64(w.b, v) }

func (w *writer) tooLong(what string, n int) {
	if w.err == nil {
		w.err = fmt.Errorf("%w: %s has %d", ErrTooLong, what, n)
	}
}

func (w *writer) shortBytes(what string, v []byte) {
	if len(v) > math.MaxUint16 {
		w.tooLong(what, len(v))
	}
	w.u16(uint16(len(v)))
	w.b = append(w.b, v...)
}

func (w *writer) bytes(what string, v []byte) {
	if uint64(len(v)) > math.MaxUint32 {
		w.tooLong(what, len(v))
	}
	w.u32(uint32(len(v)))
	w.b = append(w.b, v...)
}

func (w *writer) string(what, v string) {
	if len(v) > math.MaxUint16 {
		w.tooLong(what, len(v))
	}
	w.u16(uint16(len(v)))
	w.b = append(w.b, v...)
}

func (w *writer) count(what string, n int) {
	if uint64(n) > math.MaxUint32 {
		w.tooLong(what, n)
	}
	w.u32(uint32(n))
}
