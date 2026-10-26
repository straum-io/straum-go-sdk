package wire_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/straum-io/straum-go-sdk/internal/conformance"
	"github.com/straum-io/straum-go-sdk/internal/wire"
)

const suite = "../../testdata/conformance"

type fixture struct {
	name string
	bin  []byte
	desc conformance.Obj
}

func fixtures(t testing.TB) []fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(suite, "frames", "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var index struct{ Frames []string }
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	var out []fixture
	for _, name := range index.Frames {
		bin, err := os.ReadFile(filepath.Join(suite, "frames", name+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(suite, "frames", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		v, err := conformance.Parse(raw)
		if err != nil {
			t.Fatalf("%s.json: %v", name, err)
		}
		out = append(out, fixture{name, bin, v.(conformance.Obj)})
	}
	return out
}

func canonical(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Every golden frame decodes to the fields of its description, and
// encoding the description gives exactly the frame.
func TestGoldenFrames(t *testing.T) {
	all := fixtures(t)
	if len(all) != 59 {
		t.Fatalf("index lists %d fixtures, want 59 (testdata/conformance/SOURCE)", len(all))
	}
	for _, f := range all {
		t.Run(f.name, func(t *testing.T) {
			h, body, err := wire.SplitFrame(f.bin)
			if err != nil {
				t.Fatal(err)
			}
			corr, _ := strconv.ParseUint(f.desc["correlation_id"].(string), 10, 64)
			typeCode, _ := strconv.Atoi(string(f.desc["type_code"].(json.Number)))
			size, _ := strconv.Atoi(string(f.desc["frame_bytes"].(json.Number)))
			if h.CorrelationID != corr || int(h.Type) != typeCode || len(f.bin) != size {
				t.Fatalf("header %+v, %d bytes; want type %d, correlation %d, %d bytes", h, len(f.bin), typeCode, corr, size)
			}
			msg := f.desc["message"]
			var decoded conformance.Obj
			var encoded []byte
			if f.desc["direction"] == "request" {
				req, err := wire.DecodeRequest(h.Type, body)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				decoded = conformance.RequestJSON(req)
				want, err := conformance.RequestFromJSON(msg)
				if err != nil {
					t.Fatal(err)
				}
				if encoded, err = wire.AppendRequest(nil, corr, want); err != nil {
					t.Fatal(err)
				}
			} else {
				resp, err := wire.DecodeResponse(h.Type, body)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				decoded = conformance.ResponseJSON(resp)
				want, err := conformance.ResponseFromJSON(msg)
				if err != nil {
					t.Fatal(err)
				}
				if encoded, err = wire.AppendResponse(nil, corr, want); err != nil {
					t.Fatal(err)
				}
			}
			if got, want := canonical(t, decoded), canonical(t, msg); got != want {
				t.Errorf("decoded fields differ:\n got %s\nwant %s", got, want)
			}
			if !bytes.Equal(encoded, f.bin) {
				t.Errorf("encoding differs:\n got %s\nwant %s", hex.EncodeToString(encoded), hex.EncodeToString(f.bin))
			}
		})
	}
}

// The default partitioner gives the vector's partition for every key and
// partition count from 1 to 16.
func TestPartitionerVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(suite, "partitioner", "vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Note       string
			Key        string
			Crc32c     uint32
			Partitions []uint32
		}
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	checked := 0
	for _, v := range file.Vectors {
		key, err := hex.DecodeString(v.Key)
		if err != nil {
			t.Fatal(err)
		}
		if got := crc32.Checksum(key, crc32.MakeTable(crc32.Castagnoli)); got != v.Crc32c {
			t.Errorf("%s: crc32c = %d, want %d", v.Note, got, v.Crc32c)
		}
		for i, want := range v.Partitions {
			if got := wire.Partition(key, uint32(i+1)); got != want {
				t.Errorf("%s: partition(count %d) = %d, want %d", v.Note, i+1, got, want)
			}
			checked++
		}
	}
	t.Logf("%d vectors, %d partition checks", len(file.Vectors), checked)
}
