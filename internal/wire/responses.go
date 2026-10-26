package wire

import (
	"fmt"

	"github.com/straum-io/straum-go-sdk/straumerr"
)

// HelloOk fixes the connection's protocol version (section 4.1).
type HelloOk struct {
	Version         uint16
	ClusterID       string
	NodeID          uint32
	MaxFrameBytes   uint32
	MaxMessageBytes uint32
}

// Pong answers Ping with its nonce.
type Pong struct{ Nonce uint64 }

// Node is a broker of the cluster.
type Node struct {
	NodeID uint32
	Host   string
	Port   uint16
	Rack   string // reserved: empty
}

// PartitionMetadata describes one partition's leader and seqs.
type PartitionMetadata struct {
	Partition      uint32
	LeaderNodeID   uint32
	LeaderEpoch    uint32
	Replicas       []uint32
	InSyncReplicas []uint32 // reserved: version 0 sends Replicas
	StartSeq       uint64   // first seq still readable
	NextSeq        uint64   // seq of the next message
}

// TopicMetadata is one topic of MetadataOk. Error is 0 or
// straumerr.UnknownTopic (with no partitions).
type TopicMetadata struct {
	Name       string
	Error      straumerr.Code
	Partitions []PartitionMetadata
}

// MetadataOk (section 4.3).
type MetadataOk struct {
	ClusterID        string
	ControllerNodeID uint32 // reserved: version 0 sends its own node id
	Nodes            []Node
	Topics           []TopicMetadata
}

// CreateTopicOk has an empty body.
type CreateTopicOk struct{}

// DeleteTopicOk has an empty body.
type DeleteTopicOk struct{}

// ProduceOk acknowledges a Produce: its messages have seqs
// BaseSeq … BaseSeq+Count−1.
type ProduceOk struct {
	BaseSeq     uint64
	Count       uint32
	LeaderEpoch uint32
	TimestampMs uint64 // the broker's time for the first message
}

// FetchedMessage is a message with its seq and broker timestamp.
type FetchedMessage struct {
	Seq         uint64
	TimestampMs uint64
	Message
}

// FetchOk (section 4.8).
type FetchOk struct {
	LeaderEpoch uint32
	StartSeq    uint64
	NextSeq     uint64
	Messages    []FetchedMessage
}

// TopicSummary is one entry of ListTopicsOk.
type TopicSummary struct {
	Name       string
	Partitions uint32
}

// ListTopicsOk is one page of topics by name. NextCursor is empty after
// the last page.
type ListTopicsOk struct {
	Topics     []TopicSummary
	NextCursor string
}

// PartitionDescription is one partition of DescribeTopicOk.
type PartitionDescription struct {
	Partition    uint32
	StartSeq     uint64
	NextSeq      uint64
	MessageCount uint64
	IndexBytes   uint64
}

// DescribeTopicOk (section 4.10).
type DescribeTopicOk struct {
	Name              string
	ReplicationFactor uint16
	RetentionMs       uint64
	Partitions        []PartitionDescription
}

// BrokerStatsOk holds counters since the broker started (section 4.11).
type BrokerStatsOk struct {
	NodeID               uint32
	OpenConnections      uint32
	PendingBytes         uint64
	PendingBytesPeak     uint64
	MaxPendingBytes      uint64
	FetchWaiters         uint32
	ProduceRequests      uint64
	GroupsCommitted      uint64
	Fsyncs               uint64
	MessagesAccepted     uint64
	MessagesDeduplicated uint64
	BodyBytesReceived    uint64
	BodyBytesNew         uint64
	WalBytes             uint64
	SegmentBytes         uint64
	IndexBytes           uint64
}

// Error is the response to any request that failed (section 5). Code is
// kept as received, including codes this version does not define.
type Error struct {
	Code         straumerr.Code
	Message      string
	LeaderNodeID uint32
	LeaderEpoch  uint32
	RetryAfterMs uint32
	MinVersion   uint16
	MaxVersion   uint16
}

func (*HelloOk) Type() uint16         { return TypeHello | ResponseBit }
func (*Pong) Type() uint16            { return TypePing | ResponseBit }
func (*MetadataOk) Type() uint16      { return TypeMetadata | ResponseBit }
func (*CreateTopicOk) Type() uint16   { return TypeCreateTopic | ResponseBit }
func (*DeleteTopicOk) Type() uint16   { return TypeDeleteTopic | ResponseBit }
func (*ProduceOk) Type() uint16       { return TypeProduce | ResponseBit }
func (*FetchOk) Type() uint16         { return TypeFetch | ResponseBit }
func (*ListTopicsOk) Type() uint16    { return TypeListTopics | ResponseBit }
func (*DescribeTopicOk) Type() uint16 { return TypeDescribeTopic | ResponseBit }
func (*BrokerStatsOk) Type() uint16   { return TypeBrokerStats | ResponseBit }
func (*Error) Type() uint16           { return TypeError }

func (*HelloOk) isResponse()         {}
func (*Pong) isResponse()            {}
func (*MetadataOk) isResponse()      {}
func (*CreateTopicOk) isResponse()   {}
func (*DeleteTopicOk) isResponse()   {}
func (*ProduceOk) isResponse()       {}
func (*FetchOk) isResponse()         {}
func (*ListTopicsOk) isResponse()    {}
func (*DescribeTopicOk) isResponse() {}
func (*BrokerStatsOk) isResponse()   {}
func (*Error) isResponse()           {}

func (m *HelloOk) encode(w *writer) {
	w.u16(m.Version)
	w.string("cluster_id", m.ClusterID)
	w.u32(m.NodeID)
	w.u32(m.MaxFrameBytes)
	w.u32(m.MaxMessageBytes)
}

func (m *Pong) encode(w *writer)      { w.u64(m.Nonce) }
func (*CreateTopicOk) encode(*writer) {}
func (*DeleteTopicOk) encode(*writer) {}

func (m *MetadataOk) encode(w *writer) {
	w.string("cluster_id", m.ClusterID)
	w.u32(m.ControllerNodeID)
	w.count("nodes", len(m.Nodes))
	for _, n := range m.Nodes {
		w.u32(n.NodeID)
		w.string("host", n.Host)
		w.u16(n.Port)
		w.string("rack", n.Rack)
	}
	w.count("topics", len(m.Topics))
	for _, t := range m.Topics {
		w.string("topic", t.Name)
		w.u16(uint16(t.Error))
		w.count("partitions", len(t.Partitions))
		for _, p := range t.Partitions {
			w.u32(p.Partition)
			w.u32(p.LeaderNodeID)
			w.u32(p.LeaderEpoch)
			putU32s(w, "replicas", p.Replicas)
			putU32s(w, "in_sync_replicas", p.InSyncReplicas)
			w.u64(p.StartSeq)
			w.u64(p.NextSeq)
		}
	}
}

func putU32s(w *writer, what string, v []uint32) {
	w.count(what, len(v))
	for _, x := range v {
		w.u32(x)
	}
}

func readU32s(r *reader) []uint32 {
	n := r.count(4)
	v := make([]uint32, 0, n)
	for range n {
		v = append(v, r.u32())
	}
	return v
}

func (m *ProduceOk) encode(w *writer) {
	w.u64(m.BaseSeq)
	w.u32(m.Count)
	w.u32(m.LeaderEpoch)
	w.u64(m.TimestampMs)
}

func (m *FetchOk) encode(w *writer) {
	w.u32(m.LeaderEpoch)
	w.u64(m.StartSeq)
	w.u64(m.NextSeq)
	w.count("messages", len(m.Messages))
	for i := range m.Messages {
		w.u64(m.Messages[i].Seq)
		w.u64(m.Messages[i].TimestampMs)
		m.Messages[i].encode(w)
	}
}

func (m *ListTopicsOk) encode(w *writer) {
	w.count("topics", len(m.Topics))
	for _, t := range m.Topics {
		w.string("name", t.Name)
		w.u32(t.Partitions)
	}
	w.string("next_cursor", m.NextCursor)
}

func (m *DescribeTopicOk) encode(w *writer) {
	w.string("name", m.Name)
	w.u16(m.ReplicationFactor)
	w.u64(m.RetentionMs)
	w.count("partitions", len(m.Partitions))
	for _, p := range m.Partitions {
		w.u32(p.Partition)
		w.u64(p.StartSeq)
		w.u64(p.NextSeq)
		w.u64(p.MessageCount)
		w.u64(p.IndexBytes)
	}
}

func (m *BrokerStatsOk) encode(w *writer) {
	w.u32(m.NodeID)
	w.u32(m.OpenConnections)
	w.u64(m.PendingBytes)
	w.u64(m.PendingBytesPeak)
	w.u64(m.MaxPendingBytes)
	w.u32(m.FetchWaiters)
	for _, v := range []uint64{
		m.ProduceRequests, m.GroupsCommitted, m.Fsyncs, m.MessagesAccepted,
		m.MessagesDeduplicated, m.BodyBytesReceived, m.BodyBytesNew,
		m.WalBytes, m.SegmentBytes, m.IndexBytes,
	} {
		w.u64(v)
	}
}

func (m *Error) encode(w *writer) {
	w.u16(uint16(m.Code))
	w.string("message", m.Message)
	w.u32(m.LeaderNodeID)
	w.u32(m.LeaderEpoch)
	w.u32(m.RetryAfterMs)
	w.u16(m.MinVersion)
	w.u16(m.MaxVersion)
}

// DecodeResponse decodes a response body of the given message type.
func DecodeResponse(msgType uint16, body []byte) (Response, error) {
	r := &reader{b: body}
	var resp Response
	switch msgType {
	case TypeError:
		resp = &Error{
			Code:         straumerr.Code(r.u16()),
			Message:      r.string(),
			LeaderNodeID: r.u32(),
			LeaderEpoch:  r.u32(),
			RetryAfterMs: r.u32(),
			MinVersion:   r.u16(),
			MaxVersion:   r.u16(),
		}
	case TypeHello | ResponseBit:
		resp = &HelloOk{
			Version:         r.u16(),
			ClusterID:       r.string(),
			NodeID:          r.u32(),
			MaxFrameBytes:   r.u32(),
			MaxMessageBytes: r.u32(),
		}
	case TypePing | ResponseBit:
		resp = &Pong{Nonce: r.u64()}
	case TypeMetadata | ResponseBit:
		resp = decodeMetadataOk(r)
	case TypeCreateTopic | ResponseBit:
		resp = &CreateTopicOk{}
	case TypeDeleteTopic | ResponseBit:
		resp = &DeleteTopicOk{}
	case TypeProduce | ResponseBit:
		resp = &ProduceOk{
			BaseSeq:     r.u64(),
			Count:       r.u32(),
			LeaderEpoch: r.u32(),
			TimestampMs: r.u64(),
		}
	case TypeFetch | ResponseBit:
		f := &FetchOk{LeaderEpoch: r.u32(), StartSeq: r.u64(), NextSeq: r.u64()}
		n := r.count(16 + minMessage)
		f.Messages = make([]FetchedMessage, 0, n)
		for range n {
			m := FetchedMessage{Seq: r.u64(), TimestampMs: r.u64()}
			m.Message = decodeMessage(r)
			if r.err != nil {
				break
			}
			f.Messages = append(f.Messages, m)
		}
		resp = f
	case TypeListTopics | ResponseBit:
		n := r.count(2 + 4)
		l := &ListTopicsOk{Topics: make([]TopicSummary, 0, n)}
		for range n {
			l.Topics = append(l.Topics, TopicSummary{Name: r.string(), Partitions: r.u32()})
		}
		l.NextCursor = r.string()
		resp = l
	case TypeDescribeTopic | ResponseBit:
		d := &DescribeTopicOk{Name: r.string(), ReplicationFactor: r.u16(), RetentionMs: r.u64()}
		n := r.count(4 + 32)
		d.Partitions = make([]PartitionDescription, 0, n)
		for range n {
			d.Partitions = append(d.Partitions, PartitionDescription{
				Partition:    r.u32(),
				StartSeq:     r.u64(),
				NextSeq:      r.u64(),
				MessageCount: r.u64(),
				IndexBytes:   r.u64(),
			})
		}
		resp = d
	case TypeBrokerStats | ResponseBit:
		s := &BrokerStatsOk{
			NodeID:          r.u32(),
			OpenConnections: r.u32(),
			PendingBytes:    r.u64(),
		}
		s.PendingBytesPeak = r.u64()
		s.MaxPendingBytes = r.u64()
		s.FetchWaiters = r.u32()
		for _, p := range []*uint64{
			&s.ProduceRequests, &s.GroupsCommitted, &s.Fsyncs, &s.MessagesAccepted,
			&s.MessagesDeduplicated, &s.BodyBytesReceived, &s.BodyBytesNew,
			&s.WalBytes, &s.SegmentBytes, &s.IndexBytes,
		} {
			*p = r.u64()
		}
		resp = s
	default:
		return nil, fmt.Errorf("%w: %#06x", ErrUnknownType, msgType)
	}
	if err := r.finish(); err != nil {
		return nil, err
	}
	return resp, nil
}

func decodeMetadataOk(r *reader) *MetadataOk {
	m := &MetadataOk{ClusterID: r.string(), ControllerNodeID: r.u32()}
	n := r.count(4 + 2 + 2 + 2)
	m.Nodes = make([]Node, 0, n)
	for range n {
		m.Nodes = append(m.Nodes, Node{NodeID: r.u32(), Host: r.string(), Port: r.u16(), Rack: r.string()})
	}
	n = r.count(2 + 2 + 4)
	m.Topics = make([]TopicMetadata, 0, n)
	for range n {
		t := TopicMetadata{Name: r.string(), Error: straumerr.Code(r.u16())}
		np := r.count(4*5 + 16)
		t.Partitions = make([]PartitionMetadata, 0, np)
		for range np {
			t.Partitions = append(t.Partitions, PartitionMetadata{
				Partition:      r.u32(),
				LeaderNodeID:   r.u32(),
				LeaderEpoch:    r.u32(),
				Replicas:       readU32s(r),
				InSyncReplicas: readU32s(r),
				StartSeq:       r.u64(),
				NextSeq:        r.u64(),
			})
		}
		m.Topics = append(m.Topics, t)
	}
	return m
}
