package client

import (
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Header is one message header. Names need not be unique; order is kept.
type Header struct {
	Name  []byte
	Value []byte
}

// Message is the unit of produce and fetch (docs/protocol.md section 4.6).
// A nil Key is an absent key; a non-nil empty Key is a present, empty one.
type Message struct {
	Key     []byte
	Headers []Header
	Body    []byte
}

// BrokerInfo is what a broker reports in its HelloOk.
type BrokerInfo struct {
	Version         uint16
	ClusterID       string
	NodeID          uint32
	MaxFrameBytes   uint32
	MaxMessageBytes uint32
}

// Node is a broker of the cluster.
type Node struct {
	NodeID uint32
	Host   string
	Port   uint16
	Rack   string // reserved: empty
}

// PartitionMetadata is a partition's leader and seq range.
type PartitionMetadata struct {
	Partition      uint32
	LeaderNodeID   uint32
	LeaderEpoch    uint32
	Replicas       []uint32
	InSyncReplicas []uint32 // reserved: version 0 sends Replicas
	StartSeq       uint64   // first seq still readable
	NextSeq        uint64   // seq of the next message
}

// TopicMetadata is a topic's partitions, or the Error that kept the broker
// from listing them (straumerr.UnknownTopic).
type TopicMetadata struct {
	Name       string
	Error      straumerr.Code
	Partitions []PartitionMetadata
}

// Metadata is the cluster's nodes and the requested topics.
type Metadata struct {
	ClusterID        string
	ControllerNodeID uint32 // reserved: version 0 sends its own node id
	Nodes            []Node
	Topics           []TopicMetadata
}

func newMetadata(m *wire.MetadataOk) *Metadata {
	out := &Metadata{ClusterID: m.ClusterID, ControllerNodeID: m.ControllerNodeID,
		Nodes: make([]Node, len(m.Nodes)), Topics: make([]TopicMetadata, len(m.Topics))}
	for i, n := range m.Nodes {
		out.Nodes[i] = Node(n)
	}
	for i, t := range m.Topics {
		ps := make([]PartitionMetadata, len(t.Partitions))
		for j, p := range t.Partitions {
			ps[j] = PartitionMetadata(p)
		}
		out.Topics[i] = TopicMetadata{Name: t.Name, Error: t.Error, Partitions: ps}
	}
	return out
}
