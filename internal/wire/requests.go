package wire

import "fmt"

// Hello opens a connection (section 4.1).
type Hello struct {
	MinVersion    uint16
	MaxVersion    uint16
	ClientName    string
	AuthMechanism string // version 0: "none"
	AuthPayload   []byte // reserved: empty for "none"
}

// Ping is allowed before Hello (section 4.2).
type Ping struct{ Nonce uint64 }

// Metadata asks for topics' partitions and leaders; no topics means every
// topic (section 4.3).
type Metadata struct{ Topics []string }

// CreateTopic (section 4.4).
type CreateTopic struct {
	Name              string
	Partitions        uint32 // version 0: 1
	ReplicationFactor uint16 // reserved: version 0 accepts 1
	RetentionMs       uint64 // 0 keeps everything
}

// DeleteTopic (section 4.5); version 0 brokers answer Unsupported.
type DeleteTopic struct{ Name string }

// Produce (section 4.7).
type Produce struct {
	Topic               string
	Partition           uint32
	ProducerID          uint64 // reserved: not checked by version 0
	ProducerSeq         uint64 // reserved: not checked by version 0
	AckLevel            uint8
	ExpectedLeaderEpoch uint32 // 0: any
	TimeoutMs           uint32 // reserved: ignored by version 0
	Messages            []Message
}

// Fetch (section 4.8).
type Fetch struct {
	Topic               string
	Partition           uint32
	FromSeq             uint64
	MaxBytes            uint32
	MaxWaitMs           uint32
	ExpectedLeaderEpoch uint32
}

// ListTopics has an empty body (section 4.9).
type ListTopics struct{}

// DescribeTopic (section 4.10).
type DescribeTopic struct{ Name string }

// BrokerStats has an empty body (section 4.11).
type BrokerStats struct{}

func (*Hello) Type() uint16         { return TypeHello }
func (*Ping) Type() uint16          { return TypePing }
func (*Metadata) Type() uint16      { return TypeMetadata }
func (*CreateTopic) Type() uint16   { return TypeCreateTopic }
func (*DeleteTopic) Type() uint16   { return TypeDeleteTopic }
func (*Produce) Type() uint16       { return TypeProduce }
func (*Fetch) Type() uint16         { return TypeFetch }
func (*ListTopics) Type() uint16    { return TypeListTopics }
func (*DescribeTopic) Type() uint16 { return TypeDescribeTopic }
func (*BrokerStats) Type() uint16   { return TypeBrokerStats }

func (*Hello) isRequest()         {}
func (*Ping) isRequest()          {}
func (*Metadata) isRequest()      {}
func (*CreateTopic) isRequest()   {}
func (*DeleteTopic) isRequest()   {}
func (*Produce) isRequest()       {}
func (*Fetch) isRequest()         {}
func (*ListTopics) isRequest()    {}
func (*DescribeTopic) isRequest() {}
func (*BrokerStats) isRequest()   {}

func (m *Hello) encode(w *writer) {
	w.u16(m.MinVersion)
	w.u16(m.MaxVersion)
	w.string("client_name", m.ClientName)
	w.string("auth_mechanism", m.AuthMechanism)
	w.bytes("auth_payload", m.AuthPayload)
}

func (m *Ping) encode(w *writer) { w.u64(m.Nonce) }

func (m *Metadata) encode(w *writer) {
	w.count("topics", len(m.Topics))
	for _, t := range m.Topics {
		w.string("topic", t)
	}
}

func (m *CreateTopic) encode(w *writer) {
	w.string("name", m.Name)
	w.u32(m.Partitions)
	w.u16(m.ReplicationFactor)
	w.u64(m.RetentionMs)
}

func (m *DeleteTopic) encode(w *writer)   { w.string("name", m.Name) }
func (m *DescribeTopic) encode(w *writer) { w.string("name", m.Name) }
func (*ListTopics) encode(*writer)        {}
func (*BrokerStats) encode(*writer)       {}

func (m *Produce) encode(w *writer) {
	w.string("topic", m.Topic)
	w.u32(m.Partition)
	w.u64(m.ProducerID)
	w.u64(m.ProducerSeq)
	w.u8(m.AckLevel)
	w.u32(m.ExpectedLeaderEpoch)
	w.u32(m.TimeoutMs)
	w.count("messages", len(m.Messages))
	for i := range m.Messages {
		m.Messages[i].encode(w)
	}
}

func (m *Fetch) encode(w *writer) {
	w.string("topic", m.Topic)
	w.u32(m.Partition)
	w.u64(m.FromSeq)
	w.u32(m.MaxBytes)
	w.u32(m.MaxWaitMs)
	w.u32(m.ExpectedLeaderEpoch)
}

// DecodeRequest decodes a request body of the given message type.
func DecodeRequest(msgType uint16, body []byte) (Request, error) {
	r := &reader{b: body}
	var req Request
	switch msgType {
	case TypeHello:
		req = &Hello{
			MinVersion:    r.u16(),
			MaxVersion:    r.u16(),
			ClientName:    r.string(),
			AuthMechanism: r.string(),
			AuthPayload:   r.bytes(),
		}
	case TypePing:
		req = &Ping{Nonce: r.u64()}
	case TypeMetadata:
		n := r.count(2)
		topics := make([]string, 0, n)
		for range n {
			topics = append(topics, r.string())
		}
		req = &Metadata{Topics: topics}
	case TypeCreateTopic:
		req = &CreateTopic{
			Name:              r.string(),
			Partitions:        r.u32(),
			ReplicationFactor: r.u16(),
			RetentionMs:       r.u64(),
		}
	case TypeDeleteTopic:
		req = &DeleteTopic{Name: r.string()}
	case TypeProduce:
		p := &Produce{
			Topic:               r.string(),
			Partition:           r.u32(),
			ProducerID:          r.u64(),
			ProducerSeq:         r.u64(),
			AckLevel:            r.u8(),
			ExpectedLeaderEpoch: r.u32(),
			TimeoutMs:           r.u32(),
		}
		n := r.count(minMessage)
		p.Messages = make([]Message, 0, n)
		for range n {
			p.Messages = append(p.Messages, decodeMessage(r))
			if r.err != nil {
				break
			}
		}
		req = p
	case TypeFetch:
		req = &Fetch{
			Topic:               r.string(),
			Partition:           r.u32(),
			FromSeq:             r.u64(),
			MaxBytes:            r.u32(),
			MaxWaitMs:           r.u32(),
			ExpectedLeaderEpoch: r.u32(),
		}
	case TypeListTopics:
		req = &ListTopics{}
	case TypeDescribeTopic:
		req = &DescribeTopic{Name: r.string()}
	case TypeBrokerStats:
		req = &BrokerStats{}
	default:
		return nil, fmt.Errorf("%w: %#06x", ErrUnknownType, msgType)
	}
	if err := r.finish(); err != nil {
		return nil, err
	}
	return req, nil
}
