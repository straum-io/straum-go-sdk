// Package conformance maps protocol messages to and from the JSON of
// Straum's conformance suite (conformance/README.md) and matches expected
// values. It is used by the conformance tests and runners only.
//
// JSON conventions: u8, u16 and u32 are numbers; u64 values are decimal
// strings; bytes are lowercase hex; an absent key is null; error codes are
// names, or numbers for codes version 0 does not define.
package conformance

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Obj is a JSON object as decoded by Parse.
type Obj = map[string]any

// Parse decodes JSON keeping numbers as json.Number.
func Parse(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func num[T uint8 | uint16 | uint32](v T) json.Number {
	return json.Number(strconv.FormatUint(uint64(v), 10))
}
func big(v uint64) string { return strconv.FormatUint(v, 10) }
func hx(b []byte) string  { return hex.EncodeToString(b) }

func code(c straumerr.Code) any {
	if c.Known() {
		return c.String()
	}
	return num(uint16(c))
}

func messageJSON(m *wire.Message) Obj {
	var key any
	if m.Key != nil {
		key = hx(m.Key)
	}
	headers := []any{}
	for _, h := range m.Headers {
		headers = append(headers, Obj{"name": hx(h.Name), "value": hx(h.Value)})
	}
	return Obj{"key": key, "headers": headers, "body": hx(m.Body)}
}

func u32s(v []uint32) []any {
	out := []any{}
	for _, x := range v {
		out = append(out, num(x))
	}
	return out
}

// RequestJSON returns {"type": name, "body": {...}}.
func RequestJSON(req wire.Request) Obj {
	var name string
	var body Obj
	switch r := req.(type) {
	case *wire.Hello:
		name, body = "Hello", Obj{"min_version": num(r.MinVersion), "max_version": num(r.MaxVersion),
			"client_name": r.ClientName, "auth_mechanism": r.AuthMechanism, "auth_payload": hx(r.AuthPayload)}
	case *wire.Ping:
		name, body = "Ping", Obj{"nonce": big(r.Nonce)}
	case *wire.Metadata:
		topics := []any{}
		for _, t := range r.Topics {
			topics = append(topics, t)
		}
		name, body = "Metadata", Obj{"topics": topics}
	case *wire.CreateTopic:
		name, body = "CreateTopic", Obj{"name": r.Name, "partitions": num(r.Partitions),
			"replication_factor": num(r.ReplicationFactor), "retention_ms": big(r.RetentionMs)}
	case *wire.DeleteTopic:
		name, body = "DeleteTopic", Obj{"name": r.Name}
	case *wire.Produce:
		msgs := []any{}
		for i := range r.Messages {
			msgs = append(msgs, messageJSON(&r.Messages[i]))
		}
		name, body = "Produce", Obj{"topic": r.Topic, "partition": num(r.Partition),
			"producer_id": big(r.ProducerID), "producer_seq": big(r.ProducerSeq), "ack_level": num(r.AckLevel),
			"expected_leader_epoch": num(r.ExpectedLeaderEpoch), "timeout_ms": num(r.TimeoutMs), "messages": msgs}
	case *wire.Fetch:
		name, body = "Fetch", Obj{"topic": r.Topic, "partition": num(r.Partition), "from_seq": big(r.FromSeq),
			"max_bytes": num(r.MaxBytes), "max_wait_ms": num(r.MaxWaitMs), "expected_leader_epoch": num(r.ExpectedLeaderEpoch)}
	case *wire.ListTopics:
		name, body = "ListTopics", Obj{}
	case *wire.DescribeTopic:
		name, body = "DescribeTopic", Obj{"name": r.Name}
	case *wire.BrokerStats:
		name, body = "BrokerStats", Obj{}
	}
	return Obj{"type": name, "body": body}
}

// ResponseJSON returns {"type": name, "body": {...}}.
func ResponseJSON(resp wire.Response) Obj {
	var name string
	var body Obj
	switch r := resp.(type) {
	case *wire.HelloOk:
		name, body = "HelloOk", Obj{"version": num(r.Version), "cluster_id": r.ClusterID, "node_id": num(r.NodeID),
			"max_frame_bytes": num(r.MaxFrameBytes), "max_message_bytes": num(r.MaxMessageBytes)}
	case *wire.Pong:
		name, body = "Pong", Obj{"nonce": big(r.Nonce)}
	case *wire.MetadataOk:
		nodes := []any{}
		for _, n := range r.Nodes {
			nodes = append(nodes, Obj{"node_id": num(n.NodeID), "host": n.Host, "port": num(n.Port), "rack": n.Rack})
		}
		topics := []any{}
		for _, t := range r.Topics {
			parts := []any{}
			for _, p := range t.Partitions {
				parts = append(parts, Obj{"partition": num(p.Partition), "leader_node_id": num(p.LeaderNodeID),
					"leader_epoch": num(p.LeaderEpoch), "replicas": u32s(p.Replicas),
					"in_sync_replicas": u32s(p.InSyncReplicas), "start_seq": big(p.StartSeq), "next_seq": big(p.NextSeq)})
			}
			var e any = json.Number("0")
			if t.Error != 0 {
				e = code(t.Error)
			}
			topics = append(topics, Obj{"name": t.Name, "error": e, "partitions": parts})
		}
		name, body = "MetadataOk", Obj{"cluster_id": r.ClusterID, "controller_node_id": num(r.ControllerNodeID),
			"nodes": nodes, "topics": topics}
	case *wire.CreateTopicOk:
		name, body = "CreateTopicOk", Obj{}
	case *wire.DeleteTopicOk:
		name, body = "DeleteTopicOk", Obj{}
	case *wire.ProduceOk:
		name, body = "ProduceOk", Obj{"base_seq": big(r.BaseSeq), "count": num(r.Count),
			"leader_epoch": num(r.LeaderEpoch), "timestamp_ms": big(r.TimestampMs)}
	case *wire.FetchOk:
		msgs := []any{}
		for i := range r.Messages {
			m := messageJSON(&r.Messages[i].Message)
			m["seq"] = big(r.Messages[i].Seq)
			m["timestamp_ms"] = big(r.Messages[i].TimestampMs)
			msgs = append(msgs, m)
		}
		name, body = "FetchOk", Obj{"leader_epoch": num(r.LeaderEpoch), "start_seq": big(r.StartSeq),
			"next_seq": big(r.NextSeq), "messages": msgs}
	case *wire.ListTopicsOk:
		topics := []any{}
		for _, t := range r.Topics {
			topics = append(topics, Obj{"name": t.Name, "partitions": num(t.Partitions)})
		}
		name, body = "ListTopicsOk", Obj{"topics": topics}
	case *wire.DescribeTopicOk:
		parts := []any{}
		for _, p := range r.Partitions {
			parts = append(parts, Obj{"partition": num(p.Partition), "start_seq": big(p.StartSeq),
				"next_seq": big(p.NextSeq), "message_count": big(p.MessageCount), "index_bytes": big(p.IndexBytes)})
		}
		name, body = "DescribeTopicOk", Obj{"name": r.Name, "replication_factor": num(r.ReplicationFactor),
			"retention_ms": big(r.RetentionMs), "partitions": parts}
	case *wire.BrokerStatsOk:
		name, body = "BrokerStatsOk", Obj{"node_id": num(r.NodeID), "open_connections": num(r.OpenConnections),
			"pending_bytes": big(r.PendingBytes), "pending_bytes_peak": big(r.PendingBytesPeak),
			"max_pending_bytes": big(r.MaxPendingBytes), "fetch_waiters": num(r.FetchWaiters),
			"produce_requests": big(r.ProduceRequests), "groups_committed": big(r.GroupsCommitted),
			"fsyncs": big(r.Fsyncs), "messages_accepted": big(r.MessagesAccepted),
			"messages_deduplicated": big(r.MessagesDeduplicated), "body_bytes_received": big(r.BodyBytesReceived),
			"body_bytes_new": big(r.BodyBytesNew), "wal_bytes": big(r.WalBytes),
			"segment_bytes": big(r.SegmentBytes), "index_bytes": big(r.IndexBytes)}
	case *wire.Error:
		name, body = "Error", Obj{"code": code(r.Code), "message": r.Message, "leader_node_id": num(r.LeaderNodeID),
			"leader_epoch": num(r.LeaderEpoch), "retry_after_ms": num(r.RetryAfterMs),
			"min_version": num(r.MinVersion), "max_version": num(r.MaxVersion)}
	}
	return Obj{"type": name, "body": body}
}

// fields reads typed values from a JSON object; the first error sticks.
type fields struct {
	o    Obj
	path string
	err  error
}

func (f *fields) fail(format string, a ...any) {
	if f.err == nil {
		f.err = fmt.Errorf(f.path+": "+format, a...)
	}
}

func (f *fields) get(k string) any {
	v, ok := f.o[k]
	if !ok {
		f.fail("missing field %s", k)
	}
	return v
}

func (f *fields) uint(k string, maxV uint64) uint64 {
	n, ok := f.get(k).(json.Number)
	if !ok {
		f.fail("%s: expected a number", k)
		return 0
	}
	v, err := strconv.ParseUint(string(n), 10, 64)
	if err != nil || v > maxV {
		f.fail("%s: %s out of range", k, n)
	}
	return v
}

func (f *fields) u8(k string) uint8   { return uint8(f.uint(k, math.MaxUint8)) }
func (f *fields) u16(k string) uint16 { return uint16(f.uint(k, math.MaxUint16)) }
func (f *fields) u32(k string) uint32 { return uint32(f.uint(k, math.MaxUint32)) }

func (f *fields) big(k string) uint64 {
	s, ok := f.get(k).(string)
	v, err := strconv.ParseUint(s, 10, 64)
	if !ok || err != nil {
		f.fail("%s: expected a decimal string", k)
	}
	return v
}

func (f *fields) str(k string) string {
	s, ok := f.get(k).(string)
	if !ok {
		f.fail("%s: expected a string", k)
	}
	return s
}

func (f *fields) bytes(k string) []byte {
	s, ok := f.get(k).(string)
	b, err := hex.DecodeString(s)
	if !ok || err != nil {
		f.fail("%s: expected hex", k)
	}
	return b
}

func (f *fields) arr(k string) []any {
	a, ok := f.get(k).([]any)
	if !ok {
		f.fail("%s: expected an array", k)
	}
	return a
}

func (f *fields) obj(v any, path string) *fields {
	o, ok := v.(Obj)
	if !ok {
		f.fail("%s: expected an object", path)
		o = Obj{}
	}
	return &fields{o: o, path: f.path + "." + path}
}

func (f *fields) code(k string) straumerr.Code {
	switch v := f.get(k).(type) {
	case string:
		c, ok := straumerr.CodeByName(v)
		if !ok {
			f.fail("%s: unknown code %s", k, v)
		}
		return c
	case json.Number:
		return straumerr.Code(f.u16(k))
	}
	f.fail("%s: expected an error code", k)
	return 0
}

func (f *fields) u32s(k string) []uint32 {
	out := []uint32{}
	for i, x := range f.arr(k) {
		g := &fields{o: Obj{"v": x}, path: fmt.Sprintf("%s.%s[%d]", f.path, k, i)}
		out = append(out, g.u32("v"))
		if g.err != nil {
			f.fail("%v", g.err)
		}
	}
	return out
}

// MessageFromJSON decodes a message object.
func MessageFromJSON(v any) (wire.Message, error) {
	f := (&fields{path: "message"}).obj(v, "")
	m := message(f)
	return m, f.err
}

func message(f *fields) wire.Message {
	var m wire.Message
	if f.get("key") != nil {
		m.Key = f.bytes("key")
	}
	for i, h := range f.arr("headers") {
		g := f.obj(h, fmt.Sprintf("headers[%d]", i))
		m.Headers = append(m.Headers, wire.Header{Name: g.bytes("name"), Value: g.bytes("value")})
		if g.err != nil {
			f.fail("%v", g.err)
		}
	}
	m.Body = f.bytes("body")
	return m
}

func top(v any) (string, *fields, error) {
	o, ok := v.(Obj)
	if !ok {
		return "", nil, fmt.Errorf("expected an object")
	}
	name, _ := o["type"].(string)
	body := Obj{}
	if b, ok := o["body"]; ok {
		if body, ok = b.(Obj); !ok {
			return "", nil, fmt.Errorf("%s: body is not an object", name)
		}
	}
	return name, &fields{o: body, path: name}, nil
}

// RequestFromJSON decodes {"type": name, "body": {...}}; a missing body is
// empty.
func RequestFromJSON(v any) (wire.Request, error) {
	name, f, err := top(v)
	if err != nil {
		return nil, err
	}
	var req wire.Request
	switch name {
	case "Hello":
		req = &wire.Hello{MinVersion: f.u16("min_version"), MaxVersion: f.u16("max_version"),
			ClientName: f.str("client_name"), AuthMechanism: f.str("auth_mechanism"), AuthPayload: f.bytes("auth_payload")}
	case "Ping":
		req = &wire.Ping{Nonce: f.big("nonce")}
	case "Metadata":
		var topics []string
		for _, t := range f.arr("topics") {
			s, ok := t.(string)
			if !ok {
				f.fail("topics: expected strings")
			}
			topics = append(topics, s)
		}
		req = &wire.Metadata{Topics: topics}
	case "CreateTopic":
		req = &wire.CreateTopic{Name: f.str("name"), Partitions: f.u32("partitions"),
			ReplicationFactor: f.u16("replication_factor"), RetentionMs: f.big("retention_ms")}
	case "DeleteTopic":
		req = &wire.DeleteTopic{Name: f.str("name")}
	case "Produce":
		p := &wire.Produce{Topic: f.str("topic"), Partition: f.u32("partition"), ProducerID: f.big("producer_id"),
			ProducerSeq: f.big("producer_seq"), AckLevel: f.u8("ack_level"),
			ExpectedLeaderEpoch: f.u32("expected_leader_epoch"), TimeoutMs: f.u32("timeout_ms")}
		for i, m := range f.arr("messages") {
			p.Messages = append(p.Messages, message(f.obj(m, fmt.Sprintf("messages[%d]", i))))
		}
		req = p
	case "Fetch":
		req = &wire.Fetch{Topic: f.str("topic"), Partition: f.u32("partition"), FromSeq: f.big("from_seq"),
			MaxBytes: f.u32("max_bytes"), MaxWaitMs: f.u32("max_wait_ms"), ExpectedLeaderEpoch: f.u32("expected_leader_epoch")}
	case "ListTopics":
		req = &wire.ListTopics{}
	case "DescribeTopic":
		req = &wire.DescribeTopic{Name: f.str("name")}
	case "BrokerStats":
		req = &wire.BrokerStats{}
	default:
		return nil, fmt.Errorf("unknown request type %q", name)
	}
	return req, f.err
}

// ResponseFromJSON decodes {"type": name, "body": {...}}.
func ResponseFromJSON(v any) (wire.Response, error) {
	name, f, err := top(v)
	if err != nil {
		return nil, err
	}
	var resp wire.Response
	switch name {
	case "HelloOk":
		resp = &wire.HelloOk{Version: f.u16("version"), ClusterID: f.str("cluster_id"), NodeID: f.u32("node_id"),
			MaxFrameBytes: f.u32("max_frame_bytes"), MaxMessageBytes: f.u32("max_message_bytes")}
	case "Pong":
		resp = &wire.Pong{Nonce: f.big("nonce")}
	case "MetadataOk":
		m := &wire.MetadataOk{ClusterID: f.str("cluster_id"), ControllerNodeID: f.u32("controller_node_id")}
		for i, n := range f.arr("nodes") {
			g := f.obj(n, fmt.Sprintf("nodes[%d]", i))
			m.Nodes = append(m.Nodes, wire.Node{NodeID: g.u32("node_id"), Host: g.str("host"), Port: g.u16("port"), Rack: g.str("rack")})
			f.merge(g)
		}
		for i, t := range f.arr("topics") {
			g := f.obj(t, fmt.Sprintf("topics[%d]", i))
			tm := wire.TopicMetadata{Name: g.str("name"), Error: g.code("error")}
			for j, p := range g.arr("partitions") {
				h := g.obj(p, fmt.Sprintf("partitions[%d]", j))
				tm.Partitions = append(tm.Partitions, wire.PartitionMetadata{Partition: h.u32("partition"),
					LeaderNodeID: h.u32("leader_node_id"), LeaderEpoch: h.u32("leader_epoch"), Replicas: h.u32s("replicas"),
					InSyncReplicas: h.u32s("in_sync_replicas"), StartSeq: h.big("start_seq"), NextSeq: h.big("next_seq")})
				g.merge(h)
			}
			m.Topics = append(m.Topics, tm)
			f.merge(g)
		}
		resp = m
	case "CreateTopicOk":
		resp = &wire.CreateTopicOk{}
	case "DeleteTopicOk":
		resp = &wire.DeleteTopicOk{}
	case "ProduceOk":
		resp = &wire.ProduceOk{BaseSeq: f.big("base_seq"), Count: f.u32("count"), LeaderEpoch: f.u32("leader_epoch"),
			TimestampMs: f.big("timestamp_ms")}
	case "FetchOk":
		r := &wire.FetchOk{LeaderEpoch: f.u32("leader_epoch"), StartSeq: f.big("start_seq"), NextSeq: f.big("next_seq")}
		for i, m := range f.arr("messages") {
			g := f.obj(m, fmt.Sprintf("messages[%d]", i))
			r.Messages = append(r.Messages, wire.FetchedMessage{Seq: g.big("seq"), TimestampMs: g.big("timestamp_ms"), Message: message(g)})
			f.merge(g)
		}
		resp = r
	case "ListTopicsOk":
		r := &wire.ListTopicsOk{}
		for i, t := range f.arr("topics") {
			g := f.obj(t, fmt.Sprintf("topics[%d]", i))
			r.Topics = append(r.Topics, wire.TopicSummary{Name: g.str("name"), Partitions: g.u32("partitions")})
			f.merge(g)
		}
		resp = r
	case "DescribeTopicOk":
		r := &wire.DescribeTopicOk{Name: f.str("name"), ReplicationFactor: f.u16("replication_factor"), RetentionMs: f.big("retention_ms")}
		for i, p := range f.arr("partitions") {
			g := f.obj(p, fmt.Sprintf("partitions[%d]", i))
			r.Partitions = append(r.Partitions, wire.PartitionDescription{Partition: g.u32("partition"),
				StartSeq: g.big("start_seq"), NextSeq: g.big("next_seq"), MessageCount: g.big("message_count"),
				IndexBytes: g.big("index_bytes")})
			f.merge(g)
		}
		resp = r
	case "BrokerStatsOk":
		resp = &wire.BrokerStatsOk{NodeID: f.u32("node_id"), OpenConnections: f.u32("open_connections"),
			PendingBytes: f.big("pending_bytes"), PendingBytesPeak: f.big("pending_bytes_peak"),
			MaxPendingBytes: f.big("max_pending_bytes"), FetchWaiters: f.u32("fetch_waiters"),
			ProduceRequests: f.big("produce_requests"), GroupsCommitted: f.big("groups_committed"),
			Fsyncs: f.big("fsyncs"), MessagesAccepted: f.big("messages_accepted"),
			MessagesDeduplicated: f.big("messages_deduplicated"), BodyBytesReceived: f.big("body_bytes_received"),
			BodyBytesNew: f.big("body_bytes_new"), WalBytes: f.big("wal_bytes"), SegmentBytes: f.big("segment_bytes"),
			IndexBytes: f.big("index_bytes")}
	case "Error":
		resp = &wire.Error{Code: f.code("code"), Message: f.str("message"), LeaderNodeID: f.u32("leader_node_id"),
			LeaderEpoch: f.u32("leader_epoch"), RetryAfterMs: f.u32("retry_after_ms"),
			MinVersion: f.u16("min_version"), MaxVersion: f.u16("max_version")}
	default:
		return nil, fmt.Errorf("unknown response type %q", name)
	}
	return resp, f.err
}

func (f *fields) merge(g *fields) {
	if g.err != nil && f.err == nil {
		f.err = g.err
	}
}
