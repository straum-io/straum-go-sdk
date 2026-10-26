package conformance

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/straum-io/straum-go-sdk/client"
	"github.com/straum-io/straum-go-sdk/consumer"
	"github.com/straum-io/straum-go-sdk/internal/wire"
	"github.com/straum-io/straum-go-sdk/producer"
	"github.com/straum-io/straum-go-sdk/straumerr"
)

// Broker is a straum-broker process with its own data directory.
type Broker struct {
	cmd  *exec.Cmd
	Addr string
	dir  string
}

// StartBroker starts bin with a new data directory, --listen
// 127.0.0.1:0, --cluster-id conformance, --node-id 1 and the given
// settings, and reads the address from its "listening on" line.
func StartBroker(bin string, settings map[string]string) (*Broker, error) {
	dir, err := os.MkdirTemp("", "straum-go-conformance-")
	if err != nil {
		return nil, err
	}
	args := []string{"--data-dir", dir, "--listen", "127.0.0.1:0", "--cluster-id", "conformance", "--node-id", "1"}
	for k, v := range settings {
		args = append(args, "--"+strings.ReplaceAll(k, "_", "-"), v)
	}
	cmd := exec.Command(bin, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	b := &Broker{cmd: cmd, dir: dir}
	addr := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			if a, ok := strings.CutPrefix(s.Text(), "straum-broker: listening on "); ok {
				addr <- a
			}
		}
	}()
	select {
	case b.Addr = <-addr:
		return b, nil
	case <-time.After(30 * time.Second):
		b.Stop()
		return nil, errors.New("the broker did not start listening")
	}
}

// Stop kills the broker and removes its data directory.
func (b *Broker) Stop() {
	_ = b.cmd.Process.Kill()
	_ = b.cmd.Wait()
	_ = os.RemoveAll(b.dir)
}

// Settings returns a scenario's "broker" object as flag values.
func Settings(scenario Obj) map[string]string {
	out := map[string]string{}
	if o, ok := scenario["broker"].(Obj); ok {
		for k, v := range o {
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}

type received struct {
	resp wire.Response
	at   time.Time
}

// scriptConn is one connection of a broker scenario. Writes go through a
// goroutine, so a send never blocks the script even when the broker stops
// reading. Responses are read only while a step waits for one, so a
// scenario that never expects them lets them fill the socket.
type scriptConn struct {
	nc     net.Conn
	out    chan []byte
	nextID uint64
	got    map[uint64]received
	buf    []byte
	closed bool
}

func openScriptConn(addr string) (*scriptConn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &scriptConn{nc: nc, out: make(chan []byte, 1<<16), nextID: 1, got: map[uint64]received{}}
	go func() {
		for b := range c.out {
			if _, err := nc.Write(b); err != nil {
				return
			}
		}
	}()
	return c, nil
}

// wait reads until more frames are complete, the connection closes, or the
// deadline; false at the deadline.
func (c *scriptConn) wait(deadline time.Time) (bool, error) {
	if !time.Now().Before(deadline) {
		return false, nil
	}
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return false, err
	}
	chunk := make([]byte, 64<<10)
	n, err := c.nc.Read(chunk)
	c.buf = append(c.buf, chunk[:n]...)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return false, nil
		}
		c.closed = true
	}
	for len(c.buf) >= wire.HeaderSize {
		h, err := wire.ParseHeader(c.buf)
		if err != nil {
			return true, err
		}
		if len(c.buf) < wire.HeaderSize+h.BodyLen() {
			break
		}
		resp, err := wire.DecodeResponse(h.Type, c.buf[wire.HeaderSize:wire.HeaderSize+h.BodyLen()])
		if err != nil {
			return true, fmt.Errorf("response does not decode: %w", err)
		}
		c.got[h.CorrelationID] = received{resp, time.Now()}
		c.buf = c.buf[wire.HeaderSize+h.BodyLen():]
	}
	return true, nil
}

type sent struct {
	conn string
	corr uint64
	at   time.Time
}

type script struct {
	addr  string
	conns map[string]*scriptConn
	sent  map[string]sent
}

func ms(o Obj, k string, def int64) time.Duration {
	if n, ok := o[k].(json.Number); ok {
		if v, err := n.Int64(); err == nil {
			return time.Duration(max(v, 0)) * time.Millisecond
		}
	}
	return time.Duration(def) * time.Millisecond
}

// RunBrokerScenario plays a scenario of conformance/scenarios against a
// new broker started from bin.
func RunBrokerScenario(bin string, scenario Obj) error {
	b, err := StartBroker(bin, Settings(scenario))
	if err != nil {
		return err
	}
	defer b.Stop()
	s := &script{addr: b.Addr, conns: map[string]*scriptConn{}, sent: map[string]sent{}}
	defer func() {
		for _, c := range s.conns {
			close(c.out)
			_ = c.nc.Close()
		}
	}()
	steps, _ := scenario["steps"].([]any)
	if len(steps) == 0 {
		return errors.New("scenario has no steps")
	}
	return s.steps(steps, "")
}

func (s *script) steps(steps []any, suffix string) error {
	for i, st := range steps {
		o, ok := st.(Obj)
		if !ok {
			return fmt.Errorf("step %d is not an object", i)
		}
		if err := s.step(o, suffix); err != nil {
			return fmt.Errorf("step %d %s: %w", i, show(o), err)
		}
	}
	return nil
}

func (s *script) conn(name string) (*scriptConn, error) {
	c, ok := s.conns[name]
	if !ok {
		return nil, fmt.Errorf("connection %s not open", name)
	}
	return c, nil
}

func (s *script) step(o Obj, suffix string) error {
	str := func(k string) string { v, _ := o[k].(string); return v }
	id := ""
	if v := str("id"); v != "" {
		id = v + suffix
	}
	switch {
	case str("connect") != "":
		c, err := openScriptConn(s.addr)
		if err != nil {
			return err
		}
		s.conns[str("connect")] = c
	case str("send") != "":
		name := str("send")
		c, err := s.conn(name)
		if err != nil {
			return err
		}
		req, err := RequestFromJSON(o["request"])
		if err != nil {
			return err
		}
		corr := c.nextID
		if v, ok := o["correlation_id"].(string); ok {
			if corr, err = strconv.ParseUint(v, 10, 64); err != nil {
				return err
			}
		} else {
			c.nextID++
		}
		frame, err := wire.AppendRequest(nil, corr, req)
		if err != nil {
			return err
		}
		c.out <- frame
		if id != "" {
			s.sent[id] = sent{name, corr, time.Now()}
		}
	case str("send_raw") != "":
		name := str("send_raw")
		c, err := s.conn(name)
		if err != nil {
			return err
		}
		raw, err := hex.DecodeString(str("hex"))
		if err != nil {
			return err
		}
		c.out <- raw
		if id != "" {
			corr, _ := strconv.ParseUint(str("correlation_id"), 10, 64)
			s.sent[id] = sent{name, corr, time.Now()}
		}
	case str("expect") != "":
		name := str("expect")
		snd, ok := s.sent[id]
		if !ok {
			return fmt.Errorf("no send with id %s", id)
		}
		if snd.conn != name {
			return fmt.Errorf("%s was sent on %s, not %s", id, snd.conn, name)
		}
		c, err := s.conn(name)
		if err != nil {
			return err
		}
		deadline := time.Now().Add(ms(o, "within_ms", 5000))
		var r received
		for {
			if r, ok = c.got[snd.corr]; ok {
				delete(c.got, snd.corr)
				break
			}
			if c.closed {
				return fmt.Errorf("connection closed before the response to %s", id)
			}
			more, err := c.wait(deadline)
			if err != nil {
				return err
			}
			if !more {
				return fmt.Errorf("no response to %s in time", id)
			}
		}
		if waited, least := r.at.Sub(snd.at), ms(o, "not_before_ms", 0); waited < least {
			return fmt.Errorf("response to %s after %v, expected at least %v", id, waited, least)
		}
		got, err := Parse(mustJSON(ResponseJSON(r.resp)))
		if err != nil {
			return err
		}
		return Match(o["response"], got, "response")
	case str("expect_closed") != "":
		c, err := s.conn(str("expect_closed"))
		if err != nil {
			return err
		}
		deadline := time.Now().Add(ms(o, "within_ms", 5000))
		for !c.closed {
			more, err := c.wait(deadline)
			if err != nil {
				return err
			}
			if !more {
				return errors.New("the connection is still open")
			}
		}
	case str("close") != "":
		if c, ok := s.conns[str("close")]; ok {
			close(c.out)
			_ = c.nc.Close()
			delete(s.conns, str("close"))
		}
	case o["sleep_ms"] != nil:
		time.Sleep(ms(o, "sleep_ms", 0))
	case o["repeat"] != nil:
		n, _ := o["repeat"].(json.Number).Int64()
		inner, _ := o["steps"].([]any)
		for i := range n {
			if err := s.steps(inner, fmt.Sprintf("#%d", i)); err != nil {
				return err
			}
		}
	default:
		return errors.New("unknown step")
	}
	return nil
}

// mustJSON round-trips a value built from Go types into the form Parse
// gives, so Match compares like with like.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// RunClientScenario plays a scenario of conformance/client-scenarios: it
// starts fakeServer (Straum's straum-fake-server) with the scenario file,
// performs the scenario's operation through the SDK's API, and checks the
// outcome and the fake server's exit status.
func RunClientScenario(fakeServer, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	v, err := Parse(data)
	if err != nil {
		return err
	}
	scenario := v.(Obj)
	cmd := exec.Command(fakeServer, path)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	addr, ok := strings.CutPrefix(strings.TrimSpace(line), "straum-fake-server: listening on ")
	if err != nil || !ok {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("fake server did not start: %q %v %s", line, err, stderr.String())
	}
	outcome, opErr := perform(addr, scenario["client"])
	waitErr := cmd.Wait()
	if opErr != nil {
		return opErr
	}
	if waitErr != nil {
		return fmt.Errorf("fake server: %v: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	got, err := Parse(mustJSON(outcome))
	if err != nil {
		return err
	}
	return Match(scenario["result"], got, "result")
}

// perform runs the scenario's client operation and returns its outcome as
// {"ok": <response body>} or {"error": <code>}.
func perform(addr string, op any) (Obj, error) {
	f := &fields{path: "client"}
	f = f.obj(op, "")
	cfg := client.Config{Addrs: []string{addr}}
	if _, ok := f.o["max_attempts"]; ok {
		cfg.Retry.MaxAttempts = int(f.u32("max_attempts"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.New(ctx, cfg)
	if err != nil {
		return outcome(nil, err), nil
	}
	defer c.Close()
	var resp wire.Response
	switch f.str("op") {
	case "produce":
		var msgs []client.Message
		for i, m := range f.arr("messages") {
			wm := message(f.obj(m, fmt.Sprintf("messages[%d]", i)))
			cm := client.Message{Key: wm.Key, Body: wm.Body}
			for _, h := range wm.Headers {
				cm.Headers = append(cm.Headers, client.Header(h))
			}
			msgs = append(msgs, cm)
		}
		p := producer.New(c, producer.Options{ProducerID: f.big("producer_id"), FirstSeq: f.big("producer_seq")})
		if f.err != nil {
			return nil, f.err
		}
		ack, err := p.ProduceTo(ctx, f.str("topic"), f.u32("partition"), msgs...)
		if err != nil {
			return outcome(nil, err), nil
		}
		resp = &wire.ProduceOk{BaseSeq: ack.BaseSeq, Count: ack.Count, LeaderEpoch: ack.LeaderEpoch, TimestampMs: uint64(ack.Timestamp.UnixMilli())}
	case "fetch":
		wait := time.Duration(f.u32("max_wait_ms")) * time.Millisecond
		if wait == 0 {
			wait = -1 // no wait
		}
		cons := consumer.New(c, consumer.Options{MaxBytes: f.u32("max_bytes"), MaxWait: wait})
		if f.err != nil {
			return nil, f.err
		}
		b, err := cons.Fetch(ctx, f.str("topic"), f.u32("partition"), f.big("from_seq"))
		if err != nil {
			return outcome(nil, err), nil
		}
		r := &wire.FetchOk{LeaderEpoch: b.LeaderEpoch, StartSeq: b.StartSeq, NextSeq: b.NextSeq}
		for _, m := range b.Messages {
			wm := wire.FetchedMessage{Seq: m.Seq, TimestampMs: uint64(m.Timestamp.UnixMilli()),
				Message: wire.Message{Key: m.Key, Body: m.Body}}
			for _, h := range m.Headers {
				wm.Headers = append(wm.Headers, wire.Header(h))
			}
			r.Messages = append(r.Messages, wm)
		}
		resp = r
	default:
		return nil, fmt.Errorf("unknown client op %v", f.o["op"])
	}
	return outcome(resp, nil), f.err
}

func outcome(resp wire.Response, err error) Obj {
	if err == nil {
		return Obj{"ok": ResponseJSON(resp)["body"]}
	}
	var se *straumerr.Error
	if errors.As(err, &se) {
		return Obj{"error": code(se.Code)}
	}
	return Obj{"error": "client: " + err.Error()}
}
