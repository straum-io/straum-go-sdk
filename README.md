# straum-go-sdk

A Go client for [Straum](https://github.com/straum-io/straum) brokers:
produce, fetch and administer topics over Straum protocol version 0. Pure
Go, standard library only.

Straum speaks its own binary protocol over TCP. This module implements it
for Go programs (the Straum CLI and the console backend will build on it)
and is checked against the same conformance suite as Straum's Rust client. Like
the broker, it is experimental: version 0 of the protocol is one node, one
partition per topic, and no authentication beyond loopback.

## What it does

- **Produce** keyed messages with headers. The partition comes from the
  protocol's default partitioner (CRC-32C of the key modulo the partition
  count) or from the caller. Acknowledgements are durable: the broker
  answers after fsync.
- **Fetch** with a byte budget and a long-poll wait, or iterate a partition
  from a position the caller keeps (the broker stores no offsets in
  version 0).
- **Administer**: create topics, metadata, a paged topic list, topic description,
  broker counters, ping.
- **One client for many goroutines.** Requests are pipelined on one
  connection per broker; every call takes a `context.Context`; bytes in
  flight per connection are bounded, and a full connection blocks or fails
  fast, as configured.
- **The protocol's client rules built in.** On `NotLeader` or
  `FencedLeaderEpoch` the client refreshes metadata and retries at the
  leader; on `Overloaded` it backs off; after a lost connection it
  reconnects and resends the same request. Retries are bounded.
- **Typed errors** for every protocol error code, with their retry class:
  `errors.Is(err, straumerr.UnknownTopic)`.

## Quick start

You need Go 1.26 or later and a running `straum-broker` (build it from
the Straum repository; `cargo run --release -p straum-broker -- --data-dir
/tmp/straum --cluster-id dev --node-id 1 --listen 127.0.0.1:7171`).

```sh
go get github.com/straum-io/straum-go-sdk
```

```go
ctx := context.Background()
c, err := client.New(ctx, client.Config{Addrs: []string{"127.0.0.1:7171"}})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

_ = admin.New(c).CreateTopic(ctx, "orders", admin.TopicConfig{})

ack, err := producer.New(c, producer.Options{}).Produce(ctx, "orders",
	client.Message{Key: []byte("customer-42"), Body: []byte(`{"order": 1001}`)})
if err != nil {
	log.Fatal(err)
}

it := consumer.New(c, consumer.Options{}).Iterator("orders", ack.Partition, 0)
m, err := it.Next(ctx)
fmt.Printf("seq %d: %s\n", m.Seq, m.Body)
```

Complete examples are in the package documentation (`example_test.go` of
`producer`, `consumer` and `admin`).

## Packages

| Package | |
|---|---|
| `client` | connections, Hello, pipelining, routing to partition leaders, retries; `Message`, `Metadata` |
| `producer` | `Produce` (default partitioner), `ProduceTo` (explicit partition), `Partition` |
| `consumer` | `Fetch` and an `Iterator` that keeps its position |
| `admin` | `CreateTopic`, `Metadata`, `ListTopics`, `DescribeTopic`, `BrokerStats`, `Ping` |
| `straumerr` | error codes, retry classes, the broker's error |

These five packages are the module's public API. The protocol codec is
`internal/wire` and may change without notice.

## The contract

The protocol is Straum's `docs/protocol.md` at the commit pinned in
[`CONTRACT`](CONTRACT). `testdata/conformance/` is a copy of Straum's
`conformance/` from that commit:

- every golden frame decodes to its JSON description and re-encodes to the
  same bytes (`go test ./internal/wire`);
- every partitioner vector matches;
- the broker scenarios run against a live `straum-broker`, and the client
  scenarios run the SDK against Straum's scripted `straum-fake-server`
  (`scripts/live.sh`).

`scripts/sync-conformance.sh <straum-checkout>` copies the suite from a
Straum checkout and exits 1 if it changed; `--check` compares only.

## Development

```sh
scripts/validate.sh quick                 # before a commit: gofmt, vet, staticcheck, unit tests
scripts/validate.sh push <straum-checkout> # before a push: quick, govulncheck, -race, live conformance
scripts/validate.sh pr <straum-checkout>   # before a PR: push, fuzzing 30 s per target
```

The live tests need a Straum checkout at the `CONTRACT` commit and a Rust
toolchain: `scripts/live.sh` builds `straum-broker` and
`straum-fake-server` with cargo (macOS or Linux) and runs
`go test -tags live ./internal/conformance/`. CI runs the same steps on
Linux.

Tooling outside the standard library: `staticcheck`
(`honnef.co/go/tools`) and `govulncheck` (`golang.org/x/vuln`), versions
pinned in `scripts/validate.sh` and run with `go run`, so neither is a
dependency of the module.

## License

Apache 2.0, see [LICENSE](LICENSE).
