#!/usr/bin/env bash
# Validation levels; success is the exit status.
#
#   scripts/validate.sh quick                    before every commit
#   scripts/validate.sh push <straum-checkout>   before every push
#   scripts/validate.sh pr <straum-checkout>     before a PR
#
# quick: gofmt, go vet, staticcheck (also with the live tag), unit tests.
# push:  quick, unit tests with -race, the conformance copy against the
#        checkout, the live conformance tests with -race (scripts/live.sh).
# pr:    push, then each fuzz target for FUZZTIME (default 30s).
set -euo pipefail

STATICCHECK="honnef.co/go/tools/cmd/staticcheck@v0.8.2"
level="${1:?usage: scripts/validate.sh quick|push|pr [straum-checkout]}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

quick() {
    unformatted="$(gofmt -l .)"
    if [ -n "$unformatted" ]; then
        echo "gofmt: $unformatted" >&2
        return 1
    fi
    go vet ./...
    go vet -tags live ./...
    go run "$STATICCHECK" ./...
    go run "$STATICCHECK" -tags live ./...
    go test -count=1 ./...
}

push() {
    straum="${1:?usage: scripts/validate.sh $level <straum-checkout>}"
    quick
    go test -race -count=1 ./...
    scripts/sync-conformance.sh --check "$straum"
    scripts/live.sh "$straum" -race
}

case "$level" in
quick) quick ;;
push) push "${2:-}" ;;
pr)
    push "${2:-}"
    for target in FuzzDecodeRequest FuzzDecodeResponse FuzzFrame; do
        go test ./wire/ -run '^$' -fuzz "^${target}\$" -fuzztime "${FUZZTIME:-30s}"
    done
    ;;
*)
    echo "unknown level $level" >&2
    exit 2
    ;;
esac
echo "validate $level: ok"
