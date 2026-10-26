#!/usr/bin/env bash
# Copies conformance/ from a Straum checkout into testdata/conformance/ and
# records the checkout's commit in testdata/conformance/SOURCE. Exits 1 if
# the copy differed from what was committed, so a drift is never silent.
#
#   scripts/sync-conformance.sh [--check] <straum-checkout>
#
# --check compares without writing.
set -euo pipefail

check=0
if [ "${1:-}" = "--check" ]; then
    check=1
    shift
fi
src="${1:?usage: scripts/sync-conformance.sh [--check] <straum-checkout>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
dst="$root/testdata/conformance"

commit="$(git -C "$src" rev-parse HEAD)"
pinned="$(sed -n 's/^commit: //p' "$root/CONTRACT")"
if [ "$commit" != "$pinned" ]; then
    echo "sync-conformance: $src is at $commit; CONTRACT pins $pinned" >&2
fi

tmp="$(mktemp -d)"
trap 'rm -rf -- "${tmp:?}"' EXIT
cp -R "$src/conformance/." "$tmp/"
printf 'straum %s conformance/\n' "$commit" > "$tmp/SOURCE"

if diff -r "$tmp" "$dst" > /dev/null 2>&1; then
    echo "sync-conformance: testdata/conformance matches $commit"
    exit 0
fi
diff -rq "$tmp" "$dst" || true
if [ "$check" -eq 0 ]; then
    rm -rf -- "${dst:?}"
    mkdir -p "$dst"
    cp -R "$tmp/." "$dst/"
    echo "sync-conformance: testdata/conformance updated from $commit; review and commit it" >&2
else
    echo "sync-conformance: testdata/conformance differs from $commit" >&2
fi
exit 1
