#!/usr/bin/env bash
# Builds the public demo warehouse: a slice of TSBS IoT (50 trucks × 3 days, ~1.2M rows per table)
# written as Timelake tables whose metadata already points at the URL they will be served from.
#
#   scripts/build-demo-warehouse.sh https://<store-id>.public.blob.vercel-storage.com/demo
#   scripts/build-demo-warehouse.sh http://127.0.0.1:4100/demo          # local testing (default)
#
# Output: bench/data/demo-site/<host and path of the URL>/, e.g. bench/data/demo-site/127.0.0.1_4100_demo/,
# holding the files to upload with paths relative to the URL origin. Each URL gets its own folder, so
# a local build never overwrites one made for a public host.
# Then upload them, keeping paths, to any static host with Range support (Vercel Blob, a public bucket, a CDN).
set -euo pipefail
cd "$(dirname "$0")/.."
BASE_URL=${1:-http://127.0.0.1:4100/demo}
SLUG=$(printf '%s' "$BASE_URL" | sed -E 's#^[a-z]+://##; s#/+$##; s#[^A-Za-z0-9.-]+#_#g')
OUT=bench/data/demo-site/$SLUG
SRC=bench/data/tsbs/parquet
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
TL=target/release/timelake
TRUCKS=${TRUCKS:-50}
cargo build --release -p timelake-cli
rm -rf "$OUT"
uv run python scripts/slice_tsbs.py "$SRC" "$TMP" "$TRUCKS"
# Small row groups: web readers fetch whole row-group column chunks, so 64k rows keeps a
# one-series read to a few hundred KB.
TLW=("$TL" -w "$BASE_URL" -o "timelake_local_root=$OUT")
"${TLW[@]}" import tsbs.tags "$TMP/tags.parquet"
for t in readings diagnostics; do
  "${TLW[@]}" import tsbs.$t "$TMP/$t.parquet" --time-column time --series-key tags_id --no-dedup \
    --prop timelake.row-group-rows=65536
done
"${TLW[@]}" maintain
"${TLW[@]}" index
echo; echo "built $(find "$OUT" -type f | wc -l | tr -d ' ') files, $(du -sh "$OUT" | cut -f1) in $OUT for $BASE_URL"
