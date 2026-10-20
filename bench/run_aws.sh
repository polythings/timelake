#!/usr/bin/env bash
# TSBS IoT and the Phase 0 query suites from this machine against real S3 in us-east-2 (bench/infra stack, buckets
# only: bench/infra/up.sh). Latency includes the trip from here to the region; request counts and request cost don't
# depend on where the client runs. Needs: aws login --profile timelake.
#   bench/run_aws.sh                 everything (several hours; keeps the machine awake)
#   bench/run_aws.sh bench-s3        one step: data | queries | bench-local | bench-s3
# Results: results/runs/ (timebench) and results/*.laptop-*.jsonl (Phase 0 suites, compare_aws.py).
set -euo pipefail
cd "$(dirname "$0")"
source infra/env.sh
PY="uv run python"
bucket=$(output DataBucket)
export BENCH_TABLE_BUCKET_ARN=$(output TableBucketArn)
export BENCH_MACHINE="laptop (M1 Pro) to S3 us-east-2 over the internet"
export BENCH_ICEBERG_S3_DIR=iceberg-aws

# Counting proxy on :59100 (uncounted listener :59102 for probes) and an uncounted one on :59000 for setup work,
# both in AWS mode.
proxy() {
  pkill -f '.bin/s3proxy' || true
  sleep 1
  nohup .bin/s3proxy -aws-bucket "$bucket" -listen :59100 -listen-uncounted :59102 -admin :59101 -ttfb 0ms >> data/s3proxy-aws.log 2>&1 &
  nohup .bin/s3proxy -aws-bucket "$bucket" -listen :59000 -admin :59001 -ttfb 0ms >> data/s3proxy-aws-setup.log 2>&1 &
  until curl -sf localhost:59101/stats > /dev/null && curl -sf localhost:59001/stats > /dev/null; do sleep 0.2; done
}

data() {
  for d in tsbs/parquet-sorted tsbs/vortex-sorted; do aws s3 sync "data/$d" "s3://$bucket/$d" --only-show-errors; done
  $PY scripts/tsbs_to_iceberg.py s3   # through the uncounted proxy, so paths are s3://bench/...
}

queries() {
  for t in local s3; do
    local tag=laptop-$t
    rm -f results/*."$tag".jsonl
    for engine in duckdb duckdb-iceberg duckdb-vortex datafusion polars datafusion-rs datafusion-rs-vortex; do
      for suite in tsbs_iot iot_point; do
        $PY run.py --runs 3 --timeout 300 --storage $t --tag $tag --layout sorted --suite $suite --engine $engine
      done
    done
  done
}

step() {
  echo "== $1 $(date -u +%FT%TZ)"
  case $1 in
    data) proxy; data ;;
    queries) proxy; queries ;;
    bench-local) uv run timebench run tsbs-iot --target local --systems timelake,iceberg,lance,parquet ;;
    # S3 Tables first: its managed compaction runs on its own schedule, so its clock starts early. Maintenance
    # waits until it has run (hours).
    bench-s3) proxy; uv run timebench run tsbs-iot --target aws-s3 --ttfb-ms 0 \
      --systems s3tables,timelake,iceberg,lance,parquet ;;
    *) echo "unknown step $1"; exit 2 ;;
  esac
  echo "== $1 done $(date -u +%FT%TZ)"
}

if [ $# -gt 0 ]; then
  step "$1"
else
  for s in data queries bench-local bench-s3; do
    caffeinate -i bash "$0" "$s" || echo "== $s FAILED"
  done
  pkill -f '.bin/s3proxy' || true
fi
