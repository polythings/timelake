#!/usr/bin/env bash
# The database servers timebench compares against (spec 44, spec 45), in Docker at pinned versions, so that changes
# in the history are ours; move a version deliberately. Used on a laptop and on the benchmark instance.
#   bench/infra/servers.sh up [name...]     start them (a running one at the pinned version is kept); default: all
#   bench/infra/servers.sh down [name...]   remove the containers (their data directories stay)
#   eval "$(bench/infra/servers.sh env)"    the variables timebench's adapters read
# Data lives in $BENCH_SERVERS_DIR (default bench/data/servers; /mnt/bench/servers on the instance). Ports are the
# servers' own plus 50000 (TimescaleDB 55432, ClickHouse 58123, ...), clear of servers already running on a laptop;
# QuestDB's HTTP port is 59900, as timebench's own S3 proxy uses 59000.
# Names: timescaledb clickhouse questdb influxdb3 influxdb3-defaults
set -euo pipefail
DIR=${BENCH_SERVERS_DIR:-$(cd "$(dirname "$0")/.." && pwd)/data/servers}
ALL=(timescaledb clickhouse questdb influxdb3 influxdb3-defaults)

TIMESCALEDB=timescale/timescaledb:2.30.2-pg18
CLICKHOUSE=clickhouse/clickhouse-server:26.8.28.14
QUESTDB=questdb/questdb:10.0.1
INFLUXDB=influxdb:3.12.1-core
INFLUXDB_FILE_LIMIT=100000  # InfluxDB 3 Core's default is 432 files, about 3 days in 10-minute files

# start <name> <image> [docker run options...] [-- command...]
start() {
  local name=$1 image=$2 c=timelake-bench-$1 opts=() cmd=()
  shift 2
  while [ $# -gt 0 ] && [ "$1" != -- ]; do opts+=("$1"); shift; done
  [ $# -gt 0 ] && shift && cmd=("$@")
  if [ "$(docker inspect -f '{{.Config.Image}} {{.State.Running}}' "$c" 2> /dev/null)" = "$image true" ]; then
    echo "$name: running ($image)"
    return
  fi
  docker rm -f "$c" > /dev/null 2>&1 || true
  mkdir -p "$DIR/$name"
  chmod 0777 "$DIR/$name"  # images that run as their own user (InfluxDB) write here too
  # ${a[@]+"${a[@]}"}: an empty array under set -u, which bash before 4.4 (macOS's 3.2) calls unbound
  docker run -d --name "$c" --restart unless-stopped ${opts[@]+"${opts[@]}"} "$image" ${cmd[@]+"${cmd[@]}"} > /dev/null
  echo "$name: started ($image)"
}

# wait_for <name> <command...>: until the command succeeds
wait_for() {
  local name=$1
  shift
  for _ in $(seq 120); do "$@" > /dev/null 2>&1 && return; sleep 1; done
  echo "$name didn't come up" >&2
  return 1
}

up() {
  case $1 in
    timescaledb)
      start timescaledb $TIMESCALEDB -p 55432:5432 -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench \
        -v "$DIR/timescaledb:/var/lib/postgresql"
      # Over TCP: the server that initializes the database listens on the socket only, then restarts.
      wait_for timescaledb docker exec timelake-bench-timescaledb pg_isready -q -h 127.0.0.1 -U postgres
      ;;
    clickhouse)
      start clickhouse $CLICKHOUSE -p 58123:8123 --ulimit nofile=262144:262144 -e CLICKHOUSE_PASSWORD=bench \
        -v "$DIR/clickhouse:/var/lib/clickhouse"
      wait_for clickhouse curl -sf http://localhost:58123/ping
      ;;
    questdb)
      start questdb $QUESTDB -p 59900:9000 -p 58812:8812 -v "$DIR/questdb:/var/lib/questdb"
      wait_for questdb curl -sf http://localhost:59900/
      ;;
    influxdb3 | influxdb3-defaults)
      local port=58181 limit=(--query-file-limit $INFLUXDB_FILE_LIMIT)
      [ "$1" = influxdb3-defaults ] && port=58182 limit=()
      start "$1" $INFLUXDB -p $port:8181 -v "$DIR/$1:/var/lib/influxdb3" -- influxdb3 serve --node-id bench \
        --object-store file --data-dir /var/lib/influxdb3/data --without-auth ${limit[@]+"${limit[@]}"}
      wait_for "$1" curl -sf http://localhost:$port/health
      ;;
    *) echo "unknown server $1 (${ALL[*]})" >&2; return 1 ;;
  esac
}

env_vars() {
  echo "export BENCH_TIMESCALEDB_DSN=postgresql://postgres:bench@localhost:55432/bench"
  echo "export BENCH_CLICKHOUSE_URL=http://default:bench@localhost:58123"
  echo "export BENCH_QUESTDB_HTTP=localhost:59900 BENCH_QUESTDB_PG=localhost:58812"
  echo "export BENCH_INFLUXDB_URL=http://localhost:58181"
  echo "export BENCH_INFLUXDB_DEFAULTS_URL=http://localhost:58182"
}

cmd=${1:-}
shift || true
names=("$@")
[ ${#names[@]} -eq 0 ] && names=("${ALL[@]}")
case $cmd in
  up) for n in "${names[@]}"; do up "$n"; done ;;
  down) for n in "${names[@]}"; do docker rm -f "timelake-bench-$n" > /dev/null 2>&1 || true; done ;;
  env) env_vars ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
