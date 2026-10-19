#!/usr/bin/env bash
# The weekly regression run (spec 44) on the weekly stack's instance. Run by .github/workflows/bench-weekly.yml,
# or by hand (with STACK=timelake-bench-weekly):
#   bench/infra/weekly.sh start [commit] [profile]   start the instance and the run of profiles/weekly.toml (or
#                                          another profile) at a commit (default HEAD, which must be pushed); the
#                                          instance stops itself when done
#   bench/infra/weekly.sh collect          copy finished run files from the bucket into bench/results/runs
# One-time setup of the stack: bench/infra/README.md, "Weekly runs".
set -Eeuo pipefail  # -E: functions inherit the ERR trap
cd "$(dirname "$0")"
source env.sh

start() {
  local ref=${1:-$(git rev-parse HEAD)} profile=${2:-weekly} id
  id=$(output InstanceId)
  # If anything below fails, the run never started: stop the instance again.
  trap 'aws ec2 stop-instances --instance-ids "$id" > /dev/null; echo "start failed; stopping $id"' ERR
  aws ec2 start-instances --instance-ids "$id" > /dev/null
  aws ec2 wait instance-running --instance-ids "$id"
  until [ "$(aws ssm describe-instance-information --filters Key=InstanceIds,Values="$id" \
    --query 'InstanceInformationList[0].PingStatus' --output text)" = Online ]; do sleep 10; done
  echo "$id is up; running $ref"
  # Stopping wiped the NVMe SSD: mount it again. Then check out the commit (remote.sh runs from it); earlier runs' files
  # are in the bucket already, and may be in the repository by now.
  ./ssm.sh "sudo bench/infra/mount.sh > /dev/null 2>&1 && git clean -fdq bench/results/runs && git fetch -q origin \
    && git checkout -q $ref && git log -1 --oneline"
  if [ "$profile" = weekly ]; then
    ./ssm.sh --bg 'bench/infra/remote.sh weekly'
  else
    ./ssm.sh --bg "bench/infra/remote.sh profile $profile"
  fi
  trap - ERR
}

# Run files reach the bucket after each run; only complete ones (with an end record) that aren't in the repository yet
# are copied. A published run file is never replaced: later fixes to it (such as backfilled costs) stay.
collect() {
  local tmp
  tmp=$(mktemp -d)
  aws s3 sync "s3://$(output DataBucket)/_out/runs" "$tmp" --only-show-errors
  for f in "$tmp"/*.jsonl; do
    [ -e "$f" ] || continue
    grep -q '"kind": "end"' "$f" || continue
    [ -e "../results/runs/$(basename "$f")" ] && continue
    cp "$f" ../results/runs/ && echo "new: $(basename "$f")"
  done
  rm -rf "$tmp"
}

case ${1:-} in
  start) start "${2:-}" "${3:-}" ;;
  collect) collect ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
