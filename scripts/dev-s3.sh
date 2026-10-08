#!/usr/bin/env sh
# dev-s3.sh: wait for the SeaweedFS S3 gateway from docker-compose.yml, then
# idempotently create the test bucket.
#
# Bucket-creation mechanism (verified against chrislusf/seaweedfs:4.48):
#   pipe `s3.bucket.create -name <bucket>` into `weed shell -master localhost:9333`.
#   NOTE: the shell's master flag is -master (there is no -s flag in this
#   version). The create command is NOT natively idempotent: on an existing
#   bucket it prints "error: bucket X already exists" and exits non-zero,
#   so this script treats that line as success.
#
# Credentials (verified against chrislusf/seaweedfs:4.48): WEED_S3_ACCESS_KEY_ID /
#   WEED_S3_SECRET_ACCESS_KEY env vars are NOT honored by the bundled s3
#   credential store. Without an IAM entry the gateway accepts ANY SigV4 key
#   pair (verified: wrong creds got 200). The reliable mechanism is
#   `s3.configure -access_key ... -secret_key ... -user admin -actions ...`
#   via weed shell; it persists in the filer store and survives restarts
#   (verified). s3.configure is idempotent on rerun.
#
# Requires: docker compose up -d first. Never starts docker itself.
set -eu

BUCKET="${S3_BUCKET:-test-bucket}"
# Master address as seen from INSIDE the container.
# The in-container port never changes even when the host port is remapped.
MASTER="localhost:9333"
# Default endpoint derives from the compose host port so override mode
# (SEAWEEDFS_S3_PORT=18333) polls OUR gateway, not whatever squats 8333.
S3_ENDPOINT="${S3_TEST_ENDPOINT:-http://localhost:${SEAWEEDFS_S3_PORT:-8333}}"
ACCESS_KEY="${S3_ACCESS_KEY:-test}"
SECRET_KEY="${S3_SECRET_KEY:-test12345678}"

# Run compose from the repo root regardless of the caller's cwd.
COMPOSE_FILE="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/docker-compose.yml"

# docker_volume_dir prints the filesystem backing the compose volumes when
# discoverable, so the free-space assert measures the right disk. Best
# effort: falls back to docker's default root.
docker_volume_dir() {
  docker info --format '{{.DockerRootDir}}' 2>/dev/null
}

echo "waiting for SeaweedFS S3 at ${S3_ENDPOINT} ..."
# Any HTTP response (even 403 AccessDenied) means the gateway is up; only
# connection failures mean "not ready".
i=0
until curl -s -o /dev/null -w '%{http_code}' "${S3_ENDPOINT}" --max-time 2 | grep -qE '^[1-5][0-9][0-9]$'; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "ERROR: S3 gateway not ready after ~30s; is 'docker compose up -d' running?" >&2
    exit 1
  fi
  sleep 1
done
echo "S3 endpoint is up."

# The dev stack saturates at ~20 GB and then fails SILENTLY (writes report
# success, reads return errors): assert free space up front so the failure
# is loud and attributable. Fail below the threshold; warn when close.
MIN_FREE_MB="${S3_MIN_FREE_MB:-4096}"
free_kb=$(df -Pk --output=avail "$(docker_volume_dir 2>/dev/null || echo /var/lib/docker)" 2>/dev/null | tail -1 | tr -d ' ')
if [ -n "$free_kb" ] && [ "$free_kb" -lt $((MIN_FREE_MB * 1024)) ]; then
  echo "ERROR: only $((free_kb / 1024)) MB free for the SeaweedFS volume (< ${MIN_FREE_MB} MB); the dev stack fails silently when full. Free space or raise the volume, then retry." >&2
  exit 1
fi

echo "ensuring bucket ${BUCKET} exists (idempotent) ..."
# Success prints "created bucket <name>" on stdout. On an existing bucket
# the shell exits non-zero with "already exists" on stderr — also success.
# Any other failure aborts the script via set -e.
if ! docker compose -f "$COMPOSE_FILE" exec -T seaweedfs sh -c "printf 's3.bucket.create -name %s\n' '${BUCKET}' | weed shell -master ${MASTER}" 2>&1 | grep -q 'created bucket\|already exists'; then
  echo "ERROR: failed to ensure bucket ${BUCKET}" >&2
  exit 1
fi

# Register/rotate the static S3 credentials so the gateway actually enforces
# auth (verified on 4.48: without this the gateway accepts ANY SigV4 key).
# s3.configure is idempotent: re-running it rewrites the same key pair.
echo "ensuring S3 credentials are registered ..."
# The secret only travels through stdin: never argv or the script's output.
printf 's3.configure -access_key %s -secret_key %s -user admin -actions Admin,Read,Write -apply\n' "$ACCESS_KEY" "$SECRET_KEY" |
  docker compose -f "$COMPOSE_FILE" exec -T seaweedfs weed shell -master "$MASTER" >/dev/null

# The signed Go integration probe (S3_TEST_STRICT=1 in CI) validates that
# registered credentials actually work with the SDK, not just weed shell.
echo "done: S3 ready at ${S3_ENDPOINT}, bucket ${BUCKET}."
