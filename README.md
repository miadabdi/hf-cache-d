# hf-cache-d

A self-hosted **Hugging Face pull-through cache mirror** and **private
artifact registry** in one small Go service, backed by any S3-compatible
object store (tested with SeaweedFS).

- **Mirror**: HF Hub requests (`huggingface_hub`, `curl`) are served through
  this service. A miss is fetched anonymously from the Hub upstream, streamed
  to the client while being cached, and pinned by commit SHA — warm requests
  never touch the Hub again.
- **Registry**: authenticated `PUT`/`POST` push your own weights as
  immutable, sealed versions under the same HF-compatible read routes, so a
  stock `snapshot_download` pulls them exactly like a public model.

> **READS ARE ANONYMOUS — trusted networks only.**
> Every read route (metadata, files, listings, *including your uploaded
> private weights*) requires **no authentication**. Anyone who can reach the
> service can download everything it serves. Only the write lane (push/seal)
> is protected, by `PUSH_TOKEN`. Run this on a trusted internal network
> behind your own access control; do not expose it to the public internet.

## What it is not

- Not a multi-tenant service: one instance, one bucket, in-process locks.
- No garbage collection, no deletes, no quotas: everything cached or pushed
  is kept forever (see [Limitations](#limitations)).
- No broad HF API promise: the endpoint contract below is the tested surface,
  verified against one pinned client version.

## Quickstart (local)

```sh
git clone https://github.com/miadabdi/hf-cache-d && cd hf-cache-d

# 1-2: S3 backend (single-node SeaweedFS, dev/test fixture)
docker compose up -d
./scripts/dev-s3.sh                      # waits for readiness, creates the bucket

# 3: run the service against it
S3_ENDPOINT=http://localhost:8333 S3_BUCKET=test-bucket \
S3_ACCESS_KEY=test S3_SECRET_KEY=test12345678 PUSH_TOKEN=dev-token \
go run ./cmd/hf-cache-d

# 4-5: sanity
curl -s localhost:8080/healthz           # {"status":"ok","version":"dev"}
curl -s localhost:8080/                  # route index

# 6-8: pull a public snapshot through the mirror (cached from here on)
pip install huggingface_hub==0.36.2     # the tested client version
HF_ENDPOINT=http://localhost:8080 python - <<'PY'
from huggingface_hub import snapshot_download
# any public model works; this one is tiny for a first pull
snapshot_download("hf-internal-testing/tiny-random-bert", token=False)
PY

# 9-10: push + seal your own weights, then pull them like a model
printf 'my weights' > weights.bin
curl -X PUT -H "Authorization: Bearer dev-token" \
  --data-binary @weights.bin -H "Content-Length: 10" \
  localhost:8080/v1/artifacts/my/model/v1/weights.bin
# seal with the sha256 the PUT returned (compute: sha256sum weights.bin)
curl -X POST -H "Authorization: Bearer dev-token" -H 'Content-Type: application/json' \
  -d '{"files":{"weights.bin":"<sha256>"}}' \
  localhost:8080/v1/artifacts/my/model/v1/manifest
curl -s localhost:8080/my/model/resolve/main/weights.bin   # my weights
```

(If port 8333 is taken on your machine, `SEAWEEDFS_S3_PORT=18333 docker
compose up -d` remaps the gateway; `scripts/dev-s3.sh` is port-aware.)

## Configuration

All configuration is environment variables (`deploy/config.example.env` is
the annotated template; nothing loads a file automatically).

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP listen address. |
| `S3_ENDPOINT` | *required* | S3-compatible endpoint, scheme+port; path-style addressing. |
| `S3_BUCKET` | *required* | Bucket for cached and pushed objects. |
| `S3_ACCESS_KEY` | *required* | Static S3 access key. |
| `S3_SECRET_KEY` | *required* | Static S3 secret key. |
| `HF_UPSTREAM` | `https://huggingface.co` | Hub upstream for cache misses. |
| `PUSH_TOKEN` | *(empty)* | Bearer token for the push lane. Empty disables pushing entirely (startup warning logged). |
| `INTEGRITY_CHECK_INTERVAL` | `1h` | Periodic manifest integrity self-check (Go duration). `0` disables. Mismatch logs `CRITICAL: integrity mismatch:` — never deletes. |

## HTTP endpoint contract

Exactly these routes are implemented (everything else 404s):

**HF-compatible reads (anonymous):**

| Route | Meaning |
|---|---|
| `GET /api/models/{org}/{name}` | Repo info at `main` (siblings list). |
| `GET /api/models/{org}/{name}/revision/{rev}` | Repo info at a branch/tag/40-hex commit. |
| `GET /api/models/{org}/{name}/tree/{rev}` | Recursive tree listing (`recursive`, `cursor`, `expand`, `limit` honored). |
| `GET/HEAD /{org}/{name}/resolve/{rev}/{file}` | File body / metadata. Single `Range: bytes=a-b` served as `206` from the store. |

**Private registry:**

| Route | Meaning |
|---|---|
| `GET /v1/artifacts/{org}/{name}` | Version listing (anonymous). |
| `PUT /v1/artifacts/{org}/{name}/{version}/{file}` | Stage a file (bearer token; `Content-Length` required). Returns `{file, sha256, size}`. |
| `POST /v1/artifacts/{org}/{name}/{version}/manifest` | Seal the version (bearer). Body `{"files":{path:sha256}}`; staged bytes are re-hashed server-side and must match. Returns the synthetic `{version, commit}`. |

**Operational:** `GET /healthz` (`{"status":"ok","version":...}`), `GET /`
(route index), `GET /metricsz` (Prometheus text).

### Response semantics

- **`X-Cache: HIT|MISS`** on metadata and file responses: served from the
  store vs filled from upstream. Sealed local models are always `HIT`.
- **`X-Repo-Commit`**: the 40-hex commit the response is pinned to — the
  resolved public SHA, or the synthetic commit for a sealed version.
- **`ETag`**: the stored object's sha256 (quoted). Stable across restarts
  and identical for identical bytes; usable for client-side revalidation.
- **Commit pinning**: a floating ref (`main`, branch, tag) is resolved
  upstream and memoized for 5 minutes; every fetch and cache entry is pinned
  to the SHA, so stored bytes are immutable per commit and old commits stay
  readable after `main` moves.

### Sealed local models shadow the public repo

A repo with at least one sealed version **completely shadows** the public
repo of the same name, for every revision:

- `main` resolves to the **most recently sealed** version (seal time, not
  version-string ordering).
- A version string (`v1`) or its synthetic commit serves that exact seal.
- Anything else — including a real public commit SHA — is a **404**. There is
  never a fallback to the public upstream once sealed: that is the point.

## Client usage

**huggingface_hub** — tested with **`huggingface_hub==0.36.2`** (Python
3.12; also verified locally on 3.14). Point the client at the service:

```sh
pip install huggingface_hub==0.36.2
HF_ENDPOINT=http://localhost:8080 python -c "
from huggingface_hub import snapshot_download
# public model via the mirror (tiny example repo):
snapshot_download('hf-internal-testing/tiny-random-bert', token=False)
# your own sealed weights (repo you pushed + sealed, see below):
snapshot_download('my/model', token=False)
"
```

`token=False` keeps the client from looking for Hub credentials; the service
needs none for reads. Pulling a sealed model works identically — same
snapshot API, `main` = latest seal.

**curl** push / seal / list:

```sh
# stage (note the returned sha256)
curl -X PUT -H "Authorization: Bearer $TOKEN" \
     --data-binary @model.safetensors \
     -H "Content-Length: $(stat -c%s model.safetensors)" \
     "$ENDPOINT/v1/artifacts/org/name/v1/model.safetensors"

# seal (re-hash verified server-side; sizes optional)
curl -X POST -H "Authorization: Bearer $TOKEN" \
     -d "{\"files\":{\"model.safetensors\":\"$SHA256\"}}" \
     "$ENDPOINT/v1/artifacts/org/name/v1/manifest"

# list versions (anonymous)
curl -s "$ENDPOINT/v1/artifacts/org/name"
```

## Observability

- **Access log** (stderr, one line per request): method, path, status,
  bytes, duration, `cache=HIT|MISS`, `@commit`. Never any header values —
  `Authorization` and cookies cannot appear in it.
- **`/metricsz`**: Prometheus text exposition, counters only —
  `hf_cache_requests_total{route}`, `hf_cache_hits_total`,
  `hf_cache_misses_total`, `hf_cache_upstream_errors_total`,
  `hf_cache_bytes_served_total`, `hf_cache_bytes_pulled_total`.
- **Integrity self-check**: every `INTEGRITY_CHECK_INTERVAL`, one random
  cached manifest's files are re-hashed and compared against the recorded
  sha256. A mismatch logs `CRITICAL: integrity mismatch:` (the object is
  never deleted — remediation is a human decision). Verified manifests come
  from the in-memory read cache, i.e. recently served releases.

## Testing

Four suites; the default one is fully offline.

| Suite | Command | What it covers |
|---|---|---|
| Unit (offline) | `go test ./...` | All lanes against in-memory stores and fake upstreams. |
| Integration | `go test -tags s3compose ./...` | Cold/warm snapshot flows, byte-identity, zero-upstream warm pass, disconnect survival, sealed shadowing — against real SeaweedFS via `docker compose up -d && ./scripts/dev-s3.sh`. Skips with instructions when compose is down. |
| Real client | `go test -tags client ./test/ -run TestHFClient -v -timeout 600s` | Unmodified `huggingface_hub` 0.36.2 pulling public + sealed snapshots through the service (needs compose + the pip-installed client). |
| Race/vet | `go vet ./... && go test -race ./...` | Same suites under the race detector. |

Integration/client suites probe the compose endpoint and **skip** (not fail)
when it is absent. With a remapped port: `SEAWEEDFS_S3_PORT=18333 go test
-tags s3compose ./...`.

## Docker

```sh
docker build --build-arg VERSION=$(git describe --tags --always) -t hf-cache-d .
docker run -d -p 8080:8080 \
  -e S3_ENDPOINT=http://<host>:8333 -e S3_BUCKET=test-bucket \
  -e S3_ACCESS_KEY=test -e S3_SECRET_KEY=test12345678 \
  -e PUSH_TOKEN=dev-token hf-cache-d
curl -s localhost:8080/healthz     # {"status":"ok","version":"<VERSION>"}
```

The image is distroless (`static:nonroot`, uid 65532, no shell) containing
only the static binary. CI (`.github/workflows/ci.yml`) builds, smokes it
against compose SeaweedFS, and on `v*` tags publishes
`miadabdi/hf-cache-d:<tag>` and `:latest` to Docker Hub. A sample systemd
unit is in `deploy/hf-cache-d.service` — reference only, nothing installs it.

## Limitations (v1 ceilings)

- **Single instance.** Manifest/index read caches and locks are in-process;
  two daemons on one bucket would be incoherent.
- **Anonymous reads**, including uploaded weights — trusted networks only.
- **No GC, no deletes, no quotas.** Cold pulls and pushes grow the bucket
  forever; a partial upload that failed mid-flight stays as an unreferenced
  object (never served: only manifest-listed objects are).
- **Unbounded detached pulls.** A client disconnecting mid-download does not
  stop the server-side fetch (caching to completion is the feature); there
  is no concurrency cap or per-transfer deadline. Fine on a trusted network,
  dangerous on a hostile one.
- **In-process locks.** Concurrent pulls of the same file from one process
  are serialized per-release; cross-process coordination does not exist.
- **One tested client version.** `huggingface_hub` 0.36.2 is the verified
  pin; other versions may work but are untested.
