# Leafrun Storage

Private, scoped Storage API and Temporal garbage collector for E03/M03. It never invokes Typst and never queries another domain's database. Wire types and routes come from `github.com/leafrun/leafrun-contracts/storage/v1` and its OpenAPI specification.

## Run

Build with Go 1.27. During M03, Contracts is unreleased; use the controller's external workspace containing Contracts and Storage. No local replace or go.work is shipped in this repository. After the Contracts v0.1.0 release, ordinary standalone module builds resolve that release.

```powershell
$env:GOWORK = 'C:/path/to/external/go.work'
go test ./...
go vet ./...
go build -o leafrun-storage.exe ./cmd/leafrun-storage
./leafrun-storage.exe migrate
./leafrun-storage.exe probe-s3
./leafrun-storage.exe api
# Run separately:
./leafrun-storage.exe worker
```

Required configuration:

| Variable | Meaning |
| --- | --- |
| LEAFRUN_STORAGE_DATABASE_URL | Storage-only PostgreSQL connection URL |
| LEAFRUN_STORAGE_S3_BUCKET | Private bucket; versioning required |
| LEAFRUN_STORAGE_SERVICE_CREDENTIALS | JSON array of `{token,subject,domain}`; tokens at least 16 bytes |
| LEAFRUN_STORAGE_OWNER_URL | Owner adapter base URL |
| LEAFRUN_STORAGE_OWNER_TOKEN | Separate bearer for owner adapter |
| AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY | AWS SDK credentials and region |

Optional: LEAFRUN_STORAGE_LISTEN (`:8081`), LEAFRUN_STORAGE_S3_ENDPOINT (path-style S3 emulator), LEAFRUN_STORAGE_SCRATCH_DIR (bounded upload spool), LEAFRUN_STORAGE_TEMPORAL_ADDRESS (`localhost:7233`), LEAFRUN_STORAGE_TEMPORAL_NAMESPACE (`default`), LEAFRUN_STORAGE_TEMPORAL_TASK_QUEUE (`leafrun-storage-gc`), LEAFRUN_STORAGE_SWEEP_INTERVAL (`5m`). Default writer/read leases 5 minutes, pending pin 10 minutes, orphan grace 24 hours, maximum upload 1 GiB. Recovery has an original 30-day deadline; immutable results expire 24 hours after first finalize. Snapshot/release data has no blanket result TTL.

No authorization fixture is enabled inside Storage. Development uses a separately configured owner fixture. Bearer credentials resolve Principal; caller principal/scope headers are ignored. Owner checks POST `/internal/v1/checks/access`, `/publishable`, `/pin-owner` using contract DTOs. Owner outage/unknown responses fail closed for reads and activation. Unknown pin owners protect references during reconciliation; hard result/recovery deadlines still dominate.

## Behavior and concurrency

Objects, attempts, resources, snapshots and child bindings, artifacts, pins, leases, receipts, physical versions, GC cursor, and finite deletion history live in Storage's own PostgreSQL schema. PostgreSQL clock controls all admission, expiry, and GC decisions. A transaction advisory mutex serializes catalog transitions, trading throughput for straightforward initial correctness. HTTP owner calls and S3 I/O never run inside catalog transactions. A separate session advisory lock prevents overlapping GC activities, including across worker replicas.

Uploads spool bounded bytes to temporary disk, verify SHA256 and size, and write conditional immutable keys under `storage/resource/uploads/`, `storage/result/uploads/`, or `storage/recovery/uploads/`. Catalog promotion retains the original key and exact S3 VersionId. Conditional PUT, HEAD reconciliation, and a persistent writing fence avoid duplicate versions following lost responses. An unresolved write with no discoverable object remains fenced until its writer expires; create a fresh upload rather than overwrite an uncertain attempt. GetUpload exposes its stable receipt. Temporary files are closed and removed on completion/cancel. The janitor handles only cataloged, safely stopped scratch files within its configured scratch root after writer expiry; crashed/unknown writers are not inferred dead from file age. Do not apply bucket lifecycle expiration to prefixes containing retained resources.

Resource deduplication is limited to scope, class, hash, and size. Snapshot IDs are opaque; Manifest.ID retains the canonical M02 content projection. Exact bindings cover project files plus external font trees; in-project fonts reuse project bindings. References and deletion marking are serialized atomically. Release pins retain snapshots until confirmed release/retirement. Pin holders must match the authenticated domain and subject, and must durably write a pending owner reference before activation. Only an active receipt makes a reference usable.

`POST /internal/v1/pins/{pinId}/release` accepts the exact scope/object/holder/policy binding and returns a bound `released` or `absent` receipt. Authorization on the requested scope precedes lookup, even when the pin has already been purged. A different binding is absent without changing or disclosing the existing pin. Authorization errors and dependency failures never acknowledge cleanup; the legacy DELETE endpoint remains compatible.

Finalize atomically publishes immutable metadata and the original 24-hour deadline. Identical input/idempotency retries return the original artifact even after writer expiry; changed input returns409. Finalize itself does not authorize public visibility: each artifact GET/content request requires current scope access and job publication verification before an expiry410 can be returned. Content streams through Storage, without presigned URLs or redirects. Read leases are bounded to 5 minutes and the remaining hard lifetime; EOF and cancellation release them. Admission captures its monotonic deadline before database lock wait, and S3 open latency consumes the existing lifetime. HTTP uses that exact lease deadline for socket writes so a stalled consumer cannot finish a buffered tail after expiry; writers without deadline support fail closed before content headers. Request cancellation forces an immediate write deadline; normal reader EOF does not truncate the final buffered bytes.

The Temporal interval schedule runs every 5 minutes by default. LEAFRUN_STORAGE_SWEEP_INTERVAL accepts whole-second durations from 1s to 24h; worker restart updates the same namespace-scoped leafrun-storage-gc schedule in place, preserves administrative pause state, and uses Skip overlap. The old cron recurrence is cancelled and confirmed closed before the replacement schedule is created; PostgreSQL also fences any in-flight GC activity. GC handles 100 catalog objects and 100 owner reconciliations per activity; additive migration 0002 records the last attempted pin reconciliation, ordering unattempted and least recently attempted pins first so unknown owners cannot starve later absent owners. Batch attempt time is persisted before owner I/O, including across crashes or deadline interruption; unknown references remain protected. S3 version inventory scans 500 entries with a durable cursor. It marks deleting before S3 deletion, retries physical operations, confirms absence, then removes snapshot child references and records deletion history. Extra versions and unknown orphans receive 24-hour grace; live writer keys and committed exact versions are protected. Expired multipart uploads are aborted only outside live writer protection and after grace, with absence confirmed. Metadata and deletion receipts are removed 30 days after confirmed physical deletion, without aging live pins or resources.

`/healthz`, `/readyz`, and `/metrics` expose liveness, dependency readiness, GC lag/deleted bytes/errors/orphans/unresolved pin metrics. Logs use structured slog fields. Store credentials in your deployment's secrets and protect internal health/metrics endpoints at the network boundary.

## Tests

Unit tests run normally. Real PostgreSQL/S3 tests require a dedicated Storage database and private test bucket; they create scoped data and intentionally age it for GC.

```powershell
# Set all runtime configuration listed above first.
$env:LEAFRUN_STORAGE_INTEGRATION = '1'
go test -count=1 -v ./internal/storage
# Race instrumentation requires a supported cgo compiler; Linux CI:
go test -race -count=1 ./...
```

The integration suite exercises exact scoped dedup, read/EOF release, concurrent finalize and lost receipt recovery, changed-input conflicts, publication denial, hard expiry under active pins, release snapshot retention and child collection, pending owner uncertainty, and recovery caps. Set LEAFRUN_STORAGE_TEMPORAL_INTEGRATION=1 to execute the real Temporal workflow and replay its stored history against LEAFRUN_STORAGE_TEMPORAL_ADDRESS. Lost-response tests inject an S3 acknowledgement loss and drop a real PostgreSQL successful COMMIT acknowledgement through a local test proxy. Version inventory and multipart regressions verify exact committed versions remain readable. API contract compatibility can be exercised through the generated Contracts Go client.
