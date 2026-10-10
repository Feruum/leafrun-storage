# leafrun-storage

Planned S3 artifact and snapshot service: uploads, downloads, retention, pins and garbage collection.

**Status: planned component.** This repository currently contains its scope and roadmap links. It does not yet provide a runnable service or package. The existing local rendering prototype lives in [leafrun-generation](https://github.com/leafrun/leafrun-generation).

## Responsibility

Blob inventory, snapshot manifests, artifacts, uploads, pins, expiry, GC.

Owns the logical PostgreSQL database `storage_db`, its credentials and migrations. Other domains access this data through contracts, never cross-domain SQL.

Planned process roles:

- `api-download-gateway`
- `gc-workflow-workers`
- `gc-activity-workers`
- `reconciler`

## Dependencies

- [leafrun-contracts](https://github.com/leafrun/leafrun-contracts)
- [leafrun-platform](https://github.com/leafrun/leafrun-platform)
- `s3`
- `access-api`

## Boundaries

No unique durable local state; activity scratch and workflow replay cache disposable. API and worker processes scale and release independently. S3 holds bytes; domain databases hold durable metadata. Redis is a disposable cache; NATS JetStream carries domain events; Temporal owns workflow task queues.

## Roadmap

- [LEA-18 — S3 artifacts and garbage collection](https://linear.app/bulnik/issue/LEA-18/e03-s3-rezultaty-i-ochistka)
- [LEA-114 — Caching, operations and scaling](https://linear.app/bulnik/issue/LEA-114/e15-keshirovanie-ekspluataciya-i-masshtabirovanie)

See the [development roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) for dependencies and acceptance milestones. Creating this repository does not complete its implementation issues.

## Contributing

Use English for code comments, documentation, examples, commit messages and pull requests. See the [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md) and [current architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md).

## License status

A project license has not been selected. No license is assigned by this repository scaffold.
