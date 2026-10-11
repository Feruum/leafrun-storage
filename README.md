# leafrun-storage

## Keep outputs for a limited time

The planned Storage service will put PDF results and immutable project snapshots in S3-compatible storage. It will issue artifact IDs, serve authorized downloads, and delete expired objects. The default PDF lifetime is 24 hours.

Generation writes finished outputs here. Collaboration and Projects can pin snapshots that are still in use. Storage owns bytes and retention, not render jobs.

**Status:** this repository is planned and has no runnable implementation yet. For code you can try now, see the [local prototypes](https://github.com/leafrun/leafrun/blob/main/docs/repositories.md).

**First roadmap step:** [LEA-18 — Store and expire PDFs](https://linear.app/bulnik/issue/LEA-18/e03-s3-rezultaty-i-ochistka). Linear may require workspace access; the public [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) explains the build order. See the [repository map](https://github.com/leafrun/leafrun/blob/main/docs/repositories.md) and [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md).

No project license has been selected yet.
