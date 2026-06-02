# Docker Compose Scaffold

This directory provides a local dependency stack for development and smoke testing.

Included infrastructure:

- `etcd`
- `postgres`
- `redis`
- `minio`
- `kafka` with `zookeeper`

The application services are not yet wired into Compose because the current implementation pass focuses on scaffolding the binaries and contracts first.

