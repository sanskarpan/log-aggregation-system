# Roadmap

## Milestone 1

- scaffold services, docs, and deployment targets
- lock canonical event and service boundaries

## Milestone 2

- implement single-node ingest, parse, index, and query loop
- prove WAL replay and local query execution

## Milestone 3

- implement distributed write path with etcd ring and Kafka decoupling
- flush chunks and segments to object storage

## Milestone 4

- implement query planner, caches, bloom pruning, and field-aware execution
- expose compatible query surfaces to Grafana

## Milestone 5

- add multi-tenant auth, quotas, retention, audit, and production operations
- harden Kubernetes and hybrid deployment guides

