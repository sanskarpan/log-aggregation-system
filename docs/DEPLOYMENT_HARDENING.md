# Deployment Hardening

## Kubernetes

- Use the Helm chart production values as a starting point.
- Apply pod disruption budgets to keep each critical service available during node maintenance.
- Use pod anti-affinity so replicas do not concentrate on one node.
- Enable horizontal autoscaling for stateless services.
- Mount persistent volumes for ingesters so WAL replay and local caches survive restarts.

## Bare Metal

- Place ingesters on hosts with local NVMe and keep WAL plus hot cache on dedicated disks.
- Keep etcd, Kafka, PostgreSQL, and object storage on separate failure domains when possible.
- Run compactor and control-plane services away from the hottest ingest nodes if you want to reduce interference.

## Topology Guidance

- Gateway, distributor, parser, query-frontend, and control-plane are stateless and should scale horizontally.
- Ingester and querier benefit from local disk and conservative disruption policies.
- Compactor should be isolated enough to complete retention passes without contending with peak ingest traffic.
