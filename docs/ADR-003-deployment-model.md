# ADR-003: Deployment Model

## Status

Accepted

## Decision

Support Kubernetes as a first-class deployment target while preserving hybrid bare-metal compatibility.

## Rationale

- the project must run in Kubernetes-centric environments
- the project must also support explicit host-managed deployments
- coordination and data placement must not depend on Kubernetes-only primitives

