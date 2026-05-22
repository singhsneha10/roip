# ADR-002: Delta Lake chosen over Apache Iceberg

## Status
Accepted

## Context
We need an open table format that supports ACID transactions,
schema evolution, time travel, and efficient upserts (MERGE)
on our Parquet-based lakehouse.

## Decision
Use Delta Lake (open source, not Databricks-specific).

## Consequences
**Positive:**
- Best-in-class Python/PySpark integration
- MERGE INTO syntax is mature and well-documented
- Z-ordering for data skipping is straightforward
- Strong community and Databricks ecosystem alignment
- Delta Sharing for secure cross-team data access

**Negative:**
- Less multi-engine support than Iceberg
  (Iceberg works natively with Flink, Trino, Hive)
- If we need Flink in the future, migration cost is real

## Alternatives considered
- Apache Iceberg: better for multi-engine shops, chosen by
  Netflix and Apple. Would choose Iceberg if Flink was in scope.
- Apache Hudi: strong upsert performance, but smaller community
  and steeper operational complexity.