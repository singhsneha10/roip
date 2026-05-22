# ADR-001: Kafka partitioning by order_id

## Status
Accepted

## Context
Events for the same order (PLACED → CONFIRMED → SHIPPED → DELIVERED)
must be processed in order. Kafka only guarantees ordering within a
single partition. We need to ensure all events for order ORD-123
always land on the same partition.

## Decision
Partition all topics by `order_id` as the message key.
Kafka hashes the key to a consistent partition number.

## Consequences
**Positive:**
- Ordering guaranteed per order across all event types
- Enables stateful stream joins on order_id without shuffling
- Consumer lag is measurable per order segment

**Negative:**
- Hot partitions if order_id distribution is skewed
  (e.g. bulk B2B orders with one order_id sending 10k events)
- Mitigation: composite key `order_id + hour` for bulk orders

## Alternatives considered
- Random partitioning: high throughput but no ordering guarantee
- Customer_id key: useful for customer-centric analytics but loses
  per-order ordering for delivery pipelines