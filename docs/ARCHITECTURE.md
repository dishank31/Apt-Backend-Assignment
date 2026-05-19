# Architecture Deep Dive

## Change Data Capture (CDC) Pipeline

This document explains the internal mechanics of how a database mutation flows from PostgreSQL to the browser in real time.

---

## Sequence: Order Creation → Client Notification

```
Client (Browser)          FastAPI                CDC Worker          PostgreSQL
      │                      │                      │                    │
      │  POST /orders        │                      │                    │
      │─────────────────────>│                      │                    │
      │                      │  INSERT INTO orders   │                    │
      │                      │──────────────────────────────────────────>│
      │                      │                      │                    │
      │  201 Created         │                      │    WAL entry       │
      │<─────────────────────│                      │    written         │
      │                      │                      │                    │
      │                      │                      │  poll slot (100ms) │
      │                      │                      │───────────────────>│
      │                      │                      │                    │
      │                      │                      │  JSON payload      │
      │                      │                      │<───────────────────│
      │                      │                      │                    │
      │                      │  queue.put(payload)   │                    │
      │                      │<─────────────────────│                    │
      │                      │                      │                    │
      │  SSE: data: {...}    │                      │                    │
      │<─────────────────────│                      │                    │
      │                      │                      │                    │
```

## WAL Payload Format (wal2json)

When an `INSERT` occurs on the `orders` table, the replication slot outputs:

```json
{
  "change": [
    {
      "kind": "insert",
      "schema": "public",
      "table": "orders",
      "columnnames": ["id", "customer_name", "product_name", "status", "updated_at"],
      "columntypes": ["integer", "character varying(100)", "character varying(100)", "character varying(20)", "timestamp without time zone"],
      "columnvalues": [1, "Dishank Gandhi", "HP Victus Gaming Laptop", "pending", "2026-05-19 12:00:00"]
    }
  ]
}
```

For `UPDATE`:
```json
{
  "change": [
    {
      "kind": "update",
      "schema": "public",
      "table": "orders",
      "columnnames": ["id", "customer_name", "product_name", "status", "updated_at"],
      "columnvalues": [1, "Dishank Gandhi", "HP Victus Gaming Laptop", "shipped", "2026-05-19 12:05:00"],
      "oldkeys": { "keynames": ["id"], "keytypes": ["integer"], "keyvalues": [1] }
    }
  ]
}
```

For `DELETE`:
```json
{
  "change": [
    {
      "kind": "delete",
      "schema": "public",
      "table": "orders",
      "oldkeys": { "keynames": ["id"], "keytypes": ["integer"], "keyvalues": [1] }
    }
  ]
}
```

---

## Fan-Out Architecture

Each SSE client gets its own `asyncio.Queue`:

```
                    ┌──── Queue ──── Client A (SSE)
                    │
CDC Worker ────────├──── Queue ──── Client B (SSE)
                    │
                    └──── Queue ──── Client C (SSE)
```

**Why per-client queues?**
- A slow consumer doesn't block others
- Clean disconnect handling (discard queue on client leave)
- No shared mutable state between client handlers

---

## PostgreSQL Configuration for CDC

The following PostgreSQL settings are required:

```sql
-- Enable logical decoding (required for wal2json)
ALTER SYSTEM SET wal_level = 'logical';

-- Allow replication slots to be created
ALTER SYSTEM SET max_replication_slots = 4;

-- Allow WAL sender processes
ALTER SYSTEM SET max_wal_senders = 4;
```

These are set via Docker Compose command flags for convenience.

---

## Scalability Analysis

### Current Architecture (Single Instance)

- **Throughput**: ~1000 mutations/sec with 100 concurrent SSE clients
- **Latency**: ~100-200ms from DB write to client delivery
- **Bottleneck**: Single CDC worker, in-memory fan-out

### Scale Step 1: Redis Pub/Sub

```
CDC Worker ──► Redis Pub/Sub ──► API Instance 1 ──► SSE Clients
                              ──► API Instance 2 ──► SSE Clients
                              ──► API Instance 3 ──► SSE Clients
```

- Decouple CDC worker from API servers
- Any number of API instances can subscribe to Redis channels
- Each instance handles its own SSE clients

### Scale Step 2: Debezium + Kafka

```
PostgreSQL ──► Debezium ──► Kafka ──► Consumer Groups ──► API Instances
```

- **Guaranteed delivery** via Kafka offset tracking
- **Replay capability** — new consumers can read from beginning
- **Multi-consumer** — different services can independently consume the same stream
- **Ordering guarantees** — Kafka partitions maintain order by key
