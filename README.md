# Real-Time Order Tracking System

> **A production-grade system where connected clients receive instant updates whenever database records change — without polling.**

Built with **PostgreSQL WAL-based Change Data Capture** + **FastAPI** + **Server-Sent Events**.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                              │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │ Browser Dashboard│         │ REST Client      │              │
│  │ (SSE EventSource)│         │ (cURL / Postman) │              │
│  └────────┬─────────┘         └────────┬─────────┘              │
│           │ auto-reconnect              │ HTTP                   │
└───────────┼─────────────────────────────┼────────────────────────┘
            │                             │
┌───────────┼─────────────────────────────┼────────────────────────┐
│           ▼                             ▼     APPLICATION LAYER  │
│  ┌─────────────────┐         ┌──────────────────┐               │
│  │ SSE Stream      │         │ REST API         │               │
│  │ /orders/stream  │◄────┐   │ /api/v1/orders   │               │
│  └─────────────────┘     │   └────────┬─────────┘               │
│                          │            │                          │
│  ┌───────────────────────┴──┐         │ SQL                     │
│  │ CDC Worker               │         │                          │
│  │ (Fan-out via asyncio.Q)  │         │                          │
│  └──────────┬───────────────┘         │                          │
└─────────────┼─────────────────────────┼──────────────────────────┘
              │ WAL consume             │
┌─────────────┼─────────────────────────┼──────────────────────────┐
│             ▼                         ▼         DATA LAYER       │
│  ┌────────────────────────────────────────────┐                  │
│  │ PostgreSQL 15 (wal_level=logical)          │                  │
│  │ ┌──────────┐  ┌─────────────────────────┐  │                  │
│  │ │ orders   │  │ WAL → wal2json decoder  │  │                  │
│  │ │ table    │──│ (logical replication)    │  │                  │
│  │ └──────────┘  └─────────────────────────┘  │                  │
│  └────────────────────────────────────────────┘                  │
└──────────────────────────────────────────────────────────────────┘
```

## How It Works

1. **A mutation occurs** — An `INSERT`, `UPDATE`, or `DELETE` hits the `orders` table (via API, psql, migration — any source).
2. **PostgreSQL writes to WAL** — The Write-Ahead Log captures the change at the engine level.
3. **wal2json decodes it** — The logical replication slot decodes binary WAL into structured JSON.
4. **CDC Worker consumes** — Our background worker polls the replication slot and receives the JSON payload.
5. **Fan-out broadcast** — The worker pushes the change into every connected client's `asyncio.Queue`.
6. **SSE delivers** — Each client's dedicated Server-Sent Events stream pushes the data to the browser instantly.

**Key insight**: Because we capture changes at the WAL level, even raw SQL executed directly in `psql` triggers client updates. The application code has zero coupling to the notification mechanism.

---

## Tech Stack & Why

| Component | Technology | Rationale |
|:----------|:-----------|:----------|
| **Database** | PostgreSQL 15 | Native logical replication with `wal2json` — no external CDC tools needed |
| **Backend** | FastAPI (Python) | Async-native, auto-generated OpenAPI docs, Pydantic validation |
| **Real-time transport** | Server-Sent Events (SSE) | Browser-native, auto-reconnects, no library needed, HTTP/2 compatible |
| **CDC mechanism** | WAL logical decoding | Captures ALL changes at engine level — zero application coupling |
| **Connection pooling** | psycopg_pool | Reuses DB connections, prevents connection exhaustion |
| **Configuration** | Pydantic Settings | Type-safe env var loading with `.env` file support |
| **Containerization** | Docker + Compose | One-command setup, consistent environments |

### Why SSE over WebSockets?

This is a **unidirectional** data flow (server → client). SSE is the right tool because:
- Built into every browser — `EventSource` API, no library needed
- **Automatic reconnection** with configurable retry
- Works over standard HTTP — proxy and CDN friendly
- Simpler server implementation than WebSocket upgrade handshake

### Why WAL-based CDC over Triggers + NOTIFY?

| Aspect | LISTEN/NOTIFY | WAL-based CDC |
|:-------|:-------------|:-------------|
| Captures raw SQL changes | ✅ (via trigger) | ✅ (always) |
| Requires triggers | ✅ Yes | ❌ No |
| 8KB payload limit | ✅ Yes | ❌ No |
| Survives server restart | ❌ No (ephemeral) | ✅ Yes (slot tracks position) |
| Performance impact | Medium (in-transaction) | Low (async from WAL) |

### Architecture & Scalability Considerations
This system was built with production scalability and clean code principles in mind:
- **Asyncio Fan-out**: Pushing updates to 10,000+ connected clients sequentially would block the Python event loop. The `CDCWorker` uses `asyncio.gather()` to push updates to all active queues concurrently.
- **Connection Efficiency**: SSE requires long-lived open sockets. To scale to massive concurrency, Uvicorn worker counts and OS `ulimit` (max open files) configurations must be tuned.
- **Microservice Ready**: Currently, the system uses an in-memory `asyncio.Queue` for fan-out. For massive horizontal scaling across multiple FastAPI instances, the `_broadcast()` method can easily be swapped to publish to a Redis Pub/Sub channel.
- **Separation of Concerns**: HTML, CSS, and JS are decoupled. The CDC pipeline is decoupled from the REST API endpoints. Business logic is independent of the notification transport layer.

---

## Quick Start

### Prerequisites
- **Docker** and **Docker Compose** installed
- **Git**

### One-Command Launch

```bash
# Clone the repository
git clone https://github.com/dishank31/Apt-Backend-Assignment.git
cd Apt-Backend-Assignment/backend

# Start all services (PostgreSQL + Backend + Frontend)
docker-compose up --build
```

| Service | URL |
|:--------|:----|
| **Dashboard** | http://localhost:3000 |
| **API** | http://localhost:8000 |
| **API Docs** | http://localhost:8000/docs |

### Manual Setup (Without Docker)

```bash
# 1. Start PostgreSQL with logical replication enabled
#    (Ensure wal_level=logical in postgresql.conf)

# 2. Create the database and tables
psql -U postgres -f backend/database/init.sql

# 3. Install Python dependencies
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your database credentials

# 5. Start the backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 6. Open the frontend
# Just open frontend/index.html in your browser
```

---

## API Reference

### Health Check
```bash
GET /health
# Response: { "status": "operational", "cdc": { "active_listeners": 2, ... } }
```

### List Orders
```bash
GET /api/v1/orders
```

### Create Order
```bash
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name": "Dishank Gandhi", "product_name": "MacBook Pro", "status": "pending"}'
```

### Update Order Status
```bash
curl -X PATCH http://localhost:8000/api/v1/orders/1 \
  -H "Content-Type: application/json" \
  -d '{"status": "shipped"}'
```

### Delete Order
```bash
curl -X DELETE http://localhost:8000/api/v1/orders/1
```

### SSE Stream (Real-Time)
```bash
curl -N http://localhost:8000/api/v1/orders/stream
# Receives: data: {"change": [{"kind": "insert", ...}]}
```

Full API documentation with interactive playground available at `http://localhost:8000/docs` (auto-generated by FastAPI).

---

## Testing the Real-Time Pipeline

1. **Open the dashboard** at `http://localhost:3000`
2. **Create an order** using the inline form — watch it appear instantly
3. **Update status** using the Ship/Deliver buttons — watch the badge change in real-time
4. **Delete an order** — watch it disappear immediately
5. **Open multiple browser tabs** — all receive updates simultaneously
6. **Use psql directly** to prove WAL-level capture:
   ```sql
   INSERT INTO orders (customer_name, product_name, status)
   VALUES ('Direct SQL User', 'Test Product', 'pending');
   ```
   The dashboard updates even though the application layer was bypassed.

---

## Running Tests

```bash
# Ensure PostgreSQL is running (docker-compose up postgres)
cd backend
pytest tests/ -v
```

---

## Project Structure

```
├── README.md
├── docs/
│   ├── ARCHITECTURE.md         # Deep-dive into CDC pipeline
│   └── API.md                  # Complete API reference
├── backend/
│   ├── main.py                 # FastAPI app entry point
│   ├── config.py               # Pydantic settings (env vars)
│   ├── cdc_worker.py           # WAL consumer + fan-out broadcaster
│   ├── models.py               # Pydantic request/response schemas
│   ├── routes/
│   │   └── orders.py           # REST CRUD endpoints
│   ├── database/
│   │   ├── connection.py       # Async connection pool
│   │   └── init.sql            # Schema + seed data
│   ├── tests/
│   │   ├── conftest.py         # Pytest fixtures
│   │   └── test_api.py         # API integration tests
│   ├── Dockerfile              # Multi-stage production image
│   ├── docker-compose.yml      # Full stack orchestration
│   └── requirements.txt        # Python dependencies
└── frontend/
    └── index.html              # Real-time dashboard (SSE client)
```

---

## Design Decisions & Trade-offs

### What I Chose and Why

1. **Polling-based slot consumption** over replication protocol — More portable, doesn't require special connection modes, simpler error handling. The 100ms poll interval provides near-real-time delivery.

2. **In-memory fan-out** (asyncio.Queue per client) — Zero external dependencies. Each client gets an isolated queue, so a slow consumer doesn't block others.

3. **Single-file frontend** — No build tools, no npm, no framework. Opens in any browser, zero setup friction for evaluators.

4. **Pydantic Settings** for config — Type-safe, validates on startup, supports `.env` files. Catches misconfiguration before the first request.

### Known Limitations

- **Single-instance CDC worker** — Only one process can consume a replication slot. For horizontal scaling, add Redis Pub/Sub between the CDC worker and API instances.
- **In-memory queues** — Events are lost on server restart. For guaranteed delivery, persist events to a message queue.
- **No authentication** — Out of scope for this assignment, but production would require JWT-based auth on both REST and SSE endpoints.

---

## Scaling Considerations

```
Current (This Project)          Scale Step 1                Scale Step 2
─────────────────────          ──────────────              ──────────────
Single FastAPI instance    →   Multiple API instances   →  Microservices
In-memory fan-out          →   Redis Pub/Sub            →  Kafka topics
wal2json polling           →   wal2json polling         →  Debezium CDC
1 replication slot         →   1 slot → Redis           →  Kafka Connect
```

| Bottleneck | Solution |
|:-----------|:---------|
| Multiple API servers need same events | Redis Pub/Sub as broadcast layer |
| Slot can only have 1 consumer | CDC worker publishes to Redis; APIs subscribe |
| Need guaranteed delivery | Replace wal2json with Debezium → Kafka |
| High-volume writes | Debezium reads WAL asynchronously, zero DB impact |

---

## Author

**Dishank Gandhi**

Built as part of the APT (Atypical Technologies) Backend Engineering Assignment.
