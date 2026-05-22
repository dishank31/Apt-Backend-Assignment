# API Reference

This document outlines the REST boundaries and the Server-Sent Events (SSE) stream for the Real-Time Order Tracking system.

> **Base URL:** `http://localhost:8000`  
> **Interactive Documentation:** Available at `/docs` (Swagger UI) or `/redoc` (ReDoc) when the server is running.

---

## 📡 Real-Time Stream (SSE)

### `GET /api/v1/orders/stream`
Establishes a persistent Server-Sent Events connection. The server pushes Change Data Capture (CDC) events instantly as they occur in the database.

**Headers Required:**
```http
Accept: text/event-stream
```

**Payload Schema (Event):**
The stream yields JSON payloads prefixed with `data: `.

```json
data: {
  "change": [
    {
      "kind": "insert",
      "table": "orders",
      "columnnames": ["id", "customer_name", "product_name", "status", "updated_at"],
      "columnvalues": [3, "Alice", "Widget", "pending", "2026-05-19T13:00:00"]
    }
  ]
}
```

> **Note:** To prevent proxy timeouts, the server sends a `: keep-alive` comment ping every second when idle.

**Integration Example (JavaScript):**
```javascript
const eventSource = new EventSource('http://localhost:8000/api/v1/orders/stream');

eventSource.onmessage = (event) => {
  const cdcPayload = JSON.parse(event.data);
  console.log('Database mutated:', cdcPayload);
};
```

---

## 📦 Orders Resource

### `GET /api/v1/orders`
Retrieves a paginated list of all orders, ordered by the most recently updated.

**Query Parameters:**
- `limit` (integer): Maximum orders to return (default: 100, max: 500).
- `offset` (integer): Number of orders to skip (default: 0).

**Response:** `200 OK`
```json
[
  {
    "id": 1,
    "customer_name": "Dishank Gandhi",
    "product_name": "HP Victus Gaming Laptop",
    "status": "pending",
    "updated_at": "2026-05-19T12:00:00"
  }
]
```

### `GET /api/v1/orders/{id}`
Retrieves a single order by its ID.

**Response:** `200 OK` (Returns the order object)
**Errors:** `404 Not Found` (Order ID does not exist).

### `POST /api/v1/orders`
Creates a new order in the database.

**Request Body:**
```json
{
  "customer_name": "Alice",
  "product_name": "MacBook Pro",
  "status": "pending" 
}
```
*(Note: `status` is optional and defaults to `"pending"`. `customer_name` and `product_name` have a maximum length of 100 characters. `status` must be `pending`, `shipped`, or `delivered`.)*

**Response:** `201 Created`
**Errors:** `422 Unprocessable Entity` (Failed validation: missing fields or invalid status).

### `PATCH /api/v1/orders/{id}`
Partially updates an existing order.

**Request Body:**
```json
{
  "status": "shipped"
}
```

**Response:** `200 OK` (Returns the updated order object).  
**Errors:** `404 Not Found` (Order ID does not exist), `400 Bad Request` (Empty payload).

### `DELETE /api/v1/orders/{id}`
Permanently deletes an order from the database.

**Response:** `204 No Content`  
**Errors:** `404 Not Found` (Order ID does not exist).

---

## 🩺 System Health

### `GET /health`
Liveness probe providing operational metrics for the API and CDC pipeline.

**Response:** `200 OK`
```json
{
  "status": "operational",
  "uptime_seconds": 120.5,
  "database": {
    "connected": true,
    "last_error": null
  },
  "cdc": {
    "connected": true,
    "active_listeners": 2,
    "messages_processed": 47,
    "broadcasts_sent": 12,
    "uptime_seconds": 120.5,
    "last_error": null,
    "listener_queue_depths": [0, 0]
  }
}
```
