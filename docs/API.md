# API Reference

Base URL: `http://localhost:8000`

---

## Health Check

```
GET /health
```

**Response** `200 OK`
```json
{
  "status": "operational",
  "cdc": {
    "active_listeners": 2,
    "messages_processed": 47,
    "broadcasts_sent": 12
  }
}
```

---

## Orders

### List All Orders

```
GET /api/v1/orders
```

**Response** `200 OK`
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

---

### Get Single Order

```
GET /api/v1/orders/{id}
```

**Response** `200 OK` — Order object
**Response** `404 Not Found` — `{"detail": "Order 99 not found"}`

---

### Create Order

```
POST /api/v1/orders
Content-Type: application/json

{
  "customer_name": "Dishank Gandhi",
  "product_name": "MacBook Pro",
  "status": "pending"     // optional, defaults to "pending"
}
```

**Response** `201 Created`
```json
{
  "id": 2,
  "customer_name": "Dishank Gandhi",
  "product_name": "MacBook Pro",
  "status": "pending",
  "updated_at": "2026-05-19T12:30:00"
}
```

**Validation Errors** `422 Unprocessable Entity`
- `customer_name`: required, 1-100 chars
- `product_name`: required, 1-100 chars
- `status`: must be `pending`, `shipped`, or `delivered`

---

### Update Order (Partial)

```
PATCH /api/v1/orders/{id}
Content-Type: application/json

{
  "status": "shipped"
}
```

Only provided fields are updated (PATCH semantics).

**Response** `200 OK` — Updated order object
**Response** `400 Bad Request` — Empty body
**Response** `404 Not Found` — Order doesn't exist

---

### Delete Order

```
DELETE /api/v1/orders/{id}
```

**Response** `204 No Content`
**Response** `404 Not Found` — Order doesn't exist

---

## SSE Stream (Real-Time)

```
GET /api/v1/orders/stream
Accept: text/event-stream
```

Establishes a long-lived Server-Sent Events connection. The server pushes CDC events as they occur.

**Event Format:**
```
data: {"change":[{"kind":"insert","table":"orders","columnnames":["id","customer_name","product_name","status","updated_at"],"columnvalues":[3,"Alice","Widget","pending","2026-05-19T13:00:00"]}]}
```

**Keep-alive** (sent every ~1 second when idle):
```
: keep-alive
```

### Browser Usage
```javascript
const es = new EventSource('http://localhost:8000/api/v1/orders/stream');
es.onmessage = (event) => {
  const payload = JSON.parse(event.data);
  console.log('CDC event:', payload);
};
```

### cURL Usage
```bash
curl -N http://localhost:8000/api/v1/orders/stream
```

---

## Interactive Docs

FastAPI auto-generates an interactive API playground:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
