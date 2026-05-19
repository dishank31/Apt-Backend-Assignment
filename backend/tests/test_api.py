"""
Integration tests for the Orders REST API.

Tests cover:
  - CRUD lifecycle (create → read → update → delete)
  - Input validation (missing fields, invalid status)
  - 404 handling for non-existent resources
  - Health endpoint

Requires: PostgreSQL running (docker-compose up postgres)
Run with: pytest tests/ -v
"""

import pytest


@pytest.mark.asyncio
class TestHealthEndpoint:
    """Tests for GET /health."""

    async def test_health_returns_operational(self, client):
        res = await client.get("/health")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "operational"
        assert "cdc" in body


@pytest.mark.asyncio
class TestOrdersCRUD:
    """Tests for the full CRUD lifecycle on /api/v1/orders."""

    created_order_id: int = None

    async def test_create_order(self, client):
        res = await client.post("/api/v1/orders", json={
            "customer_name": "Test User",
            "product_name": "Test Product",
            "status": "pending",
        })
        assert res.status_code == 201
        body = res.json()
        assert body["customer_name"] == "Test User"
        assert body["product_name"] == "Test Product"
        assert body["status"] == "pending"
        assert "id" in body
        TestOrdersCRUD.created_order_id = body["id"]

    async def test_list_orders_contains_created(self, client):
        res = await client.get("/api/v1/orders")
        assert res.status_code == 200
        ids = [o["id"] for o in res.json()]
        assert TestOrdersCRUD.created_order_id in ids

    async def test_get_single_order(self, client):
        oid = TestOrdersCRUD.created_order_id
        res = await client.get(f"/api/v1/orders/{oid}")
        assert res.status_code == 200
        assert res.json()["id"] == oid

    async def test_update_order_status(self, client):
        oid = TestOrdersCRUD.created_order_id
        res = await client.patch(f"/api/v1/orders/{oid}", json={"status": "shipped"})
        assert res.status_code == 200
        assert res.json()["status"] == "shipped"

    async def test_delete_order(self, client):
        oid = TestOrdersCRUD.created_order_id
        res = await client.delete(f"/api/v1/orders/{oid}")
        assert res.status_code == 204

    async def test_get_deleted_order_returns_404(self, client):
        oid = TestOrdersCRUD.created_order_id
        res = await client.get(f"/api/v1/orders/{oid}")
        assert res.status_code == 404


@pytest.mark.asyncio
class TestOrderValidation:
    """Tests for input validation on the Orders API."""

    async def test_create_missing_customer_name(self, client):
        res = await client.post("/api/v1/orders", json={
            "product_name": "Widget",
        })
        assert res.status_code == 422

    async def test_create_missing_product_name(self, client):
        res = await client.post("/api/v1/orders", json={
            "customer_name": "Alice",
        })
        assert res.status_code == 422

    async def test_create_invalid_status(self, client):
        res = await client.post("/api/v1/orders", json={
            "customer_name": "Alice",
            "product_name": "Widget",
            "status": "invalid_status",
        })
        assert res.status_code == 422

    async def test_update_empty_body(self, client):
        # First create an order to update
        create_res = await client.post("/api/v1/orders", json={
            "customer_name": "Temp",
            "product_name": "Temp Product",
        })
        oid = create_res.json()["id"]
        res = await client.patch(f"/api/v1/orders/{oid}", json={})
        assert res.status_code == 400
        # Cleanup
        await client.delete(f"/api/v1/orders/{oid}")

    async def test_get_nonexistent_order(self, client):
        res = await client.get("/api/v1/orders/999999")
        assert res.status_code == 404

    async def test_delete_nonexistent_order(self, client):
        res = await client.delete("/api/v1/orders/999999")
        assert res.status_code == 404
