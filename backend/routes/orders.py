"""
REST API routes for the Orders resource.

Provides full CRUD operations:
  GET    /api/v1/orders        — List all orders
  GET    /api/v1/orders/{id}   — Get a single order
  POST   /api/v1/orders        — Create a new order
  PATCH  /api/v1/orders/{id}   — Update an existing order
  DELETE /api/v1/orders/{id}   — Delete an order

Every mutation (POST/PATCH/DELETE) triggers a WAL change that the CDC
worker picks up and broadcasts to SSE clients automatically.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException

from database.connection import get_pool
from models import OrderCreate, OrderUpdate, OrderResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orders", tags=["Orders"])


@router.get("", response_model=list[OrderResponse])
async def list_orders(limit: int = 100, offset: int = 0):
    """Retrieve all orders, most recently updated first.

    Supports pagination via `limit` and `offset` query parameters.
    """
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, customer_name, product_name, status, updated_at "
                "FROM orders ORDER BY updated_at DESC "
                "LIMIT %s OFFSET %s",
                (limit, offset),
            )
            rows = await cur.fetchall()

    return [
        OrderResponse(
            id=row[0],
            customer_name=row[1],
            product_name=row[2],
            status=row[3],
            updated_at=row[4],
        )
        for row in rows
    ]


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(order_id: int):
    """Retrieve a single order by ID."""
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, customer_name, product_name, status, updated_at "
                "FROM orders WHERE id = %s",
                (order_id,),
            )
            row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    return OrderResponse(
        id=row[0],
        customer_name=row[1],
        product_name=row[2],
        status=row[3],
        updated_at=row[4],
    )


@router.post("", response_model=OrderResponse, status_code=201)
async def create_order(payload: OrderCreate):
    """Insert a new order — triggers CDC → SSE broadcast automatically."""
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO orders (customer_name, product_name, status) "
                "VALUES (%s, %s, %s) "
                "RETURNING id, customer_name, product_name, status, updated_at",
                (payload.customer_name, payload.product_name, payload.status.value),
            )
            row = await cur.fetchone()
        await conn.commit()

    logger.info("Order created | id=%d | customer=%s", row[0], row[1])

    return OrderResponse(
        id=row[0],
        customer_name=row[1],
        product_name=row[2],
        status=row[3],
        updated_at=row[4],
    )


@router.patch("/{order_id}", response_model=OrderResponse)
async def update_order(order_id: int, payload: OrderUpdate):
    """Update an existing order — triggers CDC → SSE broadcast automatically.

    Uses PATCH semantics: only provided fields are updated.
    """
    # Build dynamic SET clause from provided fields only
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided for update")

    set_clauses = []
    values = []
    for field, value in updates.items():
        set_clauses.append(f"{field} = %s")
        values.append(value.value if hasattr(value, "value") else value)

    values.append(order_id)
    set_sql = ", ".join(set_clauses)

    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE orders SET {set_sql} "
                f"WHERE id = %s "
                f"RETURNING id, customer_name, product_name, status, updated_at",
                values,
            )
            row = await cur.fetchone()
        await conn.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    logger.info("Order updated | id=%d | fields=%s", order_id, list(updates.keys()))

    return OrderResponse(
        id=row[0],
        customer_name=row[1],
        product_name=row[2],
        status=row[3],
        updated_at=row[4],
    )


@router.delete("/{order_id}", status_code=204)
async def delete_order(order_id: int):
    """Delete an order — triggers CDC → SSE broadcast automatically."""
    pool = get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM orders WHERE id = %s RETURNING id",
                (order_id,),
            )
            row = await cur.fetchone()
        await conn.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    logger.info("Order deleted | id=%d", order_id)
