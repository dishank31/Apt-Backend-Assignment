"""
Pydantic models for request validation and response serialization.

Defines the data contracts for the Orders REST API.
"""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    """Valid order statuses aligned with the database CHECK constraint."""

    PENDING = "pending"
    SHIPPED = "shipped"
    DELIVERED = "delivered"


class OrderCreate(BaseModel):
    """Payload for creating a new order."""

    customer_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["Dishank Gandhi"],
    )
    product_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["HP Victus Gaming Laptop"],
    )
    status: OrderStatus = Field(
        default=OrderStatus.PENDING,
        description="Initial order status",
    )


class OrderUpdate(BaseModel):
    """Payload for updating an existing order.

    All fields are optional — only provided fields are updated (PATCH semantics).
    """

    customer_name: str | None = Field(default=None, min_length=1, max_length=100)
    product_name: str | None = Field(default=None, min_length=1, max_length=100)
    status: OrderStatus | None = None


class OrderResponse(BaseModel):
    """Serialized order returned from the API."""

    id: int
    customer_name: str
    product_name: str
    status: OrderStatus
    updated_at: datetime

    model_config = {"from_attributes": True}
