"""
Pytest fixtures for API integration tests.

Provides an async test client that bypasses the CDC worker
and uses a real (or test) database connection pool.
"""

import asyncio
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from main import app
from database.connection import init_pool, close_pool


@pytest.fixture
def event_loop():
    """Override the default event loop for async fixtures."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def client():
    """Provide an async HTTP client bound to the FastAPI app.

    Initializes the DB pool before tests and tears it down after.
    Requires a running PostgreSQL instance (use docker-compose).
    """
    await init_pool()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await close_pool()
