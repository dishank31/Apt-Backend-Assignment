"""
Async connection pool for PostgreSQL.

Uses psycopg_pool to manage a pool of reusable connections,
avoiding the overhead of opening a new connection per request.
"""

import logging

from psycopg_pool import AsyncConnectionPool

from config import settings

logger = logging.getLogger(__name__)

# Module-level pool instance — initialized at app startup
pool: AsyncConnectionPool | None = None


async def init_pool() -> AsyncConnectionPool:
    """Create and open the async connection pool.

    Called once during FastAPI lifespan startup.
    """
    global pool
    pool = AsyncConnectionPool(
        conninfo=settings.database_url,
        min_size=2,
        max_size=10,
        open=False,
    )
    await pool.open()
    logger.info("Database connection pool opened (min=2, max=10)")
    return pool


async def close_pool() -> None:
    """Gracefully close the connection pool.

    Called during FastAPI lifespan shutdown.
    """
    global pool
    if pool:
        await pool.close()
        logger.info("Database connection pool closed")
        pool = None


def get_pool() -> AsyncConnectionPool:
    """Return the active connection pool.

    Raises:
        RuntimeError: If pool has not been initialized.
    """
    if pool is None:
        raise RuntimeError("Connection pool is not initialized. Call init_pool() first.")
    return pool
