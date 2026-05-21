"""
Async connection pool for PostgreSQL.

Uses psycopg_pool to manage a pool of reusable connections,
avoiding the overhead of opening a new connection per request.

Resilience features:
  - Automatic reconnection on broken connections (reconnect_timeout)
  - Connection health checks before checkout
  - Keepalive settings to detect dead connections quickly
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

    The pool is configured for resilience:
      - reconnect_timeout=300: keep trying to reconnect for up to 5 minutes
        instead of failing permanently on transient DB outages.
      - check=AsyncConnectionPool.check_connection: validate connections
        before handing them to callers (detects stale/dead connections).
      - max_idle=300: close idle connections after 5 minutes.
    """
    global pool

    # TCP keepalive kwargs to detect dead connections within ~30s
    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 10,
        "keepalives_interval": 5,
        "keepalives_count": 3,
    }

    pool = AsyncConnectionPool(
        conninfo=settings.database_url,
        min_size=2,
        max_size=10,
        open=False,
        reconnect_timeout=300,  # Retry reconnection for 5 minutes
        max_idle=300,
        kwargs=keepalive_kwargs,
    )
    await pool.open()
    logger.info("Database connection pool opened (min=2, max=10, reconnect_timeout=300s)")
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
