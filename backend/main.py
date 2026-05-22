"""
Application entry point for the Real-Time Orders API.

Bootstraps FastAPI with:
  - Async connection pool for PostgreSQL
  - CDC worker consuming WAL changes
  - SSE streaming endpoint for real-time client delivery

Resilience features:
  - Bounded client queues (maxsize=256) to prevent OOM
  - Guaranteed listener cleanup on client disconnect
  - Enhanced /health endpoint with connection diagnostics
"""

import asyncio
import json
import logging
import sys
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Windows-specific event loop policy for psycopg compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from config import settings
from cdc_worker import CDCWorker
from database.connection import init_pool, close_pool, get_pool
from routes.orders import router as orders_router

# -- Logging --

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Suppress the noisy uvicorn ASGI error when SSE clients disconnect.
# This is a known Starlette/uvicorn issue: when a streaming response
# ends because the client disconnected, Starlette tries to send a
# final empty body which raises ClientDisconnected. It is harmless.
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)

# -- CDC Worker --

cdc_worker = CDCWorker(
    db_url=settings.database_url,
    slot_name=settings.replication_slot_name,
)

_server_start_time: float = time.monotonic()


# -- Lifespan --

async def lifespan(app: FastAPI):
    """Manage startup and shutdown of shared resources."""
    global _server_start_time
    _server_start_time = time.monotonic()

    logger.info("Starting application...")
    await init_pool()
    bg_task = asyncio.create_task(cdc_worker.start_replication())
    logger.info("Application ready - accepting connections")

    yield

    logger.info("Shutting down application...")
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    await close_pool()
    logger.info("Shutdown complete")


# -- App --

app = FastAPI(
    lifespan=lifespan,
    title="Real-Time Orders API",
    description="Pushes live database mutations to clients via Server-Sent Events",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- Endpoints --

async def _database_health() -> dict:
    """Verify that the API can check out a connection and query PostgreSQL."""
    try:
        pool = get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
        return {"connected": True, "last_error": None}
    except Exception as exc:
        logger.warning("Database health check failed: %s", exc)
        return {"connected": False, "last_error": str(exc)}


@app.get("/health")
async def health_check():
    """Liveness probe with detailed operational metrics.

    Returns API uptime, database reachability, CDC connection status,
    active listener count, and listener queue depths.
    """
    try:
        database = await asyncio.wait_for(_database_health(), timeout=2.0)
    except asyncio.TimeoutError:
        database = {"connected": False, "last_error": "Database health check timed out"}

    return {
        "status": "operational",
        "uptime_seconds": round(time.monotonic() - _server_start_time, 1),
        "database": database,
        "cdc": cdc_worker.metrics,
    }


CLIENT_QUEUE_MAXSIZE = 256


@app.get("/api/v1/orders/stream")
async def stream_orders(request: Request):
    """SSE endpoint - each client gets a dedicated bounded asyncio.Queue.

    The finally block guarantees the listener is unregistered when the
    client disconnects (browser tab closed, network drop, etc.).
    """
    client_queue: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)
    await cdc_worker.register_listener(client_queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    logger.info("Client disconnect detected via is_disconnected()")
                    break

                try:
                    data = await asyncio.wait_for(client_queue.get(), timeout=1.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            # Starlette cancels the generator on client disconnect
            pass
        except Exception as exc:
            logger.error("Unexpected error in SSE stream: %s", exc)
        finally:
            await cdc_worker.unregister_listener(client_queue)
            logger.info(
                "SSE client cleaned up | remaining_listeners=%d",
                len(cdc_worker.listeners),
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# Mount REST API routes after fixed endpoints so /api/v1/orders/stream is not
# captured by the dynamic /api/v1/orders/{order_id} route.
app.include_router(orders_router)
