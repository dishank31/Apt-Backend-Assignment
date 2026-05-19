"""
Application entry point for the Real-Time Orders API.

Bootstraps FastAPI with:
  - Async connection pool for PostgreSQL
  - CDC worker consuming WAL changes
  - SSE streaming endpoint for real-time client delivery
"""

import asyncio
import json
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from config import settings
from cdc_worker import CDCWorker
from database.connection import init_pool, close_pool

# ── Logging ──────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CDC Worker ───────────────────────────────────────────────────

cdc_worker = CDCWorker(
    db_url=settings.database_url,
    slot_name=settings.replication_slot_name,
)


# ── Lifespan ─────────────────────────────────────────────────────

async def lifespan(app: FastAPI):
    """Manage startup and shutdown of shared resources."""
    # Startup
    logger.info("Starting application...")
    await init_pool()
    bg_task = asyncio.create_task(cdc_worker.start_replication())
    logger.info("Application ready — accepting connections")

    yield

    # Shutdown
    logger.info("Shutting down application...")
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    await close_pool()
    logger.info("Shutdown complete")


# ── App ──────────────────────────────────────────────────────────

app = FastAPI(
    lifespan=lifespan,
    title="Real-Time Orders API",
    description="Pushes live database mutations to clients via Server-Sent Events",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    """Liveness probe with operational metrics."""
    return {
        "status": "operational",
        "cdc": cdc_worker.metrics,
    }


@app.get("/api/v1/orders/stream")
async def stream_orders(request: Request):
    """SSE endpoint — each client gets a dedicated asyncio.Queue."""
    client_queue: asyncio.Queue = asyncio.Queue()
    await cdc_worker.register_listener(client_queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    data = await asyncio.wait_for(client_queue.get(), timeout=1.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            await cdc_worker.unregister_listener(client_queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")