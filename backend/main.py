"""
Application entry point for the Real-Time Orders API.

Bootstraps FastAPI with the CDC worker, CORS middleware,
and the SSE streaming endpoint.
"""

import asyncio
import json

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from config import settings
from cdc_worker import CDCWorker

# Initialize the CDC worker with configuration
cdc_worker = CDCWorker(
    db_url=settings.database_url,
    slot_name=settings.replication_slot_name,
)


async def lifespan(app: FastAPI):
    """Manage the CDC background worker lifecycle."""
    bg_task = asyncio.create_task(cdc_worker.start_replication())
    yield
    bg_task.cancel()


app = FastAPI(
    lifespan=lifespan,
    title="Real-Time Orders API",
    description="Pushes live database mutations to clients via SSE",
    version="1.0.0",
)

# CORS — allow the frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Liveness probe with active listener count."""
    return {
        "status": "operational",
        "active_connections": len(cdc_worker.listeners),
    }


@app.get("/api/v1/orders/stream")
async def stream_orders(request: Request):
    """Server-Sent Events endpoint — pushes CDC mutations to the browser."""
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