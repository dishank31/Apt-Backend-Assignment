import asyncio
import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingHttpResponse
from cdc_worker import CDCWorker

# Database Configuration (Matches Docker Compose credentials)
DB_URL = "postgresql://postgres:password@localhost:5432/orders_db"
cdc_worker = CDCWorker(db_url=DB_URL, slot_name="orders_realtime_slot")

# Lifespan manager to handle the background worker thread
async def lifespan(app: FastAPI):
    bg_task = asyncio.create_task(cdc_worker.start_replication())
    yield
    bg_task.cancel()

app = FastAPI(lifespan=lifespan, title="Real-Time Orders API")

# Enable CORS for local frontend testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "System Operational", "active_connections": len(cdc_worker.listeners)}

@app.get("/api/v1/orders/stream")
async def stream_orders(request: Request):
    """Exposes a highly performant, unidirectional Server-Sent Events stream."""
    client_queue = asyncio.Queue()
    await cdc_worker.register_listener(client_queue)

    async def event_generator():
        try:
            while True:
                # Break connection if client closes their browser
                if await request.is_disconnected():
                    break
                
                try:
                    # Wait for database mutation with a 1-second timeout
                    data = await asyncio.wait_for(client_queue.get(), timeout=1.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    # Send an empty comment pulse to maintain HTTP keep-alive
                    yield ": keep-alive\n\n"
        finally:
            # Cleanup when client leaves
            await cdc_worker.unregister_listener(client_queue)

    return StreamingHttpResponse(event_generator(), media_type="text/event-stream")