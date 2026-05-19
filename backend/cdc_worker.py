"""
Change Data Capture (CDC) Worker.

Connects to a PostgreSQL logical replication slot (wal2json) and
streams row-level changes from the Write-Ahead Log in real time.
Changes are broadcast to all connected SSE client queues.
"""

import asyncio
import json
import logging

import psycopg
from psycopg_pool import AsyncConnectionPool

from config import settings

logger = logging.getLogger(__name__)


class CDCWorker:
    """Consumes PostgreSQL WAL changes and fans them out to SSE listeners."""

    def __init__(self, db_url: str, slot_name: str):
        self.db_url = db_url
        self.slot_name = slot_name
        self.listeners: set[asyncio.Queue] = set()
        self._messages_processed: int = 0
        self._broadcasts_sent: int = 0

    # ── Listener Management ──────────────────────────────────────

    async def register_listener(self, queue: asyncio.Queue) -> None:
        """Add a client queue to the broadcast set."""
        self.listeners.add(queue)
        logger.info(
            "Client connected | active_listeners=%d", len(self.listeners)
        )

    async def unregister_listener(self, queue: asyncio.Queue) -> None:
        """Remove a client queue from the broadcast set."""
        self.listeners.discard(queue)
        logger.info(
            "Client disconnected | active_listeners=%d", len(self.listeners)
        )

    # ── Metrics ──────────────────────────────────────────────────

    @property
    def metrics(self) -> dict:
        """Return operational metrics for observability."""
        return {
            "active_listeners": len(self.listeners),
            "messages_processed": self._messages_processed,
            "broadcasts_sent": self._broadcasts_sent,
        }

    # ── Replication Stream ───────────────────────────────────────

    async def _ensure_replication_slot(self) -> None:
        """Create the logical replication slot if it doesn't already exist."""
        conn = await psycopg.AsyncConnection.connect(
            self.db_url, autocommit=True
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                if not await cur.fetchone():
                    logger.info("Creating replication slot: %s", self.slot_name)
                    await cur.execute(
                        "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')",
                        (self.slot_name,),
                    )
        finally:
            await conn.close()

    async def _broadcast(self, payload: dict) -> None:
        """Push a parsed WAL change to every connected client queue."""
        if not self.listeners:
            return

        self._broadcasts_sent += 1
        listener_count = len(self.listeners)
        logger.info(
            "Broadcasting mutation | listeners=%d | broadcast_id=%d",
            listener_count,
            self._broadcasts_sent,
        )

        # Fan out to all client queues concurrently
        await asyncio.gather(
            *[q.put(payload) for q in self.listeners],
            return_exceptions=True,
        )

    async def start_replication(self) -> None:
        """Main loop: connect to WAL stream and consume changes forever.

        Reconnects automatically on any connection failure with a 5-second delay.
        """
        while True:
            try:
                await self._ensure_replication_slot()

                logger.info("Starting WAL replication stream...")
                rep_conn = await psycopg.AsyncConnection.connect(
                    self.db_url,
                    autocommit=True,
                )

                async with rep_conn.cursor() as cur:
                    # Poll for changes using pg_logical_slot_get_changes
                    # This is more portable than the replication protocol
                    while True:
                        await cur.execute(
                            "SELECT lsn, data FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
                            (self.slot_name,),
                        )
                        rows = await cur.fetchall()

                        for _lsn, data in rows:
                            self._messages_processed += 1
                            try:
                                payload = json.loads(data)
                                if "change" in payload:
                                    await self._broadcast(payload)
                            except json.JSONDecodeError:
                                logger.error("Failed to decode WAL payload")

                        # Small sleep to avoid busy-waiting when no changes
                        await asyncio.sleep(0.1)

            except psycopg.OperationalError as exc:
                logger.error(
                    "Replication connection lost: %s | Reconnecting in 5s...",
                    exc,
                )
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info("CDC worker shutting down gracefully")
                break
            except Exception as exc:
                logger.error(
                    "Unexpected error in CDC worker: %s | Reconnecting in 5s...",
                    exc,
                )
                await asyncio.sleep(5)