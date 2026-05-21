"""
Change Data Capture (CDC) Worker.

Connects to a PostgreSQL logical replication slot (wal2json) and
streams row-level changes from the Write-Ahead Log in real time.
Changes are broadcast to all connected SSE client queues.

Resilience features:
  - Uses application_name for precise stale connection identification
  - Terminates previous CDC connections before starting
  - Exponential backoff on connection failures (2s -> 30s cap)
  - Slow-client eviction via bounded queue overflow detection
"""

import asyncio
import json
import logging
import os
import time

import psycopg

from config import settings

logger = logging.getLogger(__name__)

# Unique identifier for this process's CDC connection
CDC_APP_NAME = f"cdc_worker_{os.getpid()}"


class CDCWorker:
    """Consumes PostgreSQL WAL changes and fans them out to SSE listeners."""

    def __init__(self, db_url: str, slot_name: str):
        self.db_url = db_url
        self.slot_name = slot_name
        self.listeners: set[asyncio.Queue] = set()
        self._messages_processed: int = 0
        self._broadcasts_sent: int = 0
        self._connected: bool = False
        self._started_at: float = time.monotonic()
        self._last_error: str | None = None

    # -- Listener Management --

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

    # -- Metrics --

    @property
    def metrics(self) -> dict:
        """Return operational metrics for observability."""
        return {
            "connected": self._connected,
            "active_listeners": len(self.listeners),
            "messages_processed": self._messages_processed,
            "broadcasts_sent": self._broadcasts_sent,
            "uptime_seconds": round(time.monotonic() - self._started_at, 1),
            "last_error": self._last_error,
            "listener_queue_depths": [q.qsize() for q in self.listeners],
        }

    # -- Slot Setup --

    async def _initial_slot_setup(self) -> None:
        """One-time setup: terminate ALL previous CDC workers and ensure slot.

        Uses a temporary connection that is immediately closed, so it
        doesn't interfere with the main polling connection.
        """
        conn = await psycopg.AsyncConnection.connect(
            self.db_url, autocommit=True,
        )
        try:
            async with conn.cursor() as cur:
                # Terminate ALL previous CDC worker connections (from crashed/reloaded processes)
                await cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND pid != pg_backend_pid() "
                    "AND application_name LIKE %s",
                    ("cdc_worker_%",),
                )
                terminated = cur.rowcount
                if terminated:
                    logger.info("Terminated %d previous CDC worker connection(s)", terminated)
                    await asyncio.sleep(1)  # Let PostgreSQL release them

                # Also terminate any connections holding the slot
                await cur.execute(
                    "SELECT active_pid FROM pg_replication_slots "
                    "WHERE slot_name = %s AND active_pid IS NOT NULL",
                    (self.slot_name,),
                )
                row = await cur.fetchone()
                if row and row[0]:
                    logger.info("Terminating active slot holder PID=%d", row[0])
                    await cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
                    await asyncio.sleep(1)

                # Create slot if it doesn't exist
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

    # -- Broadcast --

    async def _broadcast(self, payload: dict) -> None:
        """Push a parsed WAL change to every connected client queue."""
        if not self.listeners:
            return

        self._broadcasts_sent += 1
        dead_queues: list[asyncio.Queue] = []

        for q in self.listeners:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead_queues.append(q)

        if dead_queues:
            for q in dead_queues:
                self.listeners.discard(q)
            logger.warning(
                "Evicted %d slow client(s) -- queue overflow | remaining=%d",
                len(dead_queues), len(self.listeners),
            )

    # -- Main Replication Loop --

    async def start_replication(self) -> None:
        """Main loop: connect to WAL stream and consume changes forever.

        Flow:
          1. Initial setup: terminate old CDC workers, create slot
          2. Connect with unique application_name for identification
          3. Poll pg_logical_slot_get_changes in a tight loop
          4. On failure: close connection, backoff, reconnect

        The application_name allows precise identification and cleanup
        of this process's connection on future --reload cycles.
        """
        backoff = 2
        max_backoff = 30
        was_disconnected = False
        conn = None

        # One-time setup: clean slate
        while True:
            try:
                await self._initial_slot_setup()
                break
            except Exception as exc:
                logger.error("Initial slot setup failed: %s | Retrying in %ds...", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        backoff = 2  # Reset after setup

        while True:
            try:
                # Connect with application_name for identification
                conn = await psycopg.AsyncConnection.connect(
                    self.db_url,
                    autocommit=True,
                    application_name=CDC_APP_NAME,
                )

                logger.info("Starting WAL replication stream...")

                if was_disconnected:
                    logger.info("Replication stream re-established after outage")
                    was_disconnected = False

                backoff = 2
                self._connected = True
                self._last_error = None

                async with conn.cursor() as cur:
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
                                logger.debug("WAL payload received: %s", json.dumps(payload))
                                if "change" in payload:
                                    await self._broadcast(payload)
                            except json.JSONDecodeError:
                                logger.error("Failed to decode WAL payload")

                        await asyncio.sleep(0.1)

            except psycopg.OperationalError as exc:
                self._connected = False
                was_disconnected = True
                self._last_error = str(exc)
                logger.error(
                    "Replication stream interrupted: %s | Reconnecting in %ds...",
                    exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            except asyncio.CancelledError:
                logger.info("CDC worker shutting down gracefully")
                break
            except Exception as exc:
                self._connected = False
                was_disconnected = True
                self._last_error = str(exc)
                logger.error(
                    "Unexpected error in CDC worker: %s | Reconnecting in %ds...",
                    exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            finally:
                if conn is not None:
                    try:
                        if not conn.closed:
                            await conn.close()
                    except Exception:
                        pass
                    conn = None
