import asyncio
import json
import psycopg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CDCWorker:
    def __init__(self, db_url: str, slot_name: str):
        self.db_url = db_url
        self.slot_name = slot_name
        self.listeners = set()

    async def register_listener(self, queue: asyncio.Queue):
        self.listeners.add(queue)

    async def unregister_listener(self, queue: asyncio.Queue):
        self.listeners.discard(queue)

    async def start_replication(self):
        """Connects to PostgreSQL logical replication slot and streams WAL changes."""
        while True:
            try:
                # 1. Standard connection to ensure replication slot exists
                conn = await psycopg.AsyncConnection.connect(self.db_url, autocommit=True)
                async with conn.cursor() as cur:
                    # Check if slot exists, if not create it using wal2json
                    await cur.execute(
                        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s", 
                        (self.slot_name,)
                    )
                    if not await cur.fetchone():
                        logger.info(f"Creating logical replication slot: {self.slot_name}")
                        await cur.execute(
                            f"SELECT * FROM pg_create_logical_replication_slot('{self.slot_name}', 'wal2json')"
                        )
                await conn.close()

                # 2. Connect in special Replication mode
                logger.info("Starting continuous WAL replication stream...")
                async with psycopg.AsyncReplicationConnection.connect(self.db_url) as rep_conn:
                    async with rep_conn.cursor() as rep_cur:
                        await rep_cur.start_replication(
                            slot_name=self.slot_name,
                            options={"include-pk": "true", "include-xids": "false"}
                        )
                        
                        while True:
                            msg = await rep_conn.receive_message()
                            if not msg:
                                continue
                            
                            # Parse the JSON payload from the WAL
                            payload_str = msg.payload.decode('utf-8')
                            try:
                                payload = json.loads(payload_str)
                                
                                # Broadcast to all active client SSE queues
                                if self.listeners and "change" in payload:
                                    logger.info(f"Broadcasting DB mutation to {len(self.listeners)} clients.")
                                    await asyncio.gather(*[q.put(payload) for q in self.listeners])
                                    
                            except json.JSONDecodeError:
                                logger.error("Failed to decode WAL JSON payload")

                            # Acknowledge log processing to clear WAL space in Postgres
                            rep_conn.send_feedback(write_lsn=msg.wal_end)
                            
            except (psycopg.OperationalError, Exception) as e:
                logger.error(f"Replication stream interrupted: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)