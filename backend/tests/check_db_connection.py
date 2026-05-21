"""
Validate the configured PostgreSQL connection before starting the app.

Usage:
    python tests/check_db_connection.py
"""

import asyncio
import sys
from pathlib import Path
import platform

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings


async def main() -> None:
    try:
        conn = await psycopg.AsyncConnection.connect(settings.database_url)
    except Exception as exc:
        print(f"FAIL: cannot connect to DATABASE_URL: {exc}")
        raise SystemExit(1)

    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT current_database(), current_user, inet_server_port()")
            database_name, user_name, port = await cur.fetchone()

            await cur.execute("SHOW wal_level")
            wal_level = (await cur.fetchone())[0]

            await cur.execute("SELECT to_regclass('public.orders')")
            orders_table = (await cur.fetchone())[0]

        print("OK: PostgreSQL connection works")
        print(f"database={database_name}")
        print(f"user={user_name}")
        print(f"port={port}")
        print(f"wal_level={wal_level}")
        print(f"orders_table={orders_table}")

        if orders_table != "orders":
            print("FAIL: public.orders table was not found")
            raise SystemExit(1)

        if wal_level != "logical":
            print("WARN: CDC requires wal_level=logical for real-time SSE updates")
    finally:
        await conn.close()


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
