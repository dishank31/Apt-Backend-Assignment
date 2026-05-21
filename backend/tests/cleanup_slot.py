"""Quick script to terminate stale slot consumers."""
import psycopg

conn = psycopg.connect(
    "postgresql://postgres:password@localhost:5433/orders_db",
    autocommit=True,
)
cur = conn.cursor()

# Show all connections
cur.execute(
    "SELECT pid, state, query FROM pg_stat_activity "
    "WHERE datname = current_database() AND pid != pg_backend_pid()"
)
rows = cur.fetchall()
print(f"Found {len(rows)} other connections:")
for r in rows:
    print(f"  PID={r[0]} state={r[1]} query={r[2][:80]}")

# Terminate all except our own
cur.execute(
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
    "WHERE datname = current_database() AND pid != pg_backend_pid()"
)
print(f"\nTerminated {cur.rowcount} connections")

# Check slot status
cur.execute("SELECT slot_name, active, active_pid FROM pg_replication_slots")
print(f"\nSlot status: {cur.fetchall()}")

conn.close()
