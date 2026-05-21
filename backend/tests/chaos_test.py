"""
Chaos Test Verification Script.

Automates verification of the three chaos test scenarios:
  - Test 1: Database Outage Recovery (manual PostgreSQL stop/start)
  - Test 2: Memory Leak / Client Cleanup (SSE listener tracking)
  - Test 3: Data Tsunami (bulk insert via REST API)

Usage:
    python tests/chaos_test.py --test 1   # Database outage (interactive)
    python tests/chaos_test.py --test 2   # Memory leak
    python tests/chaos_test.py --test 3   # Data tsunami
    python tests/chaos_test.py --all      # Run all tests
"""

import argparse
import asyncio
import json
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError


BASE_URL = "http://127.0.0.1:8000"
API_URL = f"{BASE_URL}/api/v1/orders"
HEALTH_URL = f"{BASE_URL}/health"


def get_health() -> dict:
    """Fetch /health endpoint and return parsed JSON."""
    try:
        with urlopen(HEALTH_URL, timeout=5) as resp:
            return json.loads(resp.read())
    except URLError as e:
        print(f"  ❌ Cannot reach server: {e}")
        return {}


def get_listeners() -> int:
    """Return the current number of active SSE listeners."""
    health = get_health()
    if not health:
        return -1
    return health.get("cdc", {}).get("active_listeners", -1)


def post_order(customer: str, product: str, status: str = "pending") -> bool:
    """Create an order via REST API. Returns True on success."""
    payload = json.dumps({
        "customer_name": customer,
        "product_name": product,
        "status": status,
    }).encode()
    req = Request(API_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=5) as resp:
            return resp.status == 201
    except URLError as e:
        print(f"  ❌ POST failed: {e}")
        return False


# ── Test 1: Database Outage ──────────────────────────────────────

def test_database_outage():
    """Interactive test: checks server survives a PostgreSQL stop/start cycle."""
    print("\n" + "=" * 60)
    print("TEST 1: Database Outage Recovery")
    print("=" * 60)

    # Step 1: Verify server is running
    print("\n[1/5] Checking server health...")
    health = get_health()
    if not health:
        print("  [FAIL] -- Server is not reachable. Start it first.")
        return False

    cdc_connected = health.get("cdc", {}).get("connected", False)
    print(f"  [OK] Server is up | CDC connected: {cdc_connected}")

    # Step 2: Ask user to stop PostgreSQL
    print("\n[2/5] Now STOP PostgreSQL manually:")
    print("       -> Open 'Services' app (Win+R -> services.msc)")
    print("       -> Find your PostgreSQL service")
    print("       -> Right-click -> Stop")
    input("       Press ENTER after stopping PostgreSQL...")

    # Step 3: Verify server survived
    print("\n[3/5] Checking server survived the outage...")
    health = get_health()
    if not health:
        print("  [FAIL] -- Server crashed when PostgreSQL stopped!")
        return False

    cdc_connected = health.get("cdc", {}).get("connected", False)
    last_error = health.get("cdc", {}).get("last_error", "none")
    print(f"  [OK] Server is ALIVE | CDC connected: {cdc_connected}")
    print(f"     Last error: {last_error}")

    if cdc_connected:
        print("  [WARN] CDC still shows connected -- it may not have detected the outage yet")
        print("     Wait a few seconds and re-check...")

    # Step 4: Ask user to start PostgreSQL
    print("\n[4/5] Now START PostgreSQL again:")
    print("       -> Open 'Services' app")
    print("       -> Find your PostgreSQL service")
    print("       -> Right-click -> Start")
    input("       Press ENTER after starting PostgreSQL...")

    # Step 5: Wait for recovery
    print("\n[5/5] Waiting for CDC to re-establish connection...")
    for attempt in range(15):
        time.sleep(2)
        health = get_health()
        if health.get("cdc", {}).get("connected", False):
            print(f"  [OK] PASS -- CDC reconnected after {(attempt + 1) * 2}s!")
            # Verify data flows
            print("  Verifying data pipeline...")
            if post_order("Chaos Test Recovery", "Resilience Badge"):
                print("  [OK] PASS -- Order created successfully after recovery!")
            else:
                print("  [FAIL] -- Could not create order after recovery")
            return True
        print(f"     Attempt {attempt + 1}/15 -- CDC not yet reconnected...")

    print("  [FAIL] -- CDC did not reconnect within 30 seconds")
    return False


# ── Test 2: Memory Leak ──────────────────────────────────────────

def test_memory_leak():
    """
    Simulates multiple SSE clients connecting, then verifies cleanup.

    Since we can't open/close browser tabs programmatically, this test:
    1. Records the current listener count
    2. Opens N SSE connections via urllib
    3. Verifies listener count increased
    4. Closes the connections
    5. Waits for cleanup and verifies listener count decreased
    """
    print("\n" + "=" * 60)
    print("TEST 2: Memory Leak / Client Cleanup")
    print("=" * 60)

    import http.client
    import socket

    # Step 1: Record baseline
    print("\n[1/4] Recording baseline listener count...")
    baseline = get_listeners()
    if baseline < 0:
        print("  [FAIL] -- Cannot reach server")
        return False
    print(f"  Baseline listeners: {baseline}")

    # Step 2: Open SSE connections
    NUM_CLIENTS = 4
    print(f"\n[2/4] Opening {NUM_CLIENTS} SSE connections...")

    connections = []
    for i in range(NUM_CLIENTS):
        try:
            conn = http.client.HTTPConnection("localhost", 8000, timeout=5)
            conn.request("GET", "/api/v1/orders/stream", headers={"Accept": "text/event-stream"})
            # Start reading the response (don't consume it fully)
            resp = conn.getresponse()
            connections.append((conn, resp))
            print(f"  Client {i + 1} connected (status={resp.status})")
        except Exception as e:
            print(f"  [FAIL] Client {i + 1} failed: {e}")

    # Give server a moment to register listeners
    time.sleep(2)

    # Step 3: Verify listener count increased
    print("\n[3/4] Verifying listener count increased...")
    current = get_listeners()
    expected_min = baseline + len(connections)
    print(f"  Current listeners: {current} (expected >= {expected_min})")

    if current >= expected_min:
        print(f"  [OK] Listeners registered correctly")
    else:
        print(f"  [WARN] Listener count lower than expected")

    # Step 4: Close connections and verify cleanup
    print(f"\n[4/4] Closing {len(connections)} connections and waiting for cleanup...")
    for conn, resp in connections:
        try:
            conn.close()
        except Exception:
            pass

    # Wait for the server to detect disconnects (up to 15 seconds)
    cleanup_success = False
    for attempt in range(15):
        time.sleep(1)
        current = get_listeners()
        print(f"  Attempt {attempt + 1}/15 -- Active listeners: {current}")
        if current <= baseline:
            cleanup_success = True
            break

    if cleanup_success:
        print(f"\n  [OK] PASS -- Listeners cleaned up! Final count: {current} (baseline was {baseline})")
        return True
    else:
        print(f"\n  [FAIL] -- Memory leak detected! Listeners: {current}, expected: {baseline}")
        print(f"     {current - baseline} orphaned listener(s) remain")
        return False


# ── Test 3: Data Tsunami ─────────────────────────────────────────

def test_data_tsunami():
    """Insert 10000 orders rapidly and verify they all arrived."""
    print("\n" + "=" * 60)
    print("TEST 3: Data Tsunami (100 rapid inserts)")
    print("=" * 60)

    NUM_ORDERS = 10000

    # Step 1: Record baseline
    print("\n[1/3] Recording baseline order count...")
    try:
        with urlopen(API_URL, timeout=5) as resp:
            existing = json.loads(resp.read())
            baseline = len(existing)
    except URLError:
        print("  [FAIL] -- Cannot reach API")
        return False
    print(f"  Existing orders: {baseline}")

    # Step 2: Rapid-fire inserts
    print(f"\n[2/3] Inserting {NUM_ORDERS} orders as fast as possible...")
    start = time.perf_counter()
    successes = 0

    for i in range(1, NUM_ORDERS + 1):
        if post_order(f"Tsunami Test {i}", "Storm Item"):
            successes += 1

    elapsed = time.perf_counter() - start
    rate = successes / elapsed if elapsed > 0 else 0
    print(f"  Inserted {successes}/{NUM_ORDERS} in {elapsed:.2f}s ({rate:.0f} orders/sec)")

    if successes < NUM_ORDERS:
        print(f"  [WARN] {NUM_ORDERS - successes} inserts failed")

    # Step 3: Verify all arrived
    print("\n[3/3] Verifying all orders exist in the database...")
    time.sleep(2)  # Wait for CDC to process

    try:
        with urlopen(API_URL, timeout=10) as resp:
            all_orders = json.loads(resp.read())
            final_count = len(all_orders)
    except URLError as e:
        print(f"  [FAIL] -- Cannot fetch orders: {e}")
        return False

    new_orders = final_count - baseline
    print(f"  Total orders now: {final_count} (+{new_orders} new)")

    if new_orders >= successes:
        print(f"\n  [OK] PASS -- All {successes} orders persisted successfully!")
        print(f"  Now check your browser -- all {successes} rows should be visible")
        print(f"  with NO freezing or stuttering (thanks to requestAnimationFrame batching)")
        return True
    else:
        print(f"\n  [FAIL] -- Expected {successes} new orders, found {new_orders}")
        return False


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Chaos Test Suite")
    parser.add_argument("--test", type=int, choices=[1, 2, 3], help="Run a specific test")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    args = parser.parse_args()

    if not args.test and not args.all:
        parser.print_help()
        sys.exit(1)

    print("+----------------------------------------------+")
    print("|        CHAOS TEST VERIFICATION SUITE         |")
    print("+----------------------------------------------+")
    print(f"\nTarget: {BASE_URL}")

    results = {}

    if args.test == 1 or args.all:
        results["Test 1: DB Outage"] = test_database_outage()

    if args.test == 2 or args.all:
        results["Test 2: Memory Leak"] = test_memory_leak()

    if args.test == 3 or args.all:
        results["Test 3: Data Tsunami"] = test_data_tsunami()

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    for name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {status}  {name}")

    all_passed = all(results.values())
    print(f"\n{'- All tests passed!' if all_passed else '- Some tests failed.'}")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
