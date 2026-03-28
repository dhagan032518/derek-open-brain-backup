"""
backup.py
Dumps all Supabase tables to JSON files for nightly backup.

Runs via GitHub Actions on a cron schedule.
Reads SUPABASE_URL and SUPABASE_SERVICE_KEY from environment variables
(set as GitHub secrets — never hardcoded here).

Output: one JSON file per table in the /data/ folder.
"""

import os
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set as environment variables.")
    sys.exit(1)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_tables() -> list[str]:
    """Query information schema to get all user-created tables."""
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_tables"
    # Fall back to a known list if RPC not available
    # We use PostgREST's built-in schema introspection via a raw query
    url = f"{SUPABASE_URL}/rest/v1/"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"WARNING: Could not introspect schema (status {resp.status_code}). Using fallback table list.")
        return get_tables_fallback()

    # Parse OpenAPI response to extract table names
    try:
        data = resp.json()
        paths = data.get("paths", {})
        tables = [
            path.strip("/")
            for path in paths
            if path.startswith("/") and not path.startswith("/rpc")
        ]
        return [t for t in tables if t]  # filter empty strings
    except Exception as e:
        print(f"WARNING: Could not parse schema ({e}). Using fallback table list.")
        return get_tables_fallback()


def get_tables_fallback() -> list[str]:
    """Explicit table list — update this as new tables are created."""
    return [
        "thoughts",
        "agent_action_log",
    ]


def dump_table(table: str) -> int:
    """Fetch all rows from a table and write to JSON. Returns row count."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {"select": "*"}

    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        params["limit"] = page_size
        params["offset"] = offset
        resp = requests.get(url, headers=HEADERS, params=params)

        if resp.status_code == 404:
            print(f"  SKIP {table} — table not found (404)")
            return 0

        if resp.status_code != 200:
            print(f"  ERROR {table} — status {resp.status_code}: {resp.text[:200]}")
            return 0

        rows = resp.json()
        all_rows.extend(rows)

        if len(rows) < page_size:
            break  # last page
        offset += page_size

    output = {
        "table": table,
        "backed_up_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(all_rows),
        "rows": all_rows,
    }

    out_file = OUTPUT_DIR / f"{table}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    return len(all_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    started = datetime.now(timezone.utc)
    print(f"── Supabase backup started {started.isoformat()} ──")

    tables = get_tables()
    print(f"Tables to back up: {tables}\n")

    total_rows = 0
    results = []

    for table in tables:
        print(f"  Backing up: {table} ...", end=" ")
        count = dump_table(table)
        print(f"{count} rows")
        total_rows += count
        results.append({"table": table, "rows": count})

    # Write a manifest file summarising the run
    manifest = {
        "backed_up_at": started.isoformat(),
        "tables": results,
        "total_rows": total_rows,
    }
    with open(OUTPUT_DIR / "_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n── Backup complete — {len(tables)} tables, {total_rows} total rows ──")


if __name__ == "__main__":
    main()
