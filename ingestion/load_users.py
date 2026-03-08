from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple
import os

import psycopg
from dotenv import load_dotenv


# Load environment variables from repo root .env
load_dotenv()


@dataclass(frozen=True)
class UserRow:
    """A single user record to be upserted into raw.users."""
    id: int
    email: str
    created_at: datetime
    updated_at: datetime


def generate_demo_batch() -> List[UserRow]:
    """
    Demo batch:
    - Upsert existing id=1 (simulate an update)
    - Insert new id=4 (simulate a new user)
    """
    now = datetime.now(timezone.utc)
    return [
        UserRow(
            id=1,
            email=f"updated_{int(now.timestamp())}@test.com",
            created_at=now,
            updated_at=now,
        ),
        UserRow(
            id=4,
            email="new_user@test.com",
            created_at=now,
            updated_at=now,
        ),
    ]


UPSERT_SQL = """
INSERT INTO raw.users (id, email, created_at, updated_at, ingested_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (id)
DO UPDATE SET
  email = EXCLUDED.email,
  updated_at = EXCLUDED.updated_at,
  ingested_at = NOW();
"""


def upsert_users(conn: psycopg.Connection, rows: List[UserRow]) -> Tuple[int, int]:
    """
    Upsert rows into raw.users.

    Returns:
      (raw_count_before, raw_count_after)
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.users;")
        before = int(cur.fetchone()[0])

        for r in rows:
            cur.execute(UPSERT_SQL, (r.id, r.email, r.created_at, r.updated_at))

        cur.execute("SELECT COUNT(*) FROM raw.users;")
        after = int(cur.fetchone()[0])

    return before, after


def main() -> None:
    db_url = os.getenv("NEON_DB_URL")
    if not db_url:
        raise RuntimeError("Missing NEON_DB_URL. Set it in your repo root .env file.")

    batch = generate_demo_batch()

    with psycopg.connect(db_url) as conn:
        before, after = upsert_users(conn, batch)
        conn.commit()

    print("=== Ingestion Report ===")
    print(f"Rows processed: {len(batch)}")
    print(f"raw.users rowcount: {before} -> {after}")


if __name__ == "__main__":
    main()