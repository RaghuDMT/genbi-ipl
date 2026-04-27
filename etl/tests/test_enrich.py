from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from etl.enrich import (
    CURATED_VENUE_METADATA,
    HttpCache,
    RespectfulSession,
    add_enrichment_columns,
    compute_derived_columns,
    enrich_players,
    load_auction_data,
)


def test_http_cache_stores_and_retrieves(tmp_path: Path) -> None:
    cache = HttpCache(tmp_path / "cache.db")
    try:
        cache.set("https://example.com/a", "payload", 200, 30)
        assert cache.get("https://example.com/a") == "payload"
    finally:
        cache.close()


def test_http_cache_respects_ttl(tmp_path: Path) -> None:
    db_path = tmp_path / "cache.db"
    cache = HttpCache(db_path)
    cache.close()

    conn = sqlite3.connect(str(db_path))
    old_ts = (datetime.utcnow() - timedelta(days=100)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO http_cache (url, response_body, status_code, fetched_at, ttl_days)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("https://example.com/old", "stale", 200, old_ts, 30),
    )
    conn.commit()
    conn.close()

    cache = HttpCache(db_path)
    try:
        assert cache.get("https://example.com/old") is None
    finally:
        cache.close()


def test_add_enrichment_columns_idempotent() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE dim_player (player_id TEXT, player_name TEXT, cricinfo_id TEXT)")
        conn.execute("CREATE TABLE dim_venue (venue_id TEXT, venue_name TEXT, city TEXT)")
        conn.execute("CREATE TABLE dim_match (match_id TEXT, venue TEXT, date DATE)")
        conn.execute(
            """
            CREATE TABLE fact_ball (
                match_id TEXT,
                innings INTEGER,
                delivery_seq INTEGER,
                over_number INTEGER,
                ball_in_over INTEGER,
                batter TEXT,
                batter_runs INTEGER,
                total_runs INTEGER,
                is_bowler_wicket BOOLEAN
            )
            """
        )

        add_enrichment_columns(conn)
        add_enrichment_columns(conn)
    finally:
        conn.close()


def test_enrich_players_from_cricsheet_csv(tmp_path: Path) -> None:
    class MockResponse:
        status_code = 200
        text = "identifier,name,unique_name,key_cricinfo\n1,Virat Kohli,Virat Kohli,253802\n"

    conn = duckdb.connect(":memory:")
    cache = HttpCache(tmp_path / "cache.db")
    session = RespectfulSession(check_robots_startup=False)
    session.session.get = lambda *args, **kwargs: MockResponse()  # type: ignore[method-assign]
    try:
        conn.execute("CREATE TABLE dim_player (player_id TEXT, player_name TEXT, cricinfo_id TEXT, full_name TEXT)")
        conn.execute("CREATE TABLE dim_venue (venue_id TEXT, venue_name TEXT, city TEXT)")
        conn.execute("INSERT INTO dim_player VALUES ('p1', 'Virat Kohli', NULL, NULL)")
        enrich_players(conn, session, cache, top_n=1)
        full_name, cricinfo_id = conn.execute(
            "SELECT full_name, cricinfo_id FROM dim_player WHERE player_id = 'p1'"
        ).fetchone()
        assert full_name == "Virat Kohli"
        assert cricinfo_id == "253802"
    finally:
        conn.close()
        cache.close()


def test_venue_curated_table_complete() -> None:
    assert len(CURATED_VENUE_METADATA) >= 20
    for value in CURATED_VENUE_METADATA.values():
        assert "lat" in value
        assert "lon" in value
        assert "capacity" in value
        assert "pitch_type" in value


def test_compute_cumulative_runs() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE dim_player (player_id TEXT, player_name TEXT, cricinfo_id TEXT)")
        conn.execute("CREATE TABLE dim_venue (venue_id TEXT, venue_name TEXT, city TEXT)")
        conn.execute(
            """
            CREATE TABLE fact_ball (
                match_id TEXT,
                innings_number INTEGER,
                delivery_sequence INTEGER,
                over_number INTEGER,
                ball_in_over INTEGER,
                batter_name TEXT,
                batter_runs INTEGER,
                total_runs INTEGER,
                is_bowler_wicket BOOLEAN
            )
            """
        )
        for idx in range(1, 11):
            conn.execute(
                """
                INSERT INTO fact_ball (
                    match_id, innings_number, delivery_sequence, over_number, ball_in_over,
                    batter_name, batter_runs, total_runs, is_bowler_wicket
                )
                VALUES ('m1', 1, ?, 1, ?, 'A', 1, 1, FALSE)
                """,
                [idx, idx],
            )
        add_enrichment_columns(conn)
        compute_derived_columns(conn)
        rows = conn.execute(
            """
            SELECT cumulative_runs_in_innings
            FROM fact_ball
            ORDER BY delivery_sequence
            """
        ).fetchall()
        values = [r[0] for r in rows]
        assert values == sorted(values)
    finally:
        conn.close()


def test_auction_data_loads() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE dim_player_auction (
                player_name TEXT,
                season TEXT,
                team TEXT,
                sold_price_cr DECIMAL(5,2),
                base_price_cr DECIMAL(5,2),
                is_retained BOOLEAN,
                PRIMARY KEY (player_name, season)
            )
            """
        )
        load_auction_data(conn)
        count = conn.execute("SELECT COUNT(*) FROM dim_player_auction").fetchone()[0]
        min_price = conn.execute("SELECT MIN(sold_price_cr) FROM dim_player_auction").fetchone()[0]
        assert count > 100
        assert float(min_price) > 0
    finally:
        conn.close()


def test_rate_limiter_enforces_delay() -> None:
    class MockResponse:
        status_code = 200
        text = "ok"

    session = RespectfulSession(min_delay_sec=3.0, check_robots_startup=False)
    session.domain_allowed["example.com"] = True
    session.session.get = lambda *args, **kwargs: MockResponse()  # type: ignore[method-assign]
    started = time.monotonic()
    session.get("https://example.com/a")
    session.get("https://example.com/b")
    elapsed = time.monotonic() - started
    assert elapsed >= 3.0
