from __future__ import annotations

import time
from collections.abc import Sequence
from pathlib import Path
from time import perf_counter

import duckdb
import structlog

from etl.transform import generate_team_id


_CHUNK_SIZE = 5_000
logger = structlog.get_logger(__name__)

DIM_PLAYER_TABLE_SQL = """
CREATE TABLE dim_player (
    player_id VARCHAR PRIMARY KEY,
    player_name VARCHAR NOT NULL,
    name_variants VARCHAR[],
    gender VARCHAR NOT NULL CHECK (gender IN ('male', 'female'))
)
"""

DIM_TEAM_TABLE_SQL = """
CREATE TABLE dim_team (
    team_id VARCHAR PRIMARY KEY,
    team_name VARCHAR NOT NULL,
    team_name_variants VARCHAR[]
)
"""

DIM_VENUE_TABLE_SQL = """
CREATE TABLE dim_venue (
    venue_id VARCHAR PRIMARY KEY,
    venue_name VARCHAR NOT NULL,
    city VARCHAR
)
"""

DIM_SEASON_TABLE_SQL = """
CREATE TABLE dim_season (
    season_year INTEGER NOT NULL,
    season_label VARCHAR NOT NULL,
    gender VARCHAR NOT NULL CHECK (gender IN ('male', 'female')),
    total_matches INTEGER NOT NULL,
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (season_year, gender)
)
"""

DIM_MATCH_TABLE_SQL = """
CREATE TABLE dim_match (
    match_id VARCHAR PRIMARY KEY,
    season_year INTEGER NOT NULL,
    match_date DATE,
    tournament VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    venue_id VARCHAR NOT NULL REFERENCES dim_venue(venue_id),
    team1_id VARCHAR NOT NULL REFERENCES dim_team(team_id),
    team2_id VARCHAR NOT NULL REFERENCES dim_team(team_id),
    toss_winner_team_id VARCHAR REFERENCES dim_team(team_id),
    toss_decision VARCHAR,
    winner_team_id VARCHAR REFERENCES dim_team(team_id),
    win_by_runs INTEGER,
    win_by_wickets INTEGER,
    result VARCHAR,
    method VARCHAR,
    player_of_match_id VARCHAR REFERENCES dim_player(player_id)
)
"""

FACT_BALL_TABLE_SQL = """
CREATE TABLE fact_ball (
    match_id VARCHAR NOT NULL REFERENCES dim_match(match_id),
    innings_number  INTEGER NOT NULL CHECK (innings_number >= 1),
    delivery_sequence INTEGER NOT NULL,
    over_number INTEGER NOT NULL,
    ball_in_over INTEGER NOT NULL,
    match_phase VARCHAR NOT NULL CHECK (match_phase IN ('powerplay', 'middle', 'death')),
    batting_team VARCHAR NOT NULL,
    bowling_team VARCHAR NOT NULL,
    batter_name VARCHAR NOT NULL,
    batter_id VARCHAR NOT NULL REFERENCES dim_player(player_id),
    bowler_name VARCHAR NOT NULL,
    bowler_id VARCHAR NOT NULL REFERENCES dim_player(player_id),
    non_striker_name VARCHAR NOT NULL,
    non_striker_id VARCHAR NOT NULL REFERENCES dim_player(player_id),
    batter_runs INTEGER NOT NULL,
    extras_runs INTEGER NOT NULL,
    total_runs INTEGER NOT NULL,
    extras_type VARCHAR,
    is_legal_delivery BOOLEAN NOT NULL,
    is_dot_ball BOOLEAN NOT NULL,
    is_boundary_four BOOLEAN NOT NULL,
    is_boundary_six BOOLEAN NOT NULL,
    is_wicket BOOLEAN NOT NULL,
    is_bowler_wicket BOOLEAN NOT NULL,
    wicket_kind VARCHAR,
    player_out_name VARCHAR,
    player_out_id VARCHAR REFERENCES dim_player(player_id),
    fielder_name VARCHAR,
    fielder_id VARCHAR REFERENCES dim_player(player_id),
    PRIMARY KEY (match_id, innings_number, delivery_sequence)
)
"""

PLAYER_INSERT_SQL = """
INSERT INTO dim_player (player_id, player_name, name_variants, gender)
VALUES (?, ?, ?, ?)
"""

TEAM_INSERT_SQL = """
INSERT INTO dim_team (team_id, team_name, team_name_variants)
VALUES (?, ?, ?)
"""

VENUE_INSERT_SQL = """
INSERT INTO dim_venue (venue_id, venue_name, city)
VALUES (?, ?, ?)
"""

MATCH_INSERT_SQL = """
INSERT INTO dim_match (
    match_id,
    season_year,
    match_date,
    tournament,
    gender,
    venue_id,
    team1_id,
    team2_id,
    toss_winner_team_id,
    toss_decision,
    winner_team_id,
    win_by_runs,
    win_by_wickets,
    result,
    method,
    player_of_match_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SEASON_INSERT_SQL = """
INSERT INTO dim_season (
    season_year,
    season_label,
    gender,
    total_matches,
    start_date,
    end_date
)
VALUES (?, ?, ?, ?, ?, ?)
"""

FACT_BALL_INSERT_SQL = """
INSERT INTO fact_ball (
    match_id,
    innings_number,
    delivery_sequence,
    over_number,
    ball_in_over,
    match_phase,
    batting_team,
    bowling_team,
    batter_name,
    batter_id,
    bowler_name,
    bowler_id,
    non_striker_name,
    non_striker_id,
    batter_runs,
    extras_runs,
    total_runs,
    extras_type,
    is_legal_delivery,
    is_dot_ball,
    is_boundary_four,
    is_boundary_six,
    is_wicket,
    is_bowler_wicket,
    wicket_kind,
    player_out_name,
    player_out_id,
    fielder_name,
    fielder_id
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. Creates parent dir if missing.

    Args:
        db_path: Path to the target DuckDB database file.

    Returns:
        An open DuckDB connection.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop and recreate all tables with proper constraints.

    Args:
        conn: Open DuckDB connection.
    """
    started_at = perf_counter()
    for table_name in ("fact_ball", "dim_match", "dim_season", "dim_player", "dim_team", "dim_venue"):
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    for ddl in (
        DIM_PLAYER_TABLE_SQL,
        DIM_TEAM_TABLE_SQL,
        DIM_VENUE_TABLE_SQL,
        DIM_SEASON_TABLE_SQL,
        DIM_MATCH_TABLE_SQL,
        FACT_BALL_TABLE_SQL,
    ):
        conn.execute(ddl)

    logger.info("Created schema", duration_seconds=round(perf_counter() - started_at, 4))


def load_dimensions(
    conn: duckdb.DuckDBPyConnection,
    players: list[dict],
    teams: list[dict],
    venues: list[dict],
    matches: list[dict],
    seasons: list[dict],
) -> None:
    """Bulk insert dimension rows.

    Args:
        conn: Open DuckDB connection.
        players: Player dimension rows.
        teams: Team dimension rows.
        venues: Venue dimension rows.
        matches: Match dimension rows.
        seasons: Season dimension rows.
    """
    _bulk_insert(
        conn,
        "dim_player",
        players,
        ["player_id", "player_name", "name_variants", "gender"],
    )
    _bulk_insert(
        conn,
        "dim_team",
        teams,
        ["team_id", "team_name", "team_name_variants"],
    )
    _bulk_insert(
        conn,
        "dim_venue",
        venues,
        ["venue_id", "venue_name", "city"],
    )
    _bulk_insert(
        conn,
        "dim_match",
        [_normalize_match_record(match) for match in matches],
        [
            "match_id",
            "season_year",
            "match_date",
            "tournament",
            "gender",
            "venue_id",
            "team1_id",
            "team2_id",
            "toss_winner_team_id",
            "toss_decision",
            "winner_team_id",
            "win_by_runs",
            "win_by_wickets",
            "result",
            "method",
            "player_of_match_id",
        ],
    )
    _bulk_insert(
        conn,
        "dim_season",
        seasons,
        ["season_year", "season_label", "gender", "total_matches", "start_date", "end_date"],
    )


def load_fact_ball(
    conn: duckdb.DuckDBPyConnection,
    fact_rows: list[dict],
) -> None:
    """Bulk insert fact_ball rows.

    Args:
        conn: Open DuckDB connection.
        fact_rows: Fact rows to insert.
    """
    _bulk_insert(
        conn,
        "fact_ball",
        fact_rows,
        [
            "match_id",
            "innings_number",
            "delivery_sequence",
            "over_number",
            "ball_in_over",
            "match_phase",
            "batting_team",
            "bowling_team",
            "batter_name",
            "batter_id",
            "bowler_name",
            "bowler_id",
            "non_striker_name",
            "non_striker_id",
            "batter_runs",
            "extras_runs",
            "total_runs",
            "extras_type",
            "is_legal_delivery",
            "is_dot_ball",
            "is_boundary_four",
            "is_boundary_six",
            "is_wicket",
            "is_bowler_wicket",
            "wicket_kind",
            "player_out_name",
            "player_out_id",
            "fielder_name",
            "fielder_id",
        ],
    )


def create_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Create indexes on common filter columns.

    Args:
        conn: Open DuckDB connection.
    """
    started_at = perf_counter()
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_fact_ball_batter_id ON fact_ball (batter_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ball_bowler_id ON fact_ball (bowler_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ball_match_id ON fact_ball (match_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ball_match_phase ON fact_ball (match_phase)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ball_over_number ON fact_ball (over_number)",
        "CREATE INDEX IF NOT EXISTS idx_dim_match_season_year ON dim_match (season_year)",
        "CREATE INDEX IF NOT EXISTS idx_dim_match_match_date ON dim_match (match_date)",
        "CREATE INDEX IF NOT EXISTS idx_dim_match_venue_id ON dim_match (venue_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_player_player_name ON dim_player (player_name)",
    )
    for statement in statements:
        conn.execute(statement)

    logger.info("Created indexes", duration_seconds=round(perf_counter() - started_at, 4))


def _bulk_insert(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    rows: list[dict],
    columns: list[str],
) -> None:
    """Bulk insert rows using pandas DataFrame via DuckDB's native reader.

    DuckDB reads pandas DataFrames directly via its Arrow bridge.
    This is orders of magnitude faster than executemany for large datasets.

    Args:
        conn: Active DuckDB connection.
        table_name: Target table name.
        rows: List of row dicts.
        columns: Ordered list of column names matching the table schema.
    """
    if not rows:
        logger.info("No rows to insert", table_name=table_name)
        return

    start = time.perf_counter()

    import pandas as pd  # noqa: PLC0415

    df = pd.DataFrame([{col: row.get(col) for col in columns} for row in rows])
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

    duration = time.perf_counter() - start
    logger.info(
        "Loaded rows",
        table_name=table_name,
        row_count=len(rows),
        duration_seconds=round(duration, 4),
    )


def _normalize_match_record(match: dict) -> dict:
    """Map transformed match fields to schema-ready foreign keys.

    Args:
        match: Match record from ``build_dim_match``.

    Returns:
        A copy with team foreign-key columns added.
    """
    normalized = dict(match)
    normalized["team1_id"] = _team_id_from_name(match.get("team1_name"))
    normalized["team2_id"] = _team_id_from_name(match.get("team2_name"))
    normalized["toss_winner_team_id"] = _team_id_from_name(match.get("toss_winner_team_name"))
    normalized["winner_team_id"] = _team_id_from_name(match.get("winner_team_name"))
    return normalized


def _team_id_from_name(team_name: object) -> str | None:
    """Return a stable team identifier from a team name.

    Args:
        team_name: Team name value.

    Returns:
        Stable team identifier when a name is present, else ``None``.
    """
    if team_name is None:
        return None

    normalized_team_name = str(team_name).strip()
    if not normalized_team_name:
        return None

    return generate_team_id(normalized_team_name)
