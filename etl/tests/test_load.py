from __future__ import annotations

from datetime import date

import duckdb
import pytest

from etl.load import create_indexes, create_schema, load_dimensions, load_fact_ball
from etl.quality_checks import run_all_checks
from etl.transform import generate_team_id, generate_venue_id


DUCKDB_INTEGRITY_EXCEPTIONS = (duckdb.IntegrityError, duckdb.ConstraintException)


def _players() -> list[dict]:
    return [
        {
            "player_id": "player-batter",
            "player_name": "Batter One",
            "name_variants": ["Batter One"],
            "gender": "male",
        },
        {
            "player_id": "player-bowler",
            "player_name": "Bowler One",
            "name_variants": ["Bowler One"],
            "gender": "male",
        },
        {
            "player_id": "player-non-striker",
            "player_name": "Non Striker",
            "name_variants": ["Non Striker"],
            "gender": "male",
        },
    ]


def _teams() -> list[dict]:
    return [
        {
            "team_id": generate_team_id("Mumbai Indians"),
            "team_name": "Mumbai Indians",
            "team_name_variants": [],
        },
        {
            "team_id": generate_team_id("Chennai Super Kings"),
            "team_name": "Chennai Super Kings",
            "team_name_variants": [],
        },
    ]


def _venues() -> list[dict]:
    return [
        {
            "venue_id": generate_venue_id("Wankhede Stadium"),
            "venue_name": "Wankhede Stadium",
            "city": "Mumbai",
        }
    ]


def _matches() -> list[dict]:
    return [
        {
            "match_id": "match-001",
            "season_year": 2024,
            "match_date": "2024-04-01",
            "tournament": "IPL",
            "gender": "male",
            "venue_id": generate_venue_id("Wankhede Stadium"),
            "team1_name": "Mumbai Indians",
            "team2_name": "Chennai Super Kings",
            "toss_winner_team_name": "Mumbai Indians",
            "toss_decision": "bat",
            "winner_team_name": "Mumbai Indians",
            "win_by_runs": 12,
            "win_by_wickets": None,
            "result": None,
            "method": None,
            "player_of_match_id": "player-batter",
        }
    ]


def _seasons() -> list[dict]:
    return [
        {
            "season_year": 2024,
            "season_label": "IPL 2024",
            "gender": "male",
            "total_matches": 1,
            "start_date": date(2024, 4, 1),
            "end_date": date(2024, 4, 1),
        }
    ]


def _fact_rows() -> list[dict]:
    return [
        {
            "match_id": "match-001",
            "innings_number": 1,
            "delivery_sequence": index,
            "over_number": 1 if index <= 2 else 8 if index <= 4 else 17,
            "ball_in_over": 1 if index in (1, 3, 5) else 2,
            "match_phase": "powerplay" if index <= 2 else "middle" if index <= 4 else "death",
            "batting_team": "Mumbai Indians",
            "bowling_team": "Chennai Super Kings",
            "batter_name": "Batter One",
            "batter_id": "player-batter",
            "bowler_name": "Bowler One",
            "bowler_id": "player-bowler",
            "non_striker_name": "Non Striker",
            "non_striker_id": "player-non-striker",
            "batter_runs": 1,
            "extras_runs": 0,
            "total_runs": 1,
            "extras_type": None,
            "is_legal_delivery": True,
            "is_dot_ball": False,
            "is_boundary_four": False,
            "is_boundary_six": False,
            "is_wicket": False,
            "is_bowler_wicket": False,
            "wicket_kind": None,
            "player_out_name": None,
            "player_out_id": None,
            "fielder_name": None,
            "fielder_id": None,
        }
        for index in range(1, 6)
    ]


def _load_valid_dataset(conn: duckdb.DuckDBPyConnection) -> None:
    create_schema(conn)
    load_dimensions(conn, _players(), _teams(), _venues(), _matches(), _seasons())
    load_fact_ball(conn, _fact_rows())


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def test_create_schema_creates_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """After create_schema, all 6 tables exist with the right columns."""
    create_schema(conn)

    rows = conn.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name IN (
              'dim_player',
              'dim_team',
              'dim_venue',
              'dim_season',
              'dim_match',
              'fact_ball'
          )
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    actual = {}
    for table_name, column_name in rows:
        actual.setdefault(table_name, []).append(column_name)

    assert actual == {
        "dim_match": [
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
        "dim_player": ["player_id", "player_name", "name_variants", "gender"],
        "dim_season": [
            "season_year",
            "season_label",
            "gender",
            "total_matches",
            "start_date",
            "end_date",
        ],
        "dim_team": ["team_id", "team_name", "team_name_variants"],
        "dim_venue": ["venue_id", "venue_name", "city"],
        "fact_ball": [
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
    }


def test_create_schema_is_idempotent(conn: duckdb.DuckDBPyConnection) -> None:
    """Calling create_schema twice does not raise."""
    create_schema(conn)
    create_schema(conn)


def test_load_dimensions_inserts_expected_rows(conn: duckdb.DuckDBPyConnection) -> None:
    """Given 3 players, dim_player has 3 rows."""
    create_schema(conn)

    load_dimensions(conn, _players(), _teams(), _venues(), _matches(), _seasons())

    assert conn.execute("SELECT COUNT(*) FROM dim_player").fetchone()[0] == 3


def test_load_fact_ball_inserts_expected_rows(conn: duckdb.DuckDBPyConnection) -> None:
    """Given 5 fact rows with valid FKs, fact_ball has 5 rows."""
    _load_valid_dataset(conn)

    assert conn.execute("SELECT COUNT(*) FROM fact_ball").fetchone()[0] == 5


def test_load_fact_ball_rejects_orphan_foreign_key(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert a fact row with batter_id not in dim_player - expect IntegrityError."""
    create_schema(conn)
    load_dimensions(conn, _players(), _teams(), _venues(), _matches(), _seasons())

    invalid_rows = _fact_rows()
    invalid_rows[0] = dict(invalid_rows[0], batter_id="missing-player")

    with pytest.raises(DUCKDB_INTEGRITY_EXCEPTIONS):
        load_fact_ball(conn, invalid_rows)


def test_load_fact_ball_rejects_invalid_match_phase(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert a fact row with match_phase='overs_1_to_6' - expect CHECK failure."""
    create_schema(conn)
    load_dimensions(conn, _players(), _teams(), _venues(), _matches(), _seasons())

    invalid_rows = _fact_rows()
    invalid_rows[0] = dict(invalid_rows[0], match_phase="overs_1_to_6")

    with pytest.raises(DUCKDB_INTEGRITY_EXCEPTIONS):
        load_fact_ball(conn, invalid_rows)


def test_create_indexes_does_not_raise(conn: duckdb.DuckDBPyConnection) -> None:
    """Smoke test that index creation completes."""
    _load_valid_dataset(conn)

    create_indexes(conn)


def test_run_all_checks_passes_on_valid_data(conn: duckdb.DuckDBPyConnection) -> None:
    """End-to-end: create schema, load minimal valid data, run checks, no errors."""
    _load_valid_dataset(conn)

    run_all_checks(
        conn,
        {
            "dim_player": 3,
            "dim_team": 2,
            "dim_venue": 1,
            "dim_match": 1,
            "dim_season": 1,
            "fact_ball": 5,
        },
    )


def test_run_all_checks_fails_on_row_count_mismatch(conn: duckdb.DuckDBPyConnection) -> None:
    """Expected 5 rows, actual 3, AssertionError raised."""
    _load_valid_dataset(conn)

    with pytest.raises(AssertionError, match="Row count mismatch for fact_ball"):
        run_all_checks(
            conn,
            {
                "dim_player": 3,
                "dim_team": 2,
                "dim_venue": 1,
                "dim_match": 1,
                "dim_season": 1,
                "fact_ball": 3,
            },
        )
