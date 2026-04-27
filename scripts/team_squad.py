"""List players who appeared for a team in a given season.

Usage:
    python scripts/team_squad.py --team "Royal Challengers Bengaluru" --season 2026
    python scripts/team_squad.py --team RCB --season 2026
    python scripts/team_squad.py --team "Gujarat Titans" --season 2026 --format json
    python scripts/team_squad.py --list-teams

Short aliases work for common teams (RCB, GT, CSK, MI, etc.).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb


DB_PATH = Path("data/db/genbi.duckdb")

# Common short aliases mapped to canonical Cricsheet names
TEAM_ALIASES: dict[str, str] = {
    "RCB": "Royal Challengers Bengaluru",
    "GT": "Gujarat Titans",
    "CSK": "Chennai Super Kings",
    "MI": "Mumbai Indians",
    "KKR": "Kolkata Knight Riders",
    "DC": "Delhi Capitals",
    "PBKS": "Punjab Kings",
    "SRH": "Sunrisers Hyderabad",
    "RR": "Rajasthan Royals",
    "LSG": "Lucknow Super Giants",
    "GL": "Gujarat Lions",
    "RPS": "Rising Pune Supergiant",
}


def resolve_team_name(conn: duckdb.DuckDBPyConnection, team_input: str) -> str | None:
    """Resolve a short alias or partial name to the canonical team name."""
    if team_input.upper() in TEAM_ALIASES:
        return TEAM_ALIASES[team_input.upper()]

    rows = conn.execute(
        "SELECT team_name FROM dim_team WHERE team_name = ?",
        [team_input],
    ).fetchall()
    if rows:
        return rows[0][0]

    rows = conn.execute(
        "SELECT team_name FROM dim_team WHERE LOWER(team_name) LIKE LOWER(?) LIMIT 5",
        [f"%{team_input}%"],
    ).fetchall()

    if not rows:
        return None
    if len(rows) == 1:
        return rows[0][0]

    print(f"Ambiguous team '{team_input}'. Matches found:", file=sys.stderr)
    for (name,) in rows:
        print(f"  - {name}", file=sys.stderr)
    return None


def list_teams(conn: duckdb.DuckDBPyConnection) -> None:
    """Print all teams in the database."""
    rows = conn.execute(
        "SELECT team_name FROM dim_team ORDER BY team_name"
    ).fetchall()
    print("Teams in database:")
    for (name,) in rows:
        alias = next((k for k, v in TEAM_ALIASES.items() if v == name), "")
        alias_str = f"  (alias: {alias})" if alias else ""
        print(f"  {name}{alias_str}")


def get_batters(
    conn: duckdb.DuckDBPyConnection,
    team_name: str,
    season_year: int,
    tournament: str,
) -> list[tuple]:
    """Return batters who played for the team in the given season."""
    return conn.execute(
        """
        SELECT
            p.player_name,
            COUNT(DISTINCT f.match_id) AS matches,
            SUM(f.batter_runs) AS total_runs,
            SUM(CASE WHEN f.is_legal_delivery THEN 1 ELSE 0 END) AS balls_faced,
            SUM(CASE WHEN f.is_boundary_four THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN f.is_boundary_six THEN 1 ELSE 0 END) AS sixes
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        JOIN dim_player p ON f.batter_id = p.player_id
        WHERE f.batting_team = ?
          AND m.season_year = ?
          AND m.tournament = ?
        GROUP BY p.player_name
        ORDER BY total_runs DESC
        """,
        [team_name, season_year, tournament],
    ).fetchall()


def get_bowlers(
    conn: duckdb.DuckDBPyConnection,
    team_name: str,
    season_year: int,
    tournament: str,
) -> list[tuple]:
    """Return bowlers who played for the team in the given season."""
    return conn.execute(
        """
        SELECT
            p.player_name,
            COUNT(DISTINCT f.match_id) AS matches,
            SUM(CASE WHEN f.is_bowler_wicket THEN 1 ELSE 0 END) AS wickets,
            SUM(CASE WHEN f.is_legal_delivery THEN 1 ELSE 0 END) AS balls_bowled,
            SUM(f.total_runs) AS runs_conceded
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        JOIN dim_player p ON f.bowler_id = p.player_id
        WHERE f.bowling_team = ?
          AND m.season_year = ?
          AND m.tournament = ?
        GROUP BY p.player_name
        ORDER BY wickets DESC, balls_bowled DESC
        """,
        [team_name, season_year, tournament],
    ).fetchall()


def print_text(
    team_name: str,
    season_year: int,
    tournament: str,
    batters: list[tuple],
    bowlers: list[tuple],
) -> None:
    """Pretty-print the squad to stdout."""
    header = f"{team_name} — {tournament} {season_year}"
    print("=" * 72)
    print(header)
    print("=" * 72)

    if not batters and not bowlers:
        print("No players found. Check team name and season with --list-teams.")
        return

    print(f"\nBatters ({len(batters)} players)")
    print(f"{'Name':<30} {'Mat':>4} {'Runs':>5} {'Balls':>6} {'SR':>7} {'4s':>3} {'6s':>3}")
    print("-" * 72)
    for name, matches, runs, balls, fours, sixes in batters:
        sr = (runs * 100.0 / balls) if balls else 0.0
        print(f"{name:<30} {matches:>4} {runs:>5} {balls:>6} {sr:>7.1f} {fours:>3} {sixes:>3}")

    print(f"\nBowlers ({len(bowlers)} players)")
    print(f"{'Name':<30} {'Mat':>4} {'Wkts':>5} {'Balls':>6} {'Econ':>6}")
    print("-" * 72)
    for name, matches, wickets, balls, runs in bowlers:
        econ = (runs * 6.0 / balls) if balls else 0.0
        print(f"{name:<30} {matches:>4} {wickets:>5} {balls:>6} {econ:>6.2f}")
    print()


def print_json(
    team_name: str,
    season_year: int,
    tournament: str,
    batters: list[tuple],
    bowlers: list[tuple],
) -> None:
    """Emit structured JSON for downstream consumption."""
    output = {
        "team": team_name,
        "tournament": tournament,
        "season_year": season_year,
        "batters": [
            {
                "player_name": name,
                "matches": matches,
                "total_runs": runs,
                "balls_faced": balls,
                "strike_rate": round(runs * 100.0 / balls, 2) if balls else None,
                "fours": fours,
                "sixes": sixes,
            }
            for name, matches, runs, balls, fours, sixes in batters
        ],
        "bowlers": [
            {
                "player_name": name,
                "matches": matches,
                "wickets": wickets,
                "balls_bowled": balls,
                "economy": round(runs * 6.0 / balls, 2) if balls else None,
                "runs_conceded": runs,
            }
            for name, matches, wickets, balls, runs in bowlers
        ],
    }
    print(json.dumps(output, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List players who appeared for a team in a given season.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--team", help="Team name or short alias (e.g. RCB, GT)")
    parser.add_argument("--season", type=int, default=2026, help="Season year (default: 2026)")
    parser.add_argument(
        "--tournament", default="IPL", choices=["IPL", "WPL"], help="Tournament (default: IPL)"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    parser.add_argument(
        "--list-teams", action="store_true", help="List all teams in the database and exit"
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the ETL first.", file=sys.stderr)
        return 1

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if args.list_teams:
            list_teams(conn)
            return 0

        if not args.team:
            parser.error("--team is required (or use --list-teams)")

        team_name = resolve_team_name(conn, args.team)
        if team_name is None:
            print(f"Team '{args.team}' not found. Use --list-teams to see all teams.", file=sys.stderr)
            return 1

        batters = get_batters(conn, team_name, args.season, args.tournament)
        bowlers = get_bowlers(conn, team_name, args.season, args.tournament)

        if args.format == "json":
            print_json(team_name, args.season, args.tournament, batters, bowlers)
        else:
            print_text(team_name, args.season, args.tournament, batters, bowlers)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())