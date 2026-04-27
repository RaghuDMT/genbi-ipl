"""Batter-vs-bowler matchup analysis for a given fixture.

Usage:
    python scripts/matchups.py \\
        --batters "V Kohli,PD Salt,RM Patidar" \\
        --bowlers "Rashid Khan,Mohammed Siraj,K Rabada" \\
        --since-year 2022

    # Or point at JSON files with one name per line
    python scripts/matchups.py \\
        --batters-file rcb_batters.txt \\
        --bowlers-file gt_bowlers.txt

    # Filter by match phase
    python scripts/matchups.py \\
        --batters "V Kohli" --bowlers "Rashid Khan" \\
        --phase death
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb


DB_PATH = Path("data/db/genbi.duckdb")


def parse_names(
    names_arg: str | None,
    names_file: str | None,
) -> list[str]:
    """Parse a comma-separated list or a file of names (one per line)."""
    if names_arg:
        return [n.strip() for n in names_arg.split(",") if n.strip()]
    if names_file:
        path = Path(names_file)
        if not path.exists():
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(1)
        return [line.strip() for line in path.read_text().splitlines() if line.strip()]
    return []


def resolve_players(
    conn: duckdb.DuckDBPyConnection, names: list[str]
) -> dict[str, str | None]:
    """Resolve a list of names to player_ids via dim_player.name_variants.

    Returns a dict mapping the input name to its resolved player_id (or None if unresolved).
    """
    resolved: dict[str, str | None] = {}
    for name in names:
        # Try exact match on player_name first
        row = conn.execute(
            "SELECT player_id FROM dim_player WHERE player_name = ? LIMIT 1",
            [name],
        ).fetchone()
        if row:
            resolved[name] = row[0]
            continue

        # Try membership in name_variants array
        row = conn.execute(
            "SELECT player_id FROM dim_player WHERE list_contains(name_variants, ?) LIMIT 1",
            [name],
        ).fetchone()
        if row:
            resolved[name] = row[0]
            continue

        # Try fuzzy LIKE on player_name as last resort
        row = conn.execute(
            "SELECT player_id FROM dim_player WHERE LOWER(player_name) LIKE LOWER(?) LIMIT 1",
            [f"%{name}%"],
        ).fetchone()
        resolved[name] = row[0] if row else None

    return resolved


def fetch_matchup(
    conn: duckdb.DuckDBPyConnection,
    batter_id: str,
    bowler_id: str,
    since_year: int,
    phase: str | None,
) -> dict:
    """Compute batter-vs-bowler stats across legal deliveries."""
    phase_filter = "AND f.match_phase = ?" if phase else ""
    params = [batter_id, bowler_id, since_year]
    if phase:
        params.append(phase)

    row = conn.execute(
        f"""
        SELECT
            SUM(CASE WHEN f.is_legal_delivery THEN 1 ELSE 0 END) AS balls,
            SUM(f.batter_runs) AS runs,
            SUM(CASE WHEN f.is_boundary_four THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN f.is_boundary_six THEN 1 ELSE 0 END) AS sixes,
            SUM(CASE WHEN f.is_dot_ball THEN 1 ELSE 0 END) AS dots,
            SUM(CASE WHEN f.is_bowler_wicket AND f.player_out_id = f.batter_id THEN 1 ELSE 0 END) AS dismissals,
            COUNT(DISTINCT f.match_id) AS matches
        FROM fact_ball f
        JOIN dim_match m ON f.match_id = m.match_id
        WHERE f.batter_id = ?
          AND f.bowler_id = ?
          AND m.season_year >= ?
          AND m.tournament = 'IPL'
          {phase_filter}
        """,
        params,
    ).fetchone()

    balls, runs, fours, sixes, dots, dismissals, matches = row
    balls = balls or 0
    runs = runs or 0

    return {
        "matches": matches or 0,
        "balls": balls,
        "runs": runs,
        "fours": fours or 0,
        "sixes": sixes or 0,
        "dots": dots or 0,
        "dismissals": dismissals or 0,
        "strike_rate": round(runs * 100.0 / balls, 1) if balls else None,
        "dot_pct": round(dots * 100.0 / balls, 1) if balls else None,
    }


def print_text(
    batters: dict[str, str | None],
    bowlers: dict[str, str | None],
    results: dict[tuple[str, str], dict],
    since_year: int,
    phase: str | None,
) -> None:
    """Print matchup grid as a readable table."""
    phase_label = f" ({phase})" if phase else ""
    print("=" * 100)
    print(f"Batter vs Bowler — IPL since {since_year}{phase_label}")
    print("=" * 100)
    print("Columns: balls/runs (SR) — 4s/6s — dismissals")
    print()

    # Header row: bowler names
    bowler_names = [n for n, pid in bowlers.items() if pid is not None]
    col_width = 18
    name_col = 22
    header = " " * name_col + "".join(f"{n[:col_width-1]:<{col_width}}" for n in bowler_names)
    print(header)
    print("-" * (name_col + col_width * len(bowler_names)))

    for batter_name, batter_id in batters.items():
        if batter_id is None:
            print(f"{batter_name + ' (unresolved)':<{name_col}}")
            continue

        row_str = f"{batter_name:<{name_col}}"
        for bowler_name in bowler_names:
            bowler_id = bowlers[bowler_name]
            key = (batter_name, bowler_name)
            stats = results.get(key, {})

            balls = stats.get("balls", 0)
            runs = stats.get("runs", 0)
            sr = stats.get("strike_rate")
            fours = stats.get("fours", 0)
            sixes = stats.get("sixes", 0)
            dismissals = stats.get("dismissals", 0)

            if balls == 0:
                cell = "—"
            else:
                sr_str = f"{sr:.0f}" if sr else "-"
                cell = f"{balls}b/{runs}r({sr_str}) {fours}/{sixes} w:{dismissals}"
            row_str += f"{cell:<{col_width}}"
        print(row_str)

    print()

    # Unresolved names callout
    unresolved_batters = [n for n, pid in batters.items() if pid is None]
    unresolved_bowlers = [n for n, pid in bowlers.items() if pid is None]
    if unresolved_batters:
        print(f"Unresolved batters: {unresolved_batters}")
    if unresolved_bowlers:
        print(f"Unresolved bowlers: {unresolved_bowlers}")


def print_json(
    batters: dict[str, str | None],
    bowlers: dict[str, str | None],
    results: dict[tuple[str, str], dict],
    since_year: int,
    phase: str | None,
) -> None:
    """Emit structured JSON."""
    output = {
        "since_year": since_year,
        "phase": phase,
        "unresolved_batters": [n for n, pid in batters.items() if pid is None],
        "unresolved_bowlers": [n for n, pid in bowlers.items() if pid is None],
        "matchups": [
            {
                "batter": batter_name,
                "bowler": bowler_name,
                **results[(batter_name, bowler_name)],
            }
            for batter_name, batter_id in batters.items()
            if batter_id is not None
            for bowler_name, bowler_id in bowlers.items()
            if bowler_id is not None
        ],
    }
    print(json.dumps(output, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batter-vs-bowler matchup analysis.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--batters", help="Comma-separated batter names")
    parser.add_argument("--batters-file", help="File with one batter name per line")
    parser.add_argument("--bowlers", help="Comma-separated bowler names")
    parser.add_argument("--bowlers-file", help="File with one bowler name per line")
    parser.add_argument(
        "--since-year", type=int, default=2020,
        help="Only include matches from this year onward (default: 2020)"
    )
    parser.add_argument(
        "--phase", choices=["powerplay", "middle", "death"],
        help="Restrict to a specific match phase"
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text", help="Output format"
    )
    args = parser.parse_args()

    batter_names = parse_names(args.batters, args.batters_file)
    bowler_names = parse_names(args.bowlers, args.bowlers_file)

    if not batter_names or not bowler_names:
        parser.error("Both --batters/--batters-file and --bowlers/--bowlers-file are required")

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the ETL first.", file=sys.stderr)
        return 1

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        batters = resolve_players(conn, batter_names)
        bowlers = resolve_players(conn, bowler_names)

        results: dict[tuple[str, str], dict] = {}
        for batter_name, batter_id in batters.items():
            if batter_id is None:
                continue
            for bowler_name, bowler_id in bowlers.items():
                if bowler_id is None:
                    continue
                results[(batter_name, bowler_name)] = fetch_matchup(
                    conn, batter_id, bowler_id, args.since_year, args.phase
                )

        if args.format == "json":
            print_json(batters, bowlers, results, args.since_year, args.phase)
        else:
            print_text(batters, bowlers, results, args.since_year, args.phase)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())