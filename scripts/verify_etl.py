"""Post-ETL verification queries. Run after a successful ETL to confirm data quality."""
import duckdb


def main() -> None:
    conn = duckdb.connect("data/db/genbi.duckdb", read_only=True)

    print("=== Row Counts ===")
    for table in ["dim_player", "dim_team", "dim_venue", "dim_match", "dim_season", "fact_ball"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<20} {count:>8}")

    print()
    print("=== Top 10 All-Time Run Scorers (IPL + WPL) ===")
    rows = conn.execute("""
        SELECT batter_name, SUM(batter_runs) AS runs, COUNT(*) AS balls_faced
        FROM fact_ball
        WHERE is_legal_delivery = TRUE
        GROUP BY batter_name
        ORDER BY runs DESC
        LIMIT 10
    """).fetchall()
    for name, runs, balls in rows:
        print(f"  {name:<30} {runs:>6} runs  {balls:>6} balls")

    print()
    print("=== Top 5 Death-Over Run Scorers ===")
    rows = conn.execute("""
        SELECT batter_name, SUM(batter_runs) AS runs
        FROM fact_ball
        WHERE match_phase = 'death' AND is_legal_delivery = TRUE
        GROUP BY batter_name
        ORDER BY runs DESC
        LIMIT 5
    """).fetchall()
    for name, runs in rows:
        print(f"  {name:<30} {runs:>6} runs")

    print()
    print("=== Top 5 Wicket Takers ===")
    rows = conn.execute("""
        SELECT bowler_name, SUM(CASE WHEN is_bowler_wicket THEN 1 ELSE 0 END) AS wickets
        FROM fact_ball
        GROUP BY bowler_name
        ORDER BY wickets DESC
        LIMIT 5
    """).fetchall()
    for name, wickets in rows:
        print(f"  {name:<30} {wickets:>4} wickets")

    print()
    print("=== Season Summary ===")
    rows = conn.execute("""
        SELECT season_label, total_matches, start_date, end_date
        FROM dim_season
        ORDER BY season_year, gender
        LIMIT 25
    """).fetchall()
    for label, matches, start, end in rows:
        print(f"  {label:<20} {matches:>3} matches  {start} to {end}")

    conn.close()
    print()
    print("Verification complete.")


if __name__ == "__main__":
    main()