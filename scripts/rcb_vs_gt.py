"""RCB vs GT match-up analysis using historical IPL data."""
import duckdb


def main() -> None:
    conn = duckdb.connect("data/db/genbi.duckdb", read_only=True)

    # 1. Head-to-head record
    print("=" * 70)
    print("RCB vs GT — Head-to-Head Record")
    print("=" * 70)
    rows = conn.execute("""
        SELECT 
            season_year,
            COUNT(*) AS matches,
            SUM(CASE WHEN winner_team_id = team1_id AND team1_id IN (
                SELECT team_id FROM dim_team WHERE team_name LIKE '%Royal Challengers%'
            ) THEN 1
            WHEN winner_team_id = team2_id AND team2_id IN (
                SELECT team_id FROM dim_team WHERE team_name LIKE '%Royal Challengers%'
            ) THEN 1 ELSE 0 END) AS rcb_wins,
            SUM(CASE WHEN winner_team_id = team1_id AND team1_id IN (
                SELECT team_id FROM dim_team WHERE team_name LIKE '%Gujarat Titans%'
            ) THEN 1
            WHEN winner_team_id = team2_id AND team2_id IN (
                SELECT team_id FROM dim_team WHERE team_name LIKE '%Gujarat Titans%'
            ) THEN 1 ELSE 0 END) AS gt_wins
        FROM dim_match
        WHERE tournament = 'IPL'
          AND team1_id IN (SELECT team_id FROM dim_team WHERE team_name IN ('Royal Challengers Bengaluru', 'Gujarat Titans'))
          AND team2_id IN (SELECT team_id FROM dim_team WHERE team_name IN ('Royal Challengers Bengaluru', 'Gujarat Titans'))
        GROUP BY season_year
        ORDER BY season_year
    """).fetchall()
    for season, matches, rcb, gt in rows:
        print(f"  IPL {season}: {matches} matches  |  RCB {rcb} - {gt} GT")

    # 2. Recent encounters ball-by-ball summary
    print()
    print("=" * 70)
    print("Last 5 RCB vs GT matches")
    print("=" * 70)
    rows = conn.execute("""
        SELECT 
            m.match_date,
            t1.team_name AS team1,
            t2.team_name AS team2,
            tw.team_name AS winner,
            m.win_by_runs,
            m.win_by_wickets,
            v.venue_name
        FROM dim_match m
        JOIN dim_team t1 ON m.team1_id = t1.team_id
        JOIN dim_team t2 ON m.team2_id = t2.team_id
        LEFT JOIN dim_team tw ON m.winner_team_id = tw.team_id
        JOIN dim_venue v ON m.venue_id = v.venue_id
        WHERE t1.team_name IN ('Royal Challengers Bengaluru', 'Gujarat Titans')
          AND t2.team_name IN ('Royal Challengers Bengaluru', 'Gujarat Titans')
        ORDER BY m.match_date DESC
        LIMIT 5
    """).fetchall()
    for d, t1, t2, w, r, wk, v in rows:
        margin = f"by {r} runs" if r else f"by {wk} wickets" if wk else "no result"
        print(f"  {d}: {t1} vs {t2} — {w or 'N/A'} won {margin}  ({v})")

    conn.close()


if __name__ == "__main__":
    main()