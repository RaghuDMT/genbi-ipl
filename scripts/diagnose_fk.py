"""Temporary diagnostic for Phase 1D FK violation. Delete after use."""
from pathlib import Path
from etl.extract import extract_matches
from etl.transform import build_dim_player
from etl.transform_facts import build_fact_ball


def main() -> None:
    matches = extract_matches(Path("data/raw"))
    players = build_dim_player(matches)
    facts = build_fact_ball(matches)

    dim_ids = {p["player_id"] for p in players}

    columns = ["batter_id", "bowler_id", "non_striker_id", "player_out_id", "fielder_id"]
    for col in columns:
        fact_ids = {r[col] for r in facts if r[col] is not None}
        missing = fact_ids - dim_ids
        print(f"{col}: {len(fact_ids)} unique, {len(missing)} missing from dim_player")
        if missing:
            sample_ids = list(missing)[:5]
            print(f"  Sample missing IDs: {sample_ids}")
            name_col = col.replace("_id", "_name")
            for row in facts:
                if row[col] in missing:
                    print(
                        f"  Example row: match_id={row['match_id']}, "
                        f"{col}={row[col]}, {name_col}={row[name_col]}"
                    )
                    break


if __name__ == "__main__":
    main()