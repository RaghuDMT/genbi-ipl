"""Temporary diagnostic for innings_number CHECK violation. Delete after use."""
from pathlib import Path
from collections import Counter
from etl.extract import extract_matches
from etl.transform_facts import build_fact_ball


def main() -> None:
    matches = extract_matches(Path("data/raw"))
    facts = build_fact_ball(matches)

    innings_dist = Counter(r["innings_number"] for r in facts)
    print("innings_number distribution:")
    for k, v in sorted(innings_dist.items()):
        print(f"  innings {k}: {v} rows")

    bad_rows = [r for r in facts if r["innings_number"] > 4]
    print(f"Rows with innings_number > 4: {len(bad_rows)}")
    if bad_rows:
        for r in bad_rows[:3]:
            print(f"  match_id={r['match_id']}, innings_number={r['innings_number']}")


if __name__ == "__main__":
    main()