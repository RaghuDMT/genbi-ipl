"""Diagnose raw season values from Cricsheet JSON files."""
from pathlib import Path
from collections import Counter
from etl.extract import extract_matches


def main() -> None:
    matches = extract_matches(Path("data/raw"))

    raw_seasons = Counter(
        str(m.get("info", {}).get("season", "MISSING"))
        for m in matches
        if m.get("info", {}).get("gender") == "male"
    )

    print("Raw season values from Cricsheet (male matches):")
    for season, count in sorted(raw_seasons.items()):
        print(f"  {season:<15} {count:>4} matches")


if __name__ == "__main__":
    main()