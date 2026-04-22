
from __future__ import annotations

import json
from pathlib import Path

from etl.extract import extract_matches, is_ipl_match, parse_match_file


def make_minimal_match(event_name: str = "Indian Premier League", gender: str = "male") -> dict:
    return {
        "meta": {"data_version": "1.1.0"},
        "info": {
            "event": {"name": event_name},
            "gender": gender,
            "match_type": "T20",
            "dates": ["2023-04-01"],
            "teams": ["Team A", "Team B"],
        },
        "innings": [
            {
                "team": "Team A",
                "overs": [
                    {
                        "over": 0,
                        "deliveries": [
                            {
                                "batter": "Player 1",
                                "bowler": "Player 2",
                                "non_striker": "Player 3",
                                "runs": {"batter": 1, "extras": 0, "total": 1},
                            },
                            {
                                "batter": "Player 1",
                                "bowler": "Player 2",
                                "non_striker": "Player 3",
                                "runs": {"batter": 4, "extras": 0, "total": 4},
                            },
                            {
                                "batter": "Player 1",
                                "bowler": "Player 2",
                                "non_striker": "Player 3",
                                "runs": {"batter": 0, "extras": 0, "total": 0},
                            },
                        ],
                    }
                ],
            }
        ],
    }


def test_is_ipl_match_true_for_full_name() -> None:
    assert is_ipl_match(make_minimal_match(event_name="Indian Premier League"))


def test_is_ipl_match_true_for_short_name() -> None:
    assert is_ipl_match(make_minimal_match(event_name="IPL"))


def test_is_ipl_match_true_for_wpl() -> None:
    assert is_ipl_match(make_minimal_match(event_name="Women's Premier League", gender="female"))


def test_is_ipl_match_false_for_non_ipl() -> None:
    assert not is_ipl_match(make_minimal_match(event_name="Big Bash League"))


def test_parse_match_file_valid_json(tmp_path: Path) -> None:
    fixture = make_minimal_match()
    filepath = tmp_path / "valid_match.json"
    filepath.write_text(json.dumps(fixture), encoding="utf-8")

    parsed = parse_match_file(filepath)

    assert parsed is not None
    assert parsed.get("meta", {}).get("data_version") == "1.1.0"
    assert parsed.get("info", {}).get("gender") == "male"


def test_extract_matches_empty_directory(tmp_path: Path) -> None:
    assert extract_matches(tmp_path) == []


def test_extract_matches_skips_malformed_json_with_warning(
    tmp_path: Path, monkeypatch
) -> None:
    malformed_path = tmp_path / "broken.json"
    malformed_path.write_text("{not-valid-json", encoding="utf-8")

    warnings: list[tuple[str, dict]] = []

    class DummyLogger:
        def warning(self, event: str, **kwargs: object) -> None:
            warnings.append((event, kwargs))

    monkeypatch.setattr("etl.extract.logger", DummyLogger())

    matches = extract_matches(tmp_path)

    assert matches == []
    assert warnings
    assert warnings[0][0] == "Failed to parse match file"


def test_extract_matches_adds_match_id(tmp_path: Path) -> None:
    fixture = make_minimal_match()
    filepath = tmp_path / "12345.json"
    filepath.write_text(json.dumps(fixture), encoding="utf-8")

    matches = extract_matches(tmp_path)

    assert len(matches) == 1
    assert matches[0]["match_id"] == "12345"
