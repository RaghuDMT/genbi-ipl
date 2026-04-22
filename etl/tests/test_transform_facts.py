from __future__ import annotations

import pytest

from etl.transform_facts import (
    build_fact_ball,
    classify_match_phase,
    extract_extras_type,
    get_bowling_team,
    is_boundary_four,
    is_boundary_six,
    is_bowler_wicket,
    is_dot_ball,
    is_legal_delivery,
    is_wicket,
    resolve_player_id,
)


def _make_match() -> dict:
    """Minimal but realistic Cricsheet-shaped match with varied deliveries."""
    return {
        "match_id": "test_match_001",
        "meta": {"data_version": "1.1.0"},
        "info": {
            "event": {"name": "Indian Premier League"},
            "gender": "male",
            "teams": ["Mumbai Indians", "Chennai Super Kings"],
            "registry": {
                "people": {
                    "RG Sharma": "uuid-rg-sharma",
                    "SA Yadav": "uuid-sa-yadav",
                    "JJ Bumrah": "uuid-jj-bumrah",
                    "MS Dhoni": "uuid-ms-dhoni",
                    "DL Chahar": "uuid-dl-chahar",
                    "RD Gaikwad": "uuid-rd-gaikwad",
                },
            },
        },
        "innings": [
            {
                "team": "Mumbai Indians",
                "overs": [
                    {
                        "over": 0,
                        "deliveries": [
                            {
                                "batter": "RG Sharma",
                                "bowler": "DL Chahar",
                                "non_striker": "SA Yadav",
                                "runs": {"batter": 0, "extras": 0, "total": 0},
                            },
                            {
                                "batter": "RG Sharma",
                                "bowler": "DL Chahar",
                                "non_striker": "SA Yadav",
                                "runs": {"batter": 4, "extras": 0, "total": 4},
                            },
                            {
                                "batter": "RG Sharma",
                                "bowler": "DL Chahar",
                                "non_striker": "SA Yadav",
                                "runs": {"batter": 6, "extras": 0, "total": 6},
                            },
                            {
                                "batter": "RG Sharma",
                                "bowler": "DL Chahar",
                                "non_striker": "SA Yadav",
                                "runs": {"batter": 0, "extras": 1, "total": 1},
                                "extras": {"wides": 1},
                            },
                            {
                                "batter": "RG Sharma",
                                "bowler": "DL Chahar",
                                "non_striker": "SA Yadav",
                                "runs": {"batter": 0, "extras": 0, "total": 0},
                                "wickets": [
                                    {
                                        "kind": "caught",
                                        "player_out": "RG Sharma",
                                        "fielders": [{"name": "MS Dhoni"}],
                                    }
                                ],
                            },
                            {
                                "batter": "SA Yadav",
                                "bowler": "DL Chahar",
                                "non_striker": "RG Sharma",
                                "runs": {"batter": 4, "extras": 0, "total": 4},
                                "non_boundary": True,
                            },
                        ],
                    },
                    {
                        "over": 15,
                        "deliveries": [
                            {
                                "batter": "SA Yadav",
                                "bowler": "DL Chahar",
                                "non_striker": "RD Gaikwad",
                                "runs": {"batter": 1, "extras": 0, "total": 1},
                                "wickets": [{"kind": "run out", "player_out": "RD Gaikwad"}],
                            },
                        ],
                    },
                ],
            },
            {
                "team": "Chennai Super Kings",
                "overs": [
                    {
                        "over": 6,
                        "deliveries": [
                            {
                                "batter": "MS Dhoni",
                                "bowler": "JJ Bumrah",
                                "non_striker": "RD Gaikwad",
                                "runs": {"batter": 2, "extras": 0, "total": 2},
                            }
                        ],
                    }
                ],
            },
        ],
    }


def test_classify_match_phase_boundaries() -> None:
    assert classify_match_phase(1) == "powerplay"
    assert classify_match_phase(6) == "powerplay"
    assert classify_match_phase(7) == "middle"
    assert classify_match_phase(15) == "middle"
    assert classify_match_phase(16) == "death"
    assert classify_match_phase(20) == "death"
    assert classify_match_phase(21) == "death"


def test_resolve_player_id_found() -> None:
    registry = _make_match()["info"]["registry"]["people"]

    assert resolve_player_id("RG Sharma", registry) == "uuid-rg-sharma"


def test_resolve_player_id_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[tuple[str, dict]] = []

    class DummyLogger:
        def warning(self, event: str, **kwargs: object) -> None:
            warnings.append((event, kwargs))

    monkeypatch.setattr("etl.transform_facts.logger", DummyLogger())

    player_id = resolve_player_id("Unknown Player", {})

    assert player_id is None
    assert warnings == [("Unresolvable player name", {"player_name": "Unknown Player"})]


def test_extract_extras_type_variants() -> None:
    assert extract_extras_type({"extras": {"wides": 1}}) == "wides"
    assert extract_extras_type({"extras": {"noballs": 1}}) == "noballs"
    assert extract_extras_type({"extras": {"byes": 2}}) == "byes"
    assert extract_extras_type({"extras": {"legbyes": 1}}) == "legbyes"
    assert extract_extras_type({"extras": {"wides": 1, "byes": 2}}) == "wides"
    assert extract_extras_type({"runs": {"total": 0}}) is None


def test_is_legal_delivery() -> None:
    assert not is_legal_delivery({"extras": {"wides": 1}})
    assert not is_legal_delivery({"extras": {"noballs": 1}})
    assert is_legal_delivery({"extras": {"legbyes": 1}})
    assert is_legal_delivery({"extras": {"byes": 1}})
    assert is_legal_delivery({"runs": {"total": 0}})


def test_is_dot_ball() -> None:
    assert is_dot_ball({"runs": {"total": 0}})
    assert not is_dot_ball({"runs": {"total": 4}})
    assert not is_dot_ball({"runs": {"total": 0}, "wickets": [{"kind": "caught"}]})
    assert not is_dot_ball({"runs": {"total": 0}, "extras": {"wides": 1}})


def test_is_boundary_four_and_six() -> None:
    assert is_boundary_four({"runs": {"batter": 4}})
    assert not is_boundary_four({"runs": {"batter": 4}, "non_boundary": True})
    assert is_boundary_six({"runs": {"batter": 6}})
    assert not is_boundary_six({"runs": {"batter": 6}, "non_boundary": True})


def test_is_wicket_and_bowler_wicket() -> None:
    caught_delivery = {"wickets": [{"kind": "caught"}]}
    run_out_delivery = {"wickets": [{"kind": "run out"}]}

    assert is_wicket(caught_delivery)
    assert is_bowler_wicket(caught_delivery)
    assert is_wicket(run_out_delivery)
    assert not is_bowler_wicket(run_out_delivery)


def test_get_bowling_team_valid_and_invalid() -> None:
    assert (
        get_bowling_team("Mumbai Indians", ["Mumbai Indians", "Chennai Super Kings"])
        == "Chennai Super Kings"
    )

    with pytest.raises(ValueError):
        get_bowling_team("Royal Challengers Bangalore", ["Mumbai Indians", "Chennai Super Kings"])


def test_build_fact_ball_row_count() -> None:
    rows = build_fact_ball([_make_match()])

    assert len(rows) == 8


def test_build_fact_ball_over_numbers_are_1_indexed() -> None:
    rows = build_fact_ball([_make_match()])

    assert rows[0]["over_number"] == 1


def test_build_fact_ball_computes_match_phase() -> None:
    rows = build_fact_ball([_make_match()])

    assert all(row["match_phase"] == "powerplay" for row in rows[:6])
    assert rows[6]["match_phase"] == "death"
    assert rows[7]["match_phase"] == "middle"


def test_build_fact_ball_resolves_player_ids() -> None:
    rows = build_fact_ball([_make_match()])

    assert rows[0]["batter_id"] == "uuid-rg-sharma"


def test_build_fact_ball_bowling_team() -> None:
    rows = build_fact_ball([_make_match()])

    assert rows[0]["bowling_team"] == "Chennai Super Kings"


def test_build_fact_ball_delivery_sequence_resets_per_innings() -> None:
    rows = build_fact_ball([_make_match()])

    assert rows[0]["delivery_sequence"] == 1
    assert rows[7]["innings_number"] == 2
    assert rows[7]["delivery_sequence"] == 1


def test_build_fact_ball_handles_empty_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[tuple[str, dict]] = []

    class DummyLogger:
        def warning(self, event: str, **kwargs: object) -> None:
            warnings.append((event, kwargs))

        def info(self, event: str, **kwargs: object) -> None:
            return None

    monkeypatch.setattr("etl.transform_facts.logger", DummyLogger())

    match = _make_match()
    match["info"]["registry"]["people"] = {}

    rows = build_fact_ball([match])

    assert len(rows) == 8
    assert all(row["batter_id"] is None for row in rows)
    assert all(row["bowler_id"] is None for row in rows)
    assert all(row["non_striker_id"] is None for row in rows)

    unresolved_names = {
        payload["player_name"]
        for event, payload in warnings
        if event == "Unresolvable player name"
    }
    assert unresolved_names == {
        "RG Sharma",
        "SA Yadav",
        "DL Chahar",
        "MS Dhoni",
        "RD Gaikwad",
        "JJ Bumrah",
    }
    assert len(unresolved_names) == 6
