
from __future__ import annotations

from datetime import date

import pytest

from etl.transform import (
    build_dim_match,
    build_dim_player,
    build_dim_season,
    build_dim_team,
    build_dim_venue,
    generate_venue_id,
    parse_season_year,
)


@pytest.fixture
def sample_matches() -> list[dict]:
    return [
        {
            "match_id": "1001",
            "info": {
                "event": {"name": "Indian Premier League"},
                "gender": "male",
                "season": "2023",
                "dates": ["2023-04-01"],
                "teams": ["Royal Challengers Bangalore", "Mumbai Indians"],
                "venue": "M Chinnaswamy Stadium",
                "city": "Bengaluru",
                "players": {
                    "Royal Challengers Bangalore": ["V Kohli", "F du Plessis"],
                    "Mumbai Indians": ["RG Sharma", "JJ Bumrah"],
                },
                "toss": {"winner": "Mumbai Indians", "decision": "field"},
                "registry": {
                    "people": {
                        "V Kohli": "player-virat",
                        "F du Plessis": "player-faf",
                        "RG Sharma": "player-rohit",
                        "JJ Bumrah": "player-bumrah",
                        "Virat Kohli": "player-virat",
                    }
                },
                "outcome": {"winner": "Royal Challengers Bangalore", "by": {"runs": 7}},
                "player_of_match": ["V Kohli"],
            },
        },
        {
            "match_id": "1002",
            "info": {
                "event": {"name": "Indian Premier League"},
                "gender": "male",
                "season": "2023/24",
                "dates": ["2023-04-05"],
                "teams": ["Royal Challengers Bangalore", "Chennai Super Kings"],
                "venue": "M Chinnaswamy Stadium",
                "players": {
                    "Royal Challengers Bangalore": ["Virat Kohli", "F du Plessis"],
                    "Chennai Super Kings": ["MS Dhoni", "RD Gaikwad"],
                },
                "toss": {"winner": "Chennai Super Kings", "decision": "bat"},
                "registry": {
                    "people": {
                        "Virat Kohli": "player-virat",
                        "F du Plessis": "player-faf",
                        "MS Dhoni": "player-dhoni",
                        "RD Gaikwad": "player-gaikwad",
                    }
                },
                "outcome": {"winner": "Chennai Super Kings", "by": {"wickets": 5}},
                "player_of_match": ["MS Dhoni"],
            },
        },
        {
            "match_id": "2001",
            "info": {
                "event": {"name": "Women's Premier League"},
                "gender": "female",
                "season": "2023",
                "dates": ["2023-03-10"],
                "teams": ["Mumbai Indians Women", "Delhi Capitals Women"],
                "venue": "Brabourne Stadium",
                "city": "Mumbai",
                "players": {
                    "Mumbai Indians Women": ["HC Kaur"],
                    "Delhi Capitals Women": ["S Verma"],
                },
                "registry": {
                    "people": {
                        "HC Kaur": "player-harmanpreet",
                        "S Verma": "player-shafali",
                    }
                },
                "outcome": {"result": "no result", "method": "D/L"},
            },
        },
        {
            "match_id": "2002",
            "info": {
                "event": {"name": "Women's Premier League"},
                "gender": "female",
                "season": "2023",
                "dates": ["2023-03-12"],
                "teams": ["Mumbai Indians Women", "UP Warriorz Women"],
                "venue": "Brabourne Stadium",
                "players": {
                    "Mumbai Indians Women": ["HC Kaur"],
                    "UP Warriorz Women": ["TM McGrath"],
                },
                "registry": {
                    "people": {
                        "HC Kaur": "player-harmanpreet",
                        "TM McGrath": "player-mcgrath",
                    }
                },
                "outcome": {"result": "tie"},
            },
        },
    ]


def test_parse_season_year_variants() -> None:
    assert parse_season_year("2023") == 2023
    assert parse_season_year("2007/08") == 2008
    assert parse_season_year("2009/10") == 2010
    assert parse_season_year("2023/24") == 2024


def test_parse_season_year_rejects_invalid_input() -> None:
    with pytest.raises(ValueError):
        parse_season_year("")

    with pytest.raises(ValueError):
        parse_season_year("23/24")


def test_parse_season_year_splits_merged_seasons() -> None:
    """2009 and 2009/10 must parse to different years."""
    assert parse_season_year("2009") == 2009
    assert parse_season_year("2009/10") == 2010
    assert parse_season_year("2009") != parse_season_year("2009/10")
    assert parse_season_year("2020/21") == 2020  # override: IPL 2020 played in 2020

def test_parse_season_year_override_takes_precedence():
    """Explicit overrides beat the trailing-year formula."""
    # 2020/21 trailing year would be 2021, but override says 2020
    assert parse_season_year("2020/21") == 2020
    # 2007/08 has no override, so trailing year applies
    assert parse_season_year("2007/08") == 2008

def test_build_dim_player_aggregates_name_variants(sample_matches: list[dict]) -> None:
    players = build_dim_player(sample_matches)

    virat = next(player for player in players if player["player_id"] == "player-virat")

    assert virat["player_name"] == "Virat Kohli"
    assert virat["name_variants"] == ["V Kohli", "Virat Kohli"]
    assert virat["gender"] == "male"


def test_build_dim_player_includes_registry_only_players(sample_matches: list[dict]) -> None:
    registry_only_name = "Sub Fielder"
    registry_only_id = "player-sub-fielder"
    sample_matches[0]["info"]["registry"]["people"][registry_only_name] = registry_only_id

    players = build_dim_player(sample_matches)

    registry_only_player = next(player for player in players if player["player_id"] == registry_only_id)

    assert registry_only_player["player_name"] == registry_only_name
    assert registry_only_player["name_variants"] == [registry_only_name]
    assert registry_only_player["gender"] == "male"


def test_build_dim_team_deduplicates_names(sample_matches: list[dict]) -> None:
    teams = build_dim_team(sample_matches)

    assert {team["team_name"] for team in teams} == {
        "Chennai Super Kings",
        "Delhi Capitals Women",
        "Mumbai Indians",
        "Mumbai Indians Women",
        "Royal Challengers Bangalore",
        "UP Warriorz Women",
    }


def test_build_dim_venue_handles_missing_city(sample_matches: list[dict]) -> None:
    venues = build_dim_venue(sample_matches)

    chinnaswamy = next(venue for venue in venues if venue["venue_name"] == "M Chinnaswamy Stadium")
    brabourne = next(venue for venue in venues if venue["venue_name"] == "Brabourne Stadium")

    assert chinnaswamy["city"] == "Bengaluru"
    assert brabourne["city"] == "Mumbai"
    assert chinnaswamy["venue_id"] == generate_venue_id("M Chinnaswamy Stadium")


def test_build_dim_match_parses_outcomes(sample_matches: list[dict]) -> None:
    venues = build_dim_venue(sample_matches)
    venue_id_map = {venue["venue_name"]: venue["venue_id"] for venue in venues}

    match_records = build_dim_match(sample_matches, venue_id_map)

    by_runs = next(match for match in match_records if match["match_id"] == "1001")
    by_wickets = next(match for match in match_records if match["match_id"] == "1002")
    no_result = next(match for match in match_records if match["match_id"] == "2001")
    tie_match = next(match for match in match_records if match["match_id"] == "2002")

    assert by_runs["winner_team_name"] == "Royal Challengers Bangalore"
    assert by_runs["toss_winner_team_name"] == "Mumbai Indians"
    assert by_runs["toss_decision"] == "field"
    assert by_runs["win_by_runs"] == 7
    assert by_runs["win_by_wickets"] is None
    assert by_runs["player_of_match_id"] == "player-virat"

    assert by_wickets["winner_team_name"] == "Chennai Super Kings"
    assert by_wickets["toss_winner_team_name"] == "Chennai Super Kings"
    assert by_wickets["toss_decision"] == "bat"
    assert by_wickets["win_by_runs"] is None
    assert by_wickets["win_by_wickets"] == 5

    assert no_result["result"] == "no result"
    assert no_result["method"] == "D/L"
    assert no_result["toss_winner_team_name"] is None
    assert no_result["winner_team_name"] is None

    assert tie_match["result"] == "tie"
    assert tie_match["toss_decision"] is None
    assert tie_match["winner_team_name"] is None


def test_build_dim_season_aggregates_matches(sample_matches: list[dict]) -> None:
    sample_matches[1]["info"]["season"] = "2023"
    venues = build_dim_venue(sample_matches)
    venue_id_map = {venue["venue_name"]: venue["venue_id"] for venue in venues}
    match_records = build_dim_match(sample_matches, venue_id_map)

    seasons = build_dim_season(match_records)

    assert seasons == [
        {
            "season_year": 2023,
            "season_label": "WPL 2023",
            "gender": "female",
            "total_matches": 2,
            "start_date": date(2023, 3, 10),
            "end_date": date(2023, 3, 12),
        },
        {
            "season_year": 2023,
            "season_label": "IPL 2023",
            "gender": "male",
            "total_matches": 2,
            "start_date": date(2023, 4, 1),
            "end_date": date(2023, 4, 5),
        },
    ]
