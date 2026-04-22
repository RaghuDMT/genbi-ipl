
from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import date

import structlog


logger = structlog.get_logger(__name__)


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list:
    return value if isinstance(value, list) else []


def _normalize_name(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _canonical_name(name_counts: Counter[str]) -> str:
    if not name_counts:
        return ""
    return min(
        name_counts,
        key=lambda name: (-name_counts[name], -len(name), name),
    )


def _derive_tournament(event_name: str, gender: str) -> str:
    event_name_lower = event_name.lower()
    if "women" in event_name_lower or "wpl" in event_name_lower or gender == "female":
        return "WPL"
    return "IPL"


def parse_season_year(season: str) -> int:
    """Parse a season label into its starting year.

    Args:
        season: Season value from Cricsheet, such as ``"2023"`` or ``"2023/24"``.

    Returns:
        The starting year as an integer.

    Raises:
        ValueError: If the season string does not begin with a four-digit year.
    """
    season_text = str(season).strip()
    if not season_text:
        raise ValueError("Season value is empty")

    leading_year = season_text.split("/", maxsplit=1)[0].strip()
    if len(leading_year) != 4 or not leading_year.isdigit():
        raise ValueError(f"Unsupported season format: {season!r}")

    return int(leading_year)


def generate_venue_id(venue_name: str) -> str:
    """Generate a stable short identifier for a venue.

    Args:
        venue_name: Venue name.

    Returns:
        An 8-character hexadecimal identifier derived from the venue name.
    """
    normalized_name = str(venue_name).strip()
    return hashlib.md5(normalized_name.encode("utf-8")).hexdigest()[:8]


def generate_team_id(team_name: str) -> str:
    """Generate a stable short identifier for a team.

    Args:
        team_name: Team name.

    Returns:
        An 8-character hexadecimal identifier derived from the team name.
    """
    normalized_name = str(team_name).strip()
    return hashlib.md5(normalized_name.encode("utf-8")).hexdigest()[:8]


def build_dim_player(matches: list[dict]) -> list[dict]:
    """Build player dimension records from parsed match payloads.

    Args:
        matches: Parsed Cricsheet match payloads with top-level ``match_id``.

    Returns:
        Player dimension records keyed by Cricsheet registry UUID.
    """
    player_index: dict[str, dict[str, object]] = {}

    for match in matches:
        info = _as_dict(match.get("info"))
        registry_people = _as_dict(_as_dict(info.get("registry")).get("people"))
        players_by_team = _as_dict(info.get("players"))
        match_gender = str(info.get("gender", "")).strip().lower()
        match_id = str(match.get("match_id", ""))

        for team_players in players_by_team.values():
            for raw_name in _as_list(team_players):
                player_name = _normalize_name(raw_name)
                if not player_name:
                    continue

                player_id = _normalize_name(registry_people.get(player_name))
                if not player_id:
                    logger.warning(
                        "Skipping player without registry UUID",
                        match_id=match_id,
                        player_name=player_name,
                    )
                    continue

                record = player_index.setdefault(
                    player_id,
                    {
                        "name_counts": Counter(),
                        "gender": match_gender or None,
                        "gender_sources": set([match_gender]) if match_gender else set(),
                    },
                )
                name_counts = record["name_counts"]
                if isinstance(name_counts, Counter):
                    name_counts[player_name] += 1

                existing_gender = record.get("gender")
                if existing_gender is None and match_gender:
                    record["gender"] = match_gender

                gender_sources = record.get("gender_sources")
                if isinstance(gender_sources, set) and match_gender:
                    gender_sources.add(match_gender)
                    if existing_gender and existing_gender != match_gender:
                        logger.warning(
                            "Conflicting player genders detected",
                            player_id=player_id,
                            existing_gender=existing_gender,
                            conflicting_gender=match_gender,
                            match_id=match_id,
                        )

    player_records: list[dict] = []
    for player_id, details in sorted(player_index.items()):
        name_counts = details.get("name_counts")
        if not isinstance(name_counts, Counter):
            continue

        canonical_name = _canonical_name(name_counts)
        player_records.append(
            {
                "player_id": player_id,
                "player_name": canonical_name,
                "name_variants": sorted(name_counts),
                "gender": str(details.get("gender") or ""),
            }
        )

    logger.info("Built player records", count=len(player_records))
    return player_records


def build_dim_team(matches: list[dict]) -> list[dict]:
    """Build team dimension records from parsed match payloads.

    Args:
        matches: Parsed Cricsheet match payloads.

    Returns:
        Team dimension records, one per distinct team name.
    """
    team_names: set[str] = set()

    for match in matches:
        info = _as_dict(match.get("info"))
        for raw_team_name in _as_list(info.get("teams")):
            team_name = _normalize_name(raw_team_name)
            if team_name:
                team_names.add(team_name)

    team_records = [
        {
            "team_id": generate_team_id(team_name),
            "team_name": team_name,
            "team_name_variants": [],
        }
        for team_name in sorted(team_names)
    ]

    logger.info("Built team records", count=len(team_records))
    return team_records


def build_dim_venue(matches: list[dict]) -> list[dict]:
    """Build venue dimension records from parsed match payloads.

    Args:
        matches: Parsed Cricsheet match payloads.

    Returns:
        Venue dimension records keyed by stable venue identifiers.
    """
    venue_index: dict[str, str | None] = {}

    for match in matches:
        info = _as_dict(match.get("info"))
        venue_name = _normalize_name(info.get("venue"))
        if not venue_name:
            continue

        city_name = _normalize_name(info.get("city")) or None
        if venue_name not in venue_index or venue_index[venue_name] is None:
            venue_index[venue_name] = city_name

    venue_records = [
        {
            "venue_id": generate_venue_id(venue_name),
            "venue_name": venue_name,
            "city": venue_index[venue_name],
        }
        for venue_name in sorted(venue_index)
    ]

    logger.info("Built venue records", count=len(venue_records))
    return venue_records


def build_dim_match(matches: list[dict], venue_id_map: dict[str, str]) -> list[dict]:
    """Build match dimension records from parsed match payloads.

    Args:
        matches: Parsed Cricsheet match payloads with top-level ``match_id``.
        venue_id_map: Mapping of venue name to stable venue identifier.

    Returns:
        Match dimension records.
    """
    match_records: list[dict] = []

    for match in matches:
        info = _as_dict(match.get("info"))
        outcome = _as_dict(info.get("outcome"))
        outcome_by = _as_dict(outcome.get("by"))
        registry_people = _as_dict(_as_dict(info.get("registry")).get("people"))
        teams = [_normalize_name(team) for team in _as_list(info.get("teams")) if _normalize_name(team)]
        event = info.get("event")
        event_name = ""
        if isinstance(event, dict):
            event_name = _normalize_name(event.get("name"))
        elif event is not None:
            event_name = _normalize_name(event)

        gender = str(info.get("gender", "")).strip().lower()
        season_value = _normalize_name(info.get("season"))
        dates = _as_list(info.get("dates"))
        match_date = _normalize_name(dates[0]) if dates else None
        venue_name = _normalize_name(info.get("venue"))
        player_of_match_list = _as_list(info.get("player_of_match"))
        player_of_match_name = _normalize_name(player_of_match_list[0]) if player_of_match_list else None
        winner_team = _normalize_name(outcome.get("winner")) or None
        result = _normalize_name(outcome.get("result")) or None
        method = _normalize_name(outcome.get("method")) or None

        match_records.append(
            {
                "match_id": _normalize_name(match.get("match_id")),
                "season_year": parse_season_year(season_value),
                "match_date": match_date,
                "gender": gender,
                "tournament": _derive_tournament(event_name, gender),
                "team1_name": teams[0] if len(teams) > 0 else None,
                "team2_name": teams[1] if len(teams) > 1 else None,
                "venue_id": venue_id_map.get(venue_name),
                "winner_team_name": winner_team,
                "win_by_runs": outcome_by.get("runs"),
                "win_by_wickets": outcome_by.get("wickets"),
                "result": result,
                "method": method,
                "player_of_match": player_of_match_name,
                "player_of_match_id": _normalize_name(registry_people.get(player_of_match_name))
                if player_of_match_name
                else None,
            }
        )

    logger.info("Built match records", count=len(match_records))
    return match_records


def build_dim_season(match_records: list[dict]) -> list[dict]:
    """Build season dimension records from match dimension records.

    Args:
        match_records: Output of :func:`build_dim_match`.

    Returns:
        Season dimension records grouped by season year and gender.
    """
    grouped: dict[tuple[int, str], dict[str, object]] = defaultdict(
        lambda: {"total_matches": 0, "start_date": None, "end_date": None}
    )

    for match_record in match_records:
        season_year = int(match_record["season_year"])
        gender = str(match_record.get("gender", ""))
        match_date_raw = match_record.get("match_date")
        match_date = date.fromisoformat(str(match_date_raw)) if match_date_raw else None

        bucket = grouped[(season_year, gender)]
        bucket["total_matches"] = int(bucket["total_matches"]) + 1

        start_date = bucket.get("start_date")
        end_date = bucket.get("end_date")
        if match_date is not None:
            if start_date is None or match_date < start_date:
                bucket["start_date"] = match_date
            if end_date is None or match_date > end_date:
                bucket["end_date"] = match_date

    season_records: list[dict] = []
    for (season_year, gender), details in sorted(grouped.items()):
        tournament = "WPL" if gender == "female" else "IPL"
        season_records.append(
            {
                "season_year": season_year,
                "season_label": f"{tournament} {season_year}",
                "gender": gender,
                "total_matches": details["total_matches"],
                "start_date": details["start_date"],
                "end_date": details["end_date"],
            }
        )

    logger.info("Built season records", count=len(season_records))
    return season_records
