from __future__ import annotations

from typing import Any

import structlog


BOWLER_CREDITED_WICKET_KINDS = {
    "bowled",
    "caught",
    "lbw",
    "stumped",
    "caught and bowled",
    "hit wicket",
}

EXTRAS_TYPE_PRIORITY = ("wides", "noballs", "byes", "legbyes", "penalty")
logger = structlog.get_logger(__name__)


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def classify_match_phase(over_number: int) -> str:
    """Classify a 1-indexed over number into a T20 match phase.

    Args:
        over_number: A 1-indexed over number.

    Returns:
        ``"powerplay"`` for overs 1-6, ``"middle"`` for overs 7-15,
        otherwise ``"death"``.
    """
    if 1 <= over_number <= 6:
        return "powerplay"
    if 7 <= over_number <= 15:
        return "middle"
    return "death"


def resolve_player_id(name: str, registry: dict[str, str]) -> str | None:
    """Resolve a player UUID from a Cricsheet registry mapping.

    Args:
        name: Player name as it appears in the match payload.
        registry: ``info.registry.people`` mapping of name to UUID.

    Returns:
        The player UUID if found, otherwise ``None``.
    """
    player_name = str(name).strip()
    if not player_name:
        logger.warning("Unresolvable player name", player_name=player_name)
        return None

    player_id = registry.get(player_name)
    if player_id:
        return player_id

    logger.warning("Unresolvable player name", player_name=player_name)
    return None


def extract_extras_type(delivery: dict) -> str | None:
    """Return the preferred extras type for a delivery.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        The highest-priority extras key present, or ``None`` when no extras
        type is recorded.
    """
    extras = _as_dict(delivery.get("extras"))
    for extras_type in EXTRAS_TYPE_PRIORITY:
        if extras_type in extras:
            return extras_type
    return None


def is_legal_delivery(delivery: dict) -> bool:
    """Return whether a delivery counts as legal.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``False`` for wides and no-balls, otherwise ``True``.
    """
    extras = _as_dict(delivery.get("extras"))
    return "wides" not in extras and "noballs" not in extras


def is_wicket(delivery: dict) -> bool:
    """Return whether a delivery contains a wicket.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``True`` when the delivery has a non-empty wickets array.
    """
    return bool(_as_list(delivery.get("wickets")))


def is_dot_ball(delivery: dict) -> bool:
    """Return whether a delivery is a legal dot ball without a wicket.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``True`` when total runs are zero, the ball is legal, and no wicket
        occurred.
    """
    runs = _as_dict(delivery.get("runs"))
    total_runs = runs.get("total", 0)
    return total_runs == 0 and is_legal_delivery(delivery) and not is_wicket(delivery)


def is_boundary_four(delivery: dict) -> bool:
    """Return whether a delivery is a credited boundary four.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``True`` when the batter scored four and the shot was not marked as a
        non-boundary.
    """
    runs = _as_dict(delivery.get("runs"))
    return runs.get("batter") == 4 and delivery.get("non_boundary") is not True


def is_boundary_six(delivery: dict) -> bool:
    """Return whether a delivery is a credited six.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``True`` when the batter scored six and the shot was not marked as a
        non-boundary.
    """
    runs = _as_dict(delivery.get("runs"))
    return runs.get("batter") == 6 and delivery.get("non_boundary") is not True


def is_bowler_wicket(delivery: dict) -> bool:
    """Return whether the first wicket is credited to the bowler.

    Args:
        delivery: A Cricsheet delivery object.

    Returns:
        ``True`` when the delivery has a wicket and the first wicket kind is
        bowler-credited.
    """
    if not is_wicket(delivery):
        return False

    first_wicket = _as_dict(_as_list(delivery.get("wickets"))[0])
    wicket_kind = str(first_wicket.get("kind", "")).strip().lower()
    return wicket_kind in BOWLER_CREDITED_WICKET_KINDS


def get_bowling_team(batting_team: str, teams: list[str]) -> str:
    """Determine the bowling team for an innings.

    Args:
        batting_team: The innings batting team.
        teams: Match team list, expected to contain exactly two distinct teams.

    Returns:
        The non-batting team.

    Raises:
        ValueError: If the batting team cannot be uniquely resolved.
    """
    normalized_batting_team = str(batting_team).strip()
    normalized_teams = [str(team).strip() for team in teams if str(team).strip()]

    if normalized_teams.count(normalized_batting_team) != 1 or len(normalized_teams) != 2:
        raise ValueError(
            f"Could not determine bowling team for batting_team={normalized_batting_team!r}"
        )

    for team_name in normalized_teams:
        if team_name != normalized_batting_team:
            return team_name

    raise ValueError(f"Could not determine bowling team for batting_team={normalized_batting_team!r}")


def build_fact_ball(matches: list[dict]) -> list[dict]:
    """Flatten parsed Cricsheet matches into one row per delivery.

    Args:
        matches: Parsed Cricsheet match payloads with top-level ``match_id``.

    Returns:
        A list of dicts conforming to the fact_ball schema. Matches without a
        two-team ``info.teams`` array are skipped. Unresolvable player names are
        logged but the delivery row is still produced.
    """
    fact_rows: list[dict] = []
    processed_matches = 0

    for match in matches:
        match_id = str(match.get("match_id", "")).strip()

        try:
            info = _as_dict(match.get("info"))
            teams = [str(team).strip() for team in _as_list(info.get("teams")) if str(team).strip()]
            if len(teams) != 2:
                logger.warning(
                    "Skipping match with invalid teams array",
                    match_id=match_id,
                    teams=teams,
                )
                continue

            registry_raw = _as_dict(_as_dict(info.get("registry")).get("people"))
            registry = {str(name).strip(): str(player_id).strip() for name, player_id in registry_raw.items()}
            innings_list = _as_list(match.get("innings"))
            processed_matches += 1

            resolved_cache: dict[str, str | None] = {}

            def resolve_once(name: object) -> str | None:
                player_name = str(name).strip()
                if player_name in resolved_cache:
                    return resolved_cache[player_name]
                player_id = resolve_player_id(player_name, registry)
                resolved_cache[player_name] = player_id
                return player_id

            for innings_number, innings in enumerate(innings_list, start=1):
                innings_data = _as_dict(innings)
                batting_team = str(innings_data.get("team", "")).strip()
                bowling_team = get_bowling_team(batting_team, teams)
                delivery_sequence = 0

                for over in _as_list(innings_data.get("overs")):
                    over_data = _as_dict(over)
                    over_number = int(over_data.get("over", 0)) + 1
                    match_phase = classify_match_phase(over_number)

                    for ball_in_over, delivery in enumerate(_as_list(over_data.get("deliveries")), start=1):
                        delivery_data = _as_dict(delivery)
                        if "replacements" in delivery_data:
                            logger.warning(
                                "Skipping delivery with replacements",
                                match_id=match_id,
                                innings_number=innings_number,
                                over_number=over_number,
                                ball_in_over=ball_in_over,
                            )
                            continue

                        delivery_sequence += 1

                        batter_name = str(delivery_data.get("batter", "")).strip()
                        bowler_name = str(delivery_data.get("bowler", "")).strip()
                        non_striker_name = str(delivery_data.get("non_striker", "")).strip()
                        runs = _as_dict(delivery_data.get("runs"))
                        wickets = _as_list(delivery_data.get("wickets"))
                        first_wicket = _as_dict(wickets[0]) if wickets else {}
                        fielders = _as_list(first_wicket.get("fielders"))
                        first_fielder = _as_dict(fielders[0]) if fielders else {}
                        player_out_name = str(first_wicket.get("player_out", "")).strip() or None
                        fielder_name = str(first_fielder.get("name", "")).strip() or None

                        fact_rows.append(
                            {
                                "match_id": match_id,
                                "innings_number": innings_number,
                                "delivery_sequence": delivery_sequence,
                                "over_number": over_number,
                                "ball_in_over": ball_in_over,
                                "match_phase": match_phase,
                                "batting_team": batting_team,
                                "bowling_team": bowling_team,
                                "batter_name": batter_name,
                                "batter_id": resolve_once(batter_name),
                                "bowler_name": bowler_name,
                                "bowler_id": resolve_once(bowler_name),
                                "non_striker_name": non_striker_name,
                                "non_striker_id": resolve_once(non_striker_name),
                                "batter_runs": runs.get("batter", 0),
                                "extras_runs": runs.get("extras", 0),
                                "total_runs": runs.get("total", 0),
                                "extras_type": extract_extras_type(delivery_data),
                                "is_legal_delivery": is_legal_delivery(delivery_data),
                                "is_dot_ball": is_dot_ball(delivery_data),
                                "is_boundary_four": is_boundary_four(delivery_data),
                                "is_boundary_six": is_boundary_six(delivery_data),
                                "is_wicket": is_wicket(delivery_data),
                                "is_bowler_wicket": is_bowler_wicket(delivery_data),
                                "wicket_kind": str(first_wicket.get("kind", "")).strip() or None,
                                "player_out_name": player_out_name,
                                "player_out_id": resolve_once(player_out_name) if player_out_name else None,
                                "fielder_name": fielder_name,
                                "fielder_id": resolve_once(fielder_name) if fielder_name else None,
                            }
                        )
        except Exception as exc:
            logger.warning("Failed to transform match into fact rows", match_id=match_id, error=str(exc))
            continue

    logger.info(
        "Built fact_ball records",
        total_matches_processed=processed_matches,
        total_rows_produced=len(fact_rows),
    )
    return fact_rows
