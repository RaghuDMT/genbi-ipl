
from __future__ import annotations

import json
from pathlib import Path

import structlog


SUPPORTED_DATA_VERSIONS = {"1.0.0", "1.1.0"}
SUPPORTED_GENDERS = {"male", "female"}
logger = structlog.get_logger(__name__)


def is_ipl_match(match_data: dict) -> bool:
    """Return whether the match belongs to IPL or WPL datasets."""
    info = match_data.get("info", {})
    event = info.get("event", {})

    event_name = ""
    if isinstance(event, dict):
        event_name = str(event.get("name", ""))
    elif isinstance(event, str):
        event_name = event

    event_name_lower = event_name.lower()
    return any(
        token in event_name_lower
        for token in (
            "indian premier league",
            "ipl",
            "women's premier league",
            "wpl",
        )
    )


def parse_match_file(filepath: Path) -> dict | None:
    try:
        with filepath.open("r", encoding="utf-8") as file_obj:
            parsed = json.load(file_obj)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to parse match file", filepath=str(filepath), error=str(exc))
        return None

    if not isinstance(parsed, dict):
        logger.warning("Malformed match payload: root is not an object", filepath=str(filepath))
        return None

    return parsed


def extract_matches(data_dir: Path) -> list[dict]:
    """Extract supported IPL and WPL match payloads from disk."""
    matches: list[dict] = []

    for filepath in sorted(data_dir.rglob("*.json")):
        match_data = parse_match_file(filepath)
        if match_data is None:
            continue

        meta = match_data.get("meta", {})
        data_version = str(meta.get("data_version", ""))
        if data_version not in SUPPORTED_DATA_VERSIONS:
            logger.warning(
                "Skipping file with unsupported Cricsheet data_version",
                filepath=str(filepath),
                data_version=data_version,
            )
            continue

        if not is_ipl_match(match_data):
            continue

        info = match_data.get("info", {})
        gender = str(info.get("gender", "")).lower()
        if gender not in SUPPORTED_GENDERS:
            logger.warning(
                "Skipping file with unsupported or missing gender",
                filepath=str(filepath),
                gender=gender,
            )
            continue

        match_with_id = dict(match_data)
        match_with_id["match_id"] = filepath.stem
        matches.append(match_with_id)

    return matches
