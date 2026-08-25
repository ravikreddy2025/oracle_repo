"""Minimal ISO-8601 duration parsing for the incremental ``overlap`` setting.

Only the subset a lookback window needs: years/months are rejected because
they are not a fixed number of seconds and an ingestion overlap must be exact.
"""

from __future__ import annotations

import re
from datetime import timedelta

_PATTERN = re.compile(
    r"^P(?!$)(?:(?P<days>\d+)D)?(?:T(?=\d)(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


class DurationParseError(ValueError):
    pass


def parse_duration(text: str | None) -> timedelta:
    """Parse ``PT6H`` / ``P1D`` / ``PT0S`` into a timedelta. ``None`` -> zero."""
    if text is None or text == "":
        return timedelta(0)
    match = _PATTERN.match(str(text))
    if not match:
        raise DurationParseError(
            f"invalid duration {text!r}: expected ISO-8601 like 'PT6H', 'P1D', 'PT30M' "
            f"(years and months are not supported -- they are not a fixed length)"
        )
    parts = {k: int(v) for k, v in match.groupdict(default="0").items()}
    return timedelta(
        days=parts["days"],
        hours=parts["hours"],
        minutes=parts["minutes"],
        seconds=parts["seconds"],
    )


def format_duration(delta: timedelta) -> str:
    """Inverse of :func:`parse_duration`, for round-tripping into config/logs."""
    total = int(delta.total_seconds())
    if total == 0:
        return "PT0S"
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    out = "P" + (f"{days}D" if days else "")
    time_part = "".join(
        [f"{hours}H" if hours else "", f"{minutes}M" if minutes else "", f"{seconds}S" if seconds else ""]
    )
    return out + (f"T{time_part}" if time_part else "")
