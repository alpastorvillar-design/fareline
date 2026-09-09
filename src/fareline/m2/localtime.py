"""Civil-time intervals that a naive TLC timestamp cannot resolve on its own.

TLC publishes wall-clock times with no offset. Fareline keeps them naive and
never manufactures a UTC instant, so the only honest thing it can do for the
two hours a year that civil time is not a bijection is to name them: the
spring-forward hour that does not exist, and the fall-back hour that happens
twice. Both intervals are derived from the system time-zone database for the
file's own month, so no transition dates are hard-coded here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

try:  # The Spark image is Linux and ships a tz database; a host may not.
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover - Python builds without zoneinfo
    ZoneInfo = None  # type: ignore[assignment]
    ZoneInfoNotFoundError = Exception  # type: ignore[misc, assignment]

NONEXISTENT = "nonexistent"
AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class LocalInterval:
    """A half-open local wall-clock interval and why it is not resolvable."""

    kind: str
    start: datetime
    end: datetime

    def as_document(self) -> dict[str, str]:
        return {"kind": self.kind, "start": self.start.isoformat(), "end": self.end.isoformat()}


class TimeZoneDatabaseUnavailable(RuntimeError):
    """The runtime has no time-zone database, so transitions cannot be derived."""


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + (month == 12), (month % 12) + 1, 1, tzinfo=timezone.utc)
    return start, end


def _transition_instant(before: datetime, after: datetime, zone: Any) -> datetime:
    """Narrow an hour that contains an offset change down to the second.

    Hourly sampling alone would misplace any transition whose UTC instant is not
    on the hour, and half-hour offsets make that a real case rather than a
    theoretical one.
    """
    before_offset = before.astimezone(zone).utcoffset()
    while after - before > timedelta(seconds=1):
        middle = before + (after - before) / 2
        if middle.astimezone(zone).utcoffset() == before_offset:
            before = middle
        else:
            after = middle
    return after


def _interval(
    instant: datetime, before_offset: timedelta, after_offset: timedelta
) -> LocalInterval:
    """The local interval an offset change skipped or replayed."""
    before_local = (instant + before_offset).replace(tzinfo=None)
    after_local = (instant + after_offset).replace(tzinfo=None)
    if after_offset > before_offset:
        # Clocks jumped forward: the skipped local interval never occurred.
        return LocalInterval(NONEXISTENT, before_local, after_local)
    # Clocks jumped back: the local interval is replayed a second time.
    return LocalInterval(AMBIGUOUS, after_local, before_local)


def transitions(year: int, month: int, zone_name: str) -> tuple[LocalInterval, ...]:
    """Find the unresolvable local intervals inside one calendar month.

    The month is walked in UTC because UTC has no transitions, and the offset is
    sampled on each side of every hour. An hour whose offset changed is then
    bisected to the exact instant, and the local interval it skipped or repeated
    is reported.
    """
    if ZoneInfo is None:
        raise TimeZoneDatabaseUnavailable("this Python build has no zoneinfo module")
    try:
        zone = ZoneInfo(zone_name)
    except ZoneInfoNotFoundError as error:
        raise TimeZoneDatabaseUnavailable(f"no time-zone database entry for {zone_name}") from error

    start, end = _month_bounds(year, month)
    # One hour of slack on each side so a transition at a month boundary is
    # still observed from both directions.
    cursor = start - timedelta(hours=1)
    found: list[LocalInterval] = []
    previous_offset = cursor.astimezone(zone).utcoffset()
    while cursor < end:
        following = cursor + timedelta(hours=1)
        offset = following.astimezone(zone).utcoffset()
        if offset != previous_offset:
            instant = _transition_instant(cursor, following, zone)
            found.append(_interval(instant, previous_offset, offset))
            previous_offset = offset
        cursor = following

    return tuple(item for item in found if item.start.year == year and item.start.month == month)


def describe(year: int, month: int, zone_name: str) -> dict[str, Any]:
    """Report the month's transitions, or why they could not be derived."""
    try:
        found = transitions(year, month, zone_name)
    except TimeZoneDatabaseUnavailable as error:
        return {"zone": zone_name, "available": False, "reason": str(error), "intervals": []}
    return {
        "zone": zone_name,
        "available": True,
        "intervals": [item.as_document() for item in found],
    }
