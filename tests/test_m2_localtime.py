from __future__ import annotations

from datetime import datetime

import pytest

from fareline.m2 import localtime

ZONE = "America/New_York"


def kinds(year: int, month: int) -> list[tuple[str, str, str]]:
    return [
        (item.kind, item.start.isoformat(), item.end.isoformat())
        for item in localtime.transitions(year, month, ZONE)
    ]


def test_the_spring_forward_hour_is_reported_as_nonexistent() -> None:
    assert kinds(2024, 3) == [(localtime.NONEXISTENT, "2024-03-10T02:00:00", "2024-03-10T03:00:00")]
    assert kinds(2025, 3) == [(localtime.NONEXISTENT, "2025-03-09T02:00:00", "2025-03-09T03:00:00")]


def test_the_fall_back_hour_is_reported_as_ambiguous() -> None:
    assert kinds(2024, 11) == [(localtime.AMBIGUOUS, "2024-11-03T01:00:00", "2024-11-03T02:00:00")]


def test_a_month_without_a_transition_reports_nothing() -> None:
    # Both periods the measured slice uses are January, so the rule is real but
    # is expected to fire on no rows there.
    assert kinds(2024, 1) == []
    assert kinds(2025, 1) == []
    assert kinds(2023, 7) == []


def test_transitions_are_reported_only_for_the_month_asked_for() -> None:
    # The walk looks an hour past each boundary; a neighbouring month's
    # transition must not leak into this month's answer.
    assert kinds(2024, 2) == []
    assert kinds(2024, 4) == []
    assert kinds(2024, 10) == []
    assert kinds(2024, 12) == []


def test_an_interval_is_half_open_around_the_transition() -> None:
    interval = localtime.transitions(2024, 11, ZONE)[0]

    assert interval.start == datetime(2024, 11, 3, 1, 0)
    assert interval.end == datetime(2024, 11, 3, 2, 0)


def test_a_missing_time_zone_database_is_reported_rather_than_ignored() -> None:
    with pytest.raises(localtime.TimeZoneDatabaseUnavailable):
        localtime.transitions(2024, 3, "Mars/Olympus_Mons")

    described = localtime.describe(2024, 3, "Mars/Olympus_Mons")
    assert described["available"] is False
    assert described["intervals"] == []


def test_describe_renders_intervals_for_evidence() -> None:
    described = localtime.describe(2024, 3, ZONE)

    assert described["available"] is True
    assert described["intervals"] == [
        {
            "kind": localtime.NONEXISTENT,
            "start": "2024-03-10T02:00:00",
            "end": "2024-03-10T03:00:00",
        }
    ]
