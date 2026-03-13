from __future__ import annotations

from datetime import date, datetime

from zoneinfo import ZoneInfo

from app.logic import PersonDayAccumulator, compute_lunch_deduction, detect_shift, finalise_person_day, prepare_break_entries
from app.utils import minutes_between
from app.models import BreakEntry

TZ = ZoneInfo("Asia/Kolkata")


def make_dt(hour: int, minute: int) -> datetime:
    return datetime(2024, 8, 12, hour, minute, tzinfo=TZ)


def test_detect_shift_on_time():
    shift, is_late, category = detect_shift(make_dt(9, 35))
    assert shift == "A"
    assert not is_late
    assert category == "On time"

    shift, is_late, category = detect_shift(make_dt(9, 55))
    assert shift == "B"
    assert not is_late
    assert category == "On time"


def test_detect_shift_late_and_severe():
    shift, is_late, category = detect_shift(make_dt(10, 20))
    assert shift == "B"
    assert is_late
    assert category == "Late"

    shift, is_late, category = detect_shift(make_dt(10, 40))
    assert shift == "Unknown"
    assert is_late
    assert category == "Severely Late"


def test_compute_lunch_deduction_auto_when_no_logged_lunch():
    first = make_dt(9, 30)
    last = make_dt(18, 30)
    deduction = compute_lunch_deduction(first, last, [])
    assert deduction == 45


def test_compute_lunch_deduction_clamped_to_overlap():
    first = make_dt(12, 45)
    last = make_dt(13, 15)
    deduction = compute_lunch_deduction(first, last, [])
    assert deduction == 15


def test_compute_lunch_deduction_skips_when_logged_lunch():
    first = make_dt(9, 30)
    last = make_dt(18, 30)
    lunch_break = BreakEntry(
        start_ts=make_dt(13, 0),
        end_ts=make_dt(13, 30),
        minutes=30,
        break_type="Lunch",
        comments=None,
    )
    deduction = compute_lunch_deduction(first, last, [lunch_break])
    assert deduction == 0


def test_finalise_person_day_break_clipping_and_net_minutes():
    acc = PersonDayAccumulator(person_id="test@example.com", date=date(2024, 8, 12))
    acc.add_checkin(make_dt(9, 45), "Tester", "test@example.com")
    acc.add_checkout(make_dt(18, 0), "Tester", "test@example.com")
    # Break mostly inside span, partially outside
    # Break spans outside gross span and should be clipped to overlap
    acc.add_break(BreakEntry(start_ts=make_dt(9, 0), end_ts=make_dt(10, 30), minutes=90, break_type="Personal", comments=None))
    person_day = finalise_person_day(acc)
    assert person_day.break_minutes == 45  # clipped overlap 09:45-10:30
    assert person_day.lunch_auto_deduct_minutes == 45  # auto lunch deducted
    expected_net = minutes_between(make_dt(9, 45), make_dt(18, 0)) - 45 - 45
    assert person_day.net_minutes == expected_net


def test_prepare_break_entries_pairs_start_end():
    start_dt = datetime(2025, 5, 8, 13, 0, tzinfo=TZ)
    end_dt = datetime(2025, 5, 8, 13, 45, tzinfo=TZ)
    records = [
        {
            "_normalized_email": "user@example.com",
            "_normalized_name": "User",
            "_start_ts": start_dt.isoformat(),
            "_end_ts": None,
            "_submission_ts": start_dt.isoformat(),
            "_local_date": "2025-05-08",
            "Break Type": "Lunch",
            "Comments / Reason": "Start lunch",
        },
        {
            "_normalized_email": "user@example.com",
            "_normalized_name": "User",
            "_start_ts": None,
            "_end_ts": end_dt.isoformat(),
            "_submission_ts": end_dt.isoformat(),
            "_local_date": "2025-05-08",
            "Break Type": "Lunch",
            "Comments / Reason": "End lunch",
        },
    ]
    entries, warnings = prepare_break_entries(records)
    entry_list = entries[("user@example.com", date(2025, 5, 8))]
    assert len(entry_list) == 1
    assert entry_list[0].minutes == 45
    assert entry_list[0].break_type == "Lunch"
    assert not warnings.get(("user@example.com", date(2025, 5, 8)))


def test_minutes_between_zero_when_end_before_start():
    start = make_dt(10, 0)
    end = make_dt(9, 0)
    assert minutes_between(start, end) == 0


def test_prepare_break_entries_reports_incomplete_entries():
    start_dt = datetime(2025, 5, 8, 13, 0, tzinfo=TZ)
    records = [
        {
            "_normalized_email": "user@example.com",
            "_normalized_name": "User",
            "_start_ts": start_dt.isoformat(),
            "_end_ts": None,
            "_submission_ts": start_dt.isoformat(),
            "_local_date": "2025-05-08",
            "Break Type": "Lunch",
            "Comments / Reason": "Start lunch",
        },
        {
            "_normalized_email": "user@example.com",
            "_normalized_name": "User",
            "_start_ts": None,
            "_end_ts": start_dt.isoformat(),
            "_submission_ts": start_dt.isoformat(),
            "_local_date": "2025-05-08",
            "Break Type": "Tea",
            "Comments / Reason": "End tea",
        },
    ]
    entries, warnings = prepare_break_entries(records)
    assert len(entries[("user@example.com", date(2025, 5, 8))]) == 0
    warning_list = warnings[("user@example.com", date(2025, 5, 8))]
    assert any("missing end time" in message for message in warning_list)
    assert any("missing start time" in message for message in warning_list)


