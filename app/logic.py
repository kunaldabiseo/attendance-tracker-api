from __future__ import annotations

import calendar
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .config import settings
from .models import BreakEntry, Filters, Holiday, KPIResponse, PersonDay, PersonSummary
from .utils import TZ, clamp_time_range, minutes_between, normalise_email, normalise_name, parse_datetime

logger = logging.getLogger(__name__)

SHIFT_A_START = time(hour=9, minute=30)
SHIFT_A_DEADLINE = time(hour=9, minute=40)
SHIFT_B_START = time(hour=10, minute=0)
SHIFT_B_DEADLINE = time(hour=10, minute=10)
SHIFT_GRACE = time(hour=10, minute=25)
LUNCH_START = time(hour=13, minute=0)
LUNCH_END = time(hour=13, minute=45)
TARGET_NET_MINUTES = 8 * 60


@dataclass
class PersonDayAccumulator:
    person_id: str
    date: date
    name: Optional[str] = None
    email: Optional[str] = None
    checkins: List[datetime] = field(default_factory=list)
    checkouts: List[datetime] = field(default_factory=list)
    breaks: List[BreakEntry] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_checkin(self, ts: datetime, name: Optional[str], email: Optional[str]) -> None:
        if ts:
            self.checkins.append(ts)
        # Always update name/email if provided, prioritizing non-empty values
        # This ensures check-in/check-out names take precedence over break-only names
        if name and name.strip():
            self.name = name
        if email and email.strip():
            self.email = email

    def add_checkout(self, ts: datetime, name: Optional[str], email: Optional[str]) -> None:
        if ts:
            self.checkouts.append(ts)
        # Always update name/email if provided, prioritizing non-empty values
        # This ensures check-in/check-out names take precedence over break-only names
        if name and name.strip():
            self.name = name
        if email and email.strip():
            self.email = email

    def add_break(self, entry: BreakEntry) -> None:
        self.breaks.append(entry)

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)


def first_and_last(checkins: Iterable[datetime], checkouts: Iterable[datetime]) -> Tuple[Optional[datetime], Optional[datetime]]:
    first = min(checkins) if checkins else None
    last = max(checkouts) if checkouts else None
    return first, last


def detect_shift(first_check_in: Optional[datetime]) -> Tuple[str, bool, str]:
    if not first_check_in:
        return "Unknown", False, "On time"

    time_only = first_check_in.timetz()
    if time_only <= SHIFT_A_DEADLINE:
        is_late = time_only > SHIFT_A_DEADLINE
        return "A", is_late, "On time" if not is_late else "Late"

    if time_only <= SHIFT_B_DEADLINE:
        is_late = time_only > SHIFT_B_DEADLINE
        return "B", is_late, "On time" if not is_late else "Late"

    if time_only <= SHIFT_GRACE:
        return "B", True, "Late"

    return "Unknown", True, "Severely Late"


def compute_lunch_deduction(first: datetime, last: datetime, breaks: List[BreakEntry]) -> int:
    if not first or not last or first >= last:
        return 0

    # If any break entry overlaps lunch window and type indicates lunch, skip auto deduction
    for br in breaks:
        if br.break_type and "lunch" in br.break_type.lower():
            return 0

    overlap_minutes = clamp_time_range(first, last, LUNCH_START, LUNCH_END)
    if overlap_minutes <= 0:
        return 0

    # Check if any logged break already covers the lunch overlap
    total_overlap = 0
    for br in breaks:
        overlap = clamp_time_range(br.start_ts, br.end_ts, LUNCH_START, LUNCH_END)
        total_overlap += overlap

    auto_deduct = max(overlap_minutes - total_overlap, 0)
    return min(auto_deduct, 45)


def prepare_break_entries(records: List[dict]) -> Tuple[Dict[Tuple[str, date], List[BreakEntry]], Dict[Tuple[str, date], List[str]]]:
    grouped_events: Dict[Tuple[str, date], List[dict]] = defaultdict(list)
    for record in records:
        email = normalise_email(record.get("_normalized_email") or record.get("Email address"))
        name = normalise_name(record.get("_normalized_name") or record.get("Full Name"))
        person_id = email or (name.lower() if name else None)
        if not person_id:
            continue

        start_ts = parse_datetime(record.get("_start_ts") or record.get("Start Time of Break"))
        end_ts = parse_datetime(record.get("_end_ts") or record.get("End Time of Break"))
        submission_ts = parse_datetime(record.get("_submission_ts") or record.get("Timestamp"))
        local_date = record.get("_local_date")
        day = None
        if start_ts:
            day = start_ts.astimezone(TZ).date()
        elif end_ts:
            day = end_ts.astimezone(TZ).date()
        elif local_date:
            try:
                day = date.fromisoformat(str(local_date))
            except ValueError:
                day = None
        if not day:
            continue

        grouped_events[(person_id, day)].append(
            {
                "start": start_ts,
                "end": end_ts,
                "submission": submission_ts or start_ts or end_ts,
                "break_type": record.get("Break Type") or "General",
                "comments": record.get("Comments / Reason"),
            }
        )

    by_key: Dict[Tuple[str, date], List[BreakEntry]] = defaultdict(list)
    warnings_by_key: Dict[Tuple[str, date], List[str]] = defaultdict(list)
    for key, events in grouped_events.items():
        # Use a single queue for all break types to allow flexible pairing
        # This handles cases where start and end entries have different break types
        # (e.g., "Lunch Start" and "Post-Lunch Returns")
        open_breaks: deque = deque()
        for event in sorted(events, key=lambda e: e["submission"] or e["start"] or e["end"]):
            start_ts = event["start"]
            end_ts = event["end"]
            break_type = event["break_type"]
            comments = event["comments"]

            if start_ts and end_ts:
                # Complete break entry with both start and end in same record
                if end_ts <= start_ts:
                    continue
                
                # Check if this complete entry matches a pending start-only entry
                # This handles cases like "Lunch Start" (start-only) followed by "Lunch Over" (complete)
                # We should use the start time from the start-only entry (more accurate) and clear it
                matched_start = None
                matched_start_idx = None
                matched_start_comments = None
                
                for i, (pending_start, pending_break_type, pending_comments) in enumerate(open_breaks):
                    # Match if start times are within 5 minutes of each other
                    # This handles slight time differences between form submissions
                    time_diff = abs((start_ts - pending_start).total_seconds())
                    if time_diff <= 300:  # 5 minutes = 300 seconds
                        # Prefer the earlier start time (from start-only entry) as it's more accurate
                        matched_start = min(start_ts, pending_start)
                        matched_start_idx = i
                        matched_start_comments = pending_comments
                        # Use break type from start-only entry if it's more specific
                        if pending_break_type != "General":
                            break_type = pending_break_type
                        break
                
                # Use the matched start time if found, otherwise use the complete entry's start time
                final_start_ts = matched_start if matched_start else start_ts
                
                # If we found a matching start-only entry, remove it from the queue
                if matched_start_idx is not None:
                    new_queue = deque()
                    for i, item in enumerate(open_breaks):
                        if i != matched_start_idx:
                            new_queue.append(item)
                    open_breaks = new_queue
                    # Combine comments from both entries
                    combined_comments = "; ".join(
                        [text for text in [matched_start_comments, comments] if text]
                    ) or None
                    comments = combined_comments
                
                if end_ts <= final_start_ts:
                    continue
                minutes = minutes_between(final_start_ts, end_ts)
                if minutes <= 0:
                    continue
                
                by_key[key].append(
                    BreakEntry(
                        start_ts=final_start_ts,
                        end_ts=end_ts,
                        minutes=minutes,
                        break_type=break_type,
                        comments=comments,
                    )
                )
                continue

            if start_ts and not end_ts:
                # Start time only - add to queue to be matched with end time
                open_breaks.append((start_ts, break_type, comments))
                continue

            if end_ts and not start_ts:
                # End time only - try to match with most recent start time
                # Match by time proximity (end should be after start) and reasonable duration (max 4 hours)
                matched = False
                # Find the best matching start time (closest before the end time)
                best_match = None
                best_match_idx = -1
                best_duration = None
                
                for i, (start_record, start_break_type, start_comments) in enumerate(open_breaks):
                    if end_ts > start_record:
                        # Check if duration is reasonable (max 4 hours = 240 minutes)
                        duration_minutes = minutes_between(start_record, end_ts)
                        if 0 < duration_minutes <= 240:
                            # Prefer the most recent start time (highest index) that's before end time
                            if best_match is None or start_record > best_match[0]:
                                best_match = (start_record, start_break_type, start_comments)
                                best_match_idx = i
                                best_duration = duration_minutes
                
                if best_match:
                    # Remove the matched start from the queue
                    # Rebuild the deque without the matched item
                    new_queue = deque()
                    for i, item in enumerate(open_breaks):
                        if i != best_match_idx:
                            new_queue.append(item)
                    open_breaks = new_queue
                    
                    start_record, start_break_type, start_comments = best_match
                    # Use the break type from the start entry, or end entry if start is "General"
                    final_break_type = start_break_type if start_break_type != "General" else break_type
                    combined_comments = "; ".join(
                        [text for text in [start_comments, comments] if text]
                    ) or None
                    by_key[key].append(
                        BreakEntry(
                            start_ts=start_record,
                            end_ts=end_ts,
                            minutes=best_duration,
                            break_type=final_break_type,
                            comments=combined_comments,
                        )
                    )
                    matched = True
                
                if not matched:
                    warnings_by_key[key].append(f"Break ending at {end_ts.strftime('%H:%M')} missing start time")
                continue

        # discard any unmatched starts; they represent incomplete break logs
        while open_breaks:
            start_ts, break_type, _ = open_breaks.popleft()
            warnings_by_key[key].append(f"Break starting at {start_ts.strftime('%H:%M')} missing end time")

    return by_key, warnings_by_key


def build_person_days(checkins: List[dict], checkouts: List[dict], breaks: List[dict]) -> Dict[Tuple[str, date], PersonDayAccumulator]:
    day_map: Dict[Tuple[str, date], PersonDayAccumulator] = {}
    
    # Create a map of person_id -> (name, email) from check-in/check-out records
    # This will be used to fill in missing names for break-only days
    person_id_to_name_email: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    
    logger.info("Building person days from %d check-ins, %d check-outs, %d breaks", len(checkins), len(checkouts), len(breaks))

    for row in checkins:
        email = normalise_email(row.get("_normalized_email") or row.get("Email address"))
        name = normalise_name(row.get("_normalized_name") or row.get("Full Name"))
        person_id = email or (name.lower() if name else None)
        if not person_id:
            logger.warning("Check-in row missing email and name, skipping")
            continue

        # IMPORTANT: Always use timestamp as the standard source for date and time
        ts = parse_datetime(row.get("_timestamp") or row.get("Timestamp"))
        if not ts:
            logger.warning("Check-in: Missing timestamp for %s (row keys: %s)", person_id, list(row.keys())[:5])
            continue
        # Ensure timestamp is timezone-aware, then use its date for the day
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=TZ)
        else:
            ts = ts.astimezone(TZ)
        # Use timestamp date for the day - this is the standard
        day = ts.date()
        key = (person_id, day)
        if key not in day_map:
            day_map[key] = PersonDayAccumulator(person_id=person_id, date=day)
        day_map[key].add_checkin(ts, name, email)
        # Store name/email mapping for this person_id
        if person_id not in person_id_to_name_email or not person_id_to_name_email[person_id][0]:
            person_id_to_name_email[person_id] = (name, email)
        if day.month == 12 and day.year == 2025 and "pankaj" in person_id.lower():
            logger.info("Check-in: %s (email=%s, name=%s) on %s at %s", person_id, email, name, day, ts)

    for row in checkouts:
        email = normalise_email(row.get("_normalized_email") or row.get("Email address"))
        name = normalise_name(row.get("_normalized_name") or row.get("Full Name"))
        person_id = email or (name.lower() if name else None)
        if not person_id:
            logger.warning("Check-out row missing email and name, skipping")
            continue
        # IMPORTANT: Always use timestamp as the standard source for date and time
        ts = parse_datetime(row.get("_timestamp") or row.get("Date & Time") or row.get("Timestamp"))
        if not ts:
            logger.warning("Check-out: Missing timestamp for %s (row keys: %s)", person_id, list(row.keys())[:5])
            continue
        # Ensure timestamp is timezone-aware, then use its date for the day
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=TZ)
        else:
            ts = ts.astimezone(TZ)
        # Use timestamp date for the day - this is the standard
        day = ts.date()
        key = (person_id, day)
        if key not in day_map:
            day_map[key] = PersonDayAccumulator(person_id=person_id, date=day)
        day_map[key].add_checkout(ts, name, email)
        # Store name/email mapping for this person_id
        if person_id not in person_id_to_name_email or not person_id_to_name_email[person_id][0]:
            person_id_to_name_email[person_id] = (name, email)
        if day.month == 12 and day.year == 2025 and "pankaj" in person_id.lower():
            logger.info("Check-out: %s (email=%s, name=%s) on %s at %s", person_id, email, name, day, ts)

    break_entries, break_warnings = prepare_break_entries(breaks)
    # Create a map of (person_id, day) -> (name, email) from break records for days without checkin/out
    # This map will be used to populate names/emails for break-only days
    # Also create a reverse map: (day, name_pattern) -> person_id to handle email variations
    break_person_info: Dict[Tuple[str, date], Tuple[Optional[str], Optional[str]]] = {}
    break_person_by_day_name: Dict[Tuple[date, str], Tuple[str, Optional[str], Optional[str]]] = {}  # (day, name_lower) -> (person_id, name, email)
    
    for record in breaks:
        email = normalise_email(record.get("_normalized_email") or record.get("Email address"))
        name = normalise_name(record.get("_normalized_name") or record.get("Full Name"))
        person_id = email or (name.lower() if name else None)
        if not person_id:
            continue
        
        # Get date from various sources (same logic as prepare_break_entries)
        local_date = record.get("_local_date")
        day = None
        if local_date:
            try:
                day = date.fromisoformat(str(local_date))
            except ValueError:
                pass
        
        # If no local_date, try to get from start/end timestamps
        if not day:
            start_ts = parse_datetime(record.get("_start_ts") or record.get("Start Time of Break"))
            end_ts = parse_datetime(record.get("_end_ts") or record.get("End Time of Break"))
            if start_ts:
                day = start_ts.astimezone(TZ).date()
            elif end_ts:
                day = end_ts.astimezone(TZ).date()
        
        if not day:
            continue
            
        key = (person_id, day)
        # Store the best available name/email (prefer non-empty values)
        if key not in break_person_info or not break_person_info[key][0]:
            break_person_info[key] = (name, email)
            if name and "pankaj" in person_id.lower() and day.month == 12 and day.year == 2025:
                logger.info("Break record: Extracted name '%s' from break for %s on %s (email=%s)", name, person_id, day, email)
        
        # Also store by day + name pattern for email variation matching
        if name:
            name_lower = name.lower().strip()
            name_key = (day, name_lower)
            if name_key not in break_person_by_day_name:
                break_person_by_day_name[name_key] = (person_id, name, email)
    
    # Process break entries - merge with existing days or create new ones
    for key, entries in break_entries.items():
        if key not in day_map:
            # Create placeholder to allow break info even if no checkin/out yet
            person_id, day = key
            name, email = break_person_info.get(key, (None, None))
            
            # If name not found by exact key match, try to find by day + name pattern
            # This handles cases where break records use different email than check-in/check-out
            if not name:
                # Try to find by matching name pattern from other break records on same day
                for (d, name_pattern), (pid, n, e) in break_person_by_day_name.items():
                    if d == day and "pankaj" in name_pattern:
                        name = n
                        email = e
                        logger.info("Found name '%s' for %s on %s by name pattern matching", name, person_id, day)
                        break
            
            # If still no name, try to get it from person_id_to_name_email (from check-in/check-out records)
            if not name and person_id in person_id_to_name_email:
                name, email = person_id_to_name_email[person_id]
                if name and "pankaj" in person_id.lower() and day.month == 12 and day.year == 2025:
                    logger.info("Found name '%s' for %s on %s from check-in/check-out records", name, person_id, day)
            
            day_map[key] = PersonDayAccumulator(person_id=person_id, date=day, name=name, email=email)
            if name and "pankaj" in person_id.lower() and day.month == 12 and day.year == 2025:
                logger.info("Created break-only day for %s on %s with name=%s, email=%s", person_id, day, name, email)
            elif not name and "pankaj" in person_id.lower() and day.month == 12 and day.year == 2025:
                logger.warning("Created break-only day for %s on %s WITHOUT name (break_person_info keys: %s)", 
                             person_id, day, list(break_person_info.keys())[:5])
        else:
            # If day already exists from check-in/check-out, update name/email from breaks if missing
            acc = day_map[key]
            name, email = break_person_info.get(key, (None, None))
            if name and name.strip() and not acc.name:
                acc.name = name
                if "pankaj" in acc.person_id.lower() and acc.date.month == 12 and acc.date.year == 2025:
                    logger.info("Updated name from break for %s on %s: %s", acc.person_id, acc.date, name)
            if email and email.strip() and not acc.email:
                acc.email = email
        for entry in entries:
            day_map[key].add_break(entry)
    
    # Process break warnings - merge with existing days or create new ones
    for key, warnings in break_warnings.items():
        if key not in day_map:
            person_id, day = key
            name, email = break_person_info.get(key, (None, None))
            day_map[key] = PersonDayAccumulator(person_id=person_id, date=day, name=name, email=email)
            if name and "pankaj" in person_id.lower() and day.month == 12 and day.year == 2025:
                logger.info("Created break-only day (warnings) for %s on %s with name=%s, email=%s", person_id, day, name, email)
        else:
            # If day already exists from check-in/check-out, update name/email from breaks if missing
            acc = day_map[key]
            name, email = break_person_info.get(key, (None, None))
            if name and name.strip() and not acc.name:
                acc.name = name
                if "pankaj" in acc.person_id.lower() and acc.date.month == 12 and acc.date.year == 2025:
                    logger.info("Updated name from break (warnings) for %s on %s: %s", acc.person_id, acc.date, name)
            if email and email.strip() and not acc.email:
                acc.email = email
        for warning in warnings:
            day_map[key].add_warning(warning)
    
    # Log summary for debugging
    days_with_checkin = sum(1 for acc in day_map.values() if acc.checkins)
    days_with_checkout = sum(1 for acc in day_map.values() if acc.checkouts)
    days_with_both = sum(1 for acc in day_map.values() if acc.checkins and acc.checkouts)
    days_with_neither = sum(1 for acc in day_map.values() if not acc.checkins and not acc.checkouts)
    logger.info("Person days summary: %d total days, %d with check-in, %d with check-out, %d with both, %d with neither", 
                len(day_map), days_with_checkin, days_with_checkout, days_with_both, days_with_neither)
    
    # Check for duplicate dates for the same person
    person_date_counts: Dict[Tuple[str, date], int] = {}
    for (person_id, day), acc in day_map.items():
        key = (person_id, day)
        person_date_counts[key] = person_date_counts.get(key, 0) + 1
    
    duplicates = [(pid, d) for (pid, d), count in person_date_counts.items() if count > 1]
    if duplicates:
        logger.warning("Found duplicate person_id+date combinations: %s", duplicates[:10])
    
    # Log specific days for Pankaj in December 2025
    pankaj_days = [(k, acc) for k, acc in day_map.items() if "pankaj" in k[0].lower() and k[1].month == 12 and k[1].year == 2025]
    if pankaj_days:
        logger.info("Pankaj December 2025 days: %d days", len(pankaj_days))
        for (person_id, day), acc in sorted(pankaj_days, key=lambda x: x[0][1]):
            logger.info("  %s (person_id=%s): check-ins=%d, check-outs=%d, name=%s, email=%s", 
                       day, person_id, len(acc.checkins), len(acc.checkouts), acc.name, acc.email)

    return day_map


def finalise_person_day(acc: PersonDayAccumulator) -> PersonDay:
    first, last = first_and_last(acc.checkins, acc.checkouts)
    gross_minutes = minutes_between(first, last) if first and last else None
    break_minutes = 0

    clipped_breaks: List[BreakEntry] = []
    if first and last:
        for br in acc.breaks:
            if br.end_ts <= first or br.start_ts >= last:
                continue
            start = max(first, br.start_ts)
            end = min(last, br.end_ts)
            minutes = minutes_between(start, end)
            if minutes <= 0:
                continue
            clipped_breaks.append(
                BreakEntry(
                    start_ts=start,
                    end_ts=end,
                    minutes=minutes,
                    break_type=br.break_type,
                    comments=br.comments,
                )
            )
            break_minutes += minutes
    else:
        # If gross span missing, still retain raw break info
        clipped_breaks = acc.breaks
        break_minutes = sum(br.minutes for br in acc.breaks)

    shift, is_late, late_category = detect_shift(first)
    lunch_auto = 0
    if first and last:
        lunch_auto = compute_lunch_deduction(first, last, clipped_breaks)

    net_minutes = None
    is_under_8h = False
    if gross_minutes is not None:
        net_minutes = max(gross_minutes - break_minutes - lunch_auto, 0)
        is_under_8h = net_minutes < TARGET_NET_MINUTES and last is not None
    notes: List[str] = []
    if not first:
        notes.append("No check-in recorded")
    if not last:
        notes.append("No check-out recorded")
    if lunch_auto and lunch_auto > 0:
        notes.append(f"Auto lunch deduct {lunch_auto}m")
    is_incomplete = last is None
    return PersonDay(
        date=acc.date,
        person_id=acc.person_id,
        name=acc.name,
        email=acc.email,
        first_check_in_ts=first,
        last_check_out_ts=last,
        gross_minutes=gross_minutes,
        breaks=clipped_breaks,
        break_minutes=break_minutes,
        lunch_auto_deduct_minutes=lunch_auto,
        net_minutes=net_minutes,
        shift=shift,
        is_late=is_late,
        late_category=late_category,
        is_under_8h=is_under_8h if not is_incomplete else False,
        is_incomplete=is_incomplete,
        notes="; ".join(notes) if notes else None,
        warnings=acc.warnings,
    )


def filter_person_days(days: Iterable[PersonDay], filters: Optional[Filters]) -> List[PersonDay]:
    if not filters:
        return list(days)

    month_filter = getattr(filters, "month", None)  # backward compatibility for cached payloads
    start = filters.start_date
    end = filters.end_date
    candidates = {cand.lower() for cand in filters.candidates}

    filtered = []
    for day in days:
        if month_filter:
            year, month = month_filter.split("-")
            if day.date.year != int(year) or day.date.month != int(month):
                continue

        # Date range filtering: include dates where start <= day.date <= end
        if start and day.date < start:
            continue
        if end and day.date > end:
            continue
        if candidates:
            # Match by email (normalized) or person_id (normalized) - EXACT MATCH ONLY
            # This ensures accurate filtering without false positives
            email_match = day.email and day.email.lower() in candidates
            person_id_match = day.person_id.lower() in candidates
            
            if not email_match and not person_id_match:
                continue
        filtered.append(day)
    return filtered


def apply_calendar_filters(days: Iterable[PersonDay], include_sundays: bool, include_holidays: bool, holidays: List[Holiday]) -> List[PersonDay]:
    holiday_dates = {h.date for h in holidays} if holidays else set()
    filtered = []
    for day in days:
        weekday = day.date.weekday()
        if weekday == calendar.SUNDAY and not include_sundays:
            continue
        if not include_holidays and day.date in holiday_dates:
            continue
        filtered.append(day)
    return filtered


def compute_kpis(days: List[PersonDay]) -> KPIResponse:
    if not days:
        return KPIResponse(
            total_candidates=0,
            total_days_counted=0,
            late_days=0,
            under8h_days=0,
            avg_net_minutes=None,
            break_minutes_total=0,
        )

    people = {day.person_id for day in days}
    late_days = sum(1 for day in days if day.is_late)
    under8h_days = sum(1 for day in days if day.is_under_8h)
    net_values = [day.net_minutes for day in days if day.net_minutes is not None]
    avg_net = sum(net_values) / len(net_values) if net_values else None
    break_total = sum(day.break_minutes + day.lunch_auto_deduct_minutes for day in days)

    return KPIResponse(
        total_candidates=len(people),
        total_days_counted=len(days),
        late_days=late_days,
        under8h_days=under8h_days,
        avg_net_minutes=avg_net,
        break_minutes_total=break_total,
    )


def compute_person_summaries(days: List[PersonDay]) -> List[PersonSummary]:
    seen: Dict[str, PersonSummary] = {}
    for day in days:
        if day.person_id not in seen:
            seen[day.person_id] = PersonSummary(id=day.person_id, name=day.name, email=day.email)
        else:
            # If we already have this person but name/email is missing, try to fill it from this record
            existing = seen[day.person_id]
            # Pydantic models are immutable, so we need to create a new one
            if (not existing.name and day.name) or (not existing.email and day.email):
                seen[day.person_id] = PersonSummary(
                    id=day.person_id,
                    name=day.name if day.name else existing.name,
                    email=day.email if day.email else existing.email
                )
    return list(seen.values())


def aggregate_lateness(days: List[PersonDay]) -> pd.DataFrame:
    records = []
    for day in days:
        records.append(
            {
                "person_id": day.person_id,
                "name": day.name,
                "email": day.email,
                "is_late": day.is_late,
                "is_under_8h": day.is_under_8h,
                "net_minutes": day.net_minutes if day.net_minutes is not None else None,
            }
        )
    if not records:
        return pd.DataFrame(columns=["person_id", "late_days", "late_rate", "under8_count", "under8_rate", "avg_net"])

    df = pd.DataFrame(records)
    grouped = df.groupby("person_id", dropna=False).agg(
        late_days=("is_late", "sum"),
        total_days=("is_late", "count"),
        under8_count=("is_under_8h", "sum"),
        net_minutes=("net_minutes", "mean"),
        name=("name", "first"),
        email=("email", "first"),
    )
    grouped["late_rate"] = grouped["late_days"] / grouped["total_days"]
    grouped["under8_rate"] = grouped["under8_count"] / grouped["total_days"]
    grouped["avg_net"] = grouped["net_minutes"]
    return grouped.reset_index()


def weekly_nap_summary(days: List[PersonDay]) -> Dict[Tuple[str, str], Dict[str, int]]:
    """Return {(person_id, week_label): {"nap_minutes": int, "nap_count": int}}"""
    summary: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"nap_minutes": 0, "nap_count": 0})
    for day in days:
        week_start = day.date - timedelta(days=day.date.weekday())
        week_label = week_start.isoformat()
        for br in day.breaks:
            if br.break_type and "nap" in br.break_type.lower():
                summary[(day.person_id, week_label)]["nap_minutes"] += br.minutes
                summary[(day.person_id, week_label)]["nap_count"] += 1
    return summary


