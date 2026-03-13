from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from .config import settings
from .utils import TZ, normalise_email, normalise_name, parse_datetime, read_csv_bytes

logger = logging.getLogger(__name__)


CHECKIN_SIGNATURE_FIELDS = {"today’s key tasks / focus", "your mood this morning (1–5)"}
CHECKOUT_SIGNATURE_FIELDS = {"end-of-day energy level", "today’s work summary"}
BREAKS_SIGNATURE_FIELDS = {"start time of break", "end time of break"}


def normalise_header(header: str) -> str:
    return " ".join(header.split()).strip().lower()


@dataclass
class ParsedUploads:
    checkins: List[dict]
    checkouts: List[dict]
    breaks: List[dict]


def detect_file_kind(headers: Iterable[str]) -> Optional[str]:
    normalised = {normalise_header(h) for h in headers}
    if BREAKS_SIGNATURE_FIELDS.issubset(normalised):
        return "breaks"
    if CHECKOUT_SIGNATURE_FIELDS.issubset(normalised) and "date & time" in normalised:
        return "checkout"
    if CHECKIN_SIGNATURE_FIELDS.intersection(normalised):
        return "checkin"
    return None


def _extract_name_from_record(record: dict) -> Optional[str]:
    """Extract name from CSV record, trying multiple header variations."""
    # Try exact matches first (with various spacing)
    name = (record.get("Full Name") or 
            record.get("Full Name ") or 
            record.get(" Full Name") or
            record.get(" Full Name ") or
            record.get("Name") or
            record.get("Name ") or
            record.get(" Name") or
            record.get(" Name "))
    
    # If not found, try case-insensitive matching across all keys
    if not name:
        for key, value in record.items():
            if value and isinstance(value, str) and value.strip():
                # Normalize key: strip whitespace, convert to lowercase
                key_normalized = " ".join(key.strip().split()).lower()
                # Match variations of "Full Name" or "Name" (but not "Email address" or "Timestamp")
                if key_normalized in ("full name", "fullname", "name") and key_normalized not in ("email address", "email", "timestamp", "date & time"):
                    name = value
                    break
    
    # If still not found, try to find any key that contains "name" (case-insensitive)
    if not name:
        for key, value in record.items():
            if value and isinstance(value, str) and value.strip():
                key_lower = key.strip().lower()
                # Look for keys containing "name" but not "email" or "timestamp"
                if "name" in key_lower and "email" not in key_lower and "timestamp" not in key_lower:
                    name = value
                    break
    
    return normalise_name(name)


def parse_checkin_csv(data: bytes) -> Tuple[List[dict], List[str]]:
    rows: List[dict] = []
    errors: List[str] = []
    
    # Log headers for debugging
    first_row = None
    for row, idx in read_csv_bytes(data):
        if first_row is None:
            first_row = row
            logger.info("Check-in CSV headers found: %s", list(row.keys())[:10])
        record = deepcopy(row)
        ts_raw = record.get("Timestamp") or record.get("timestamp")
        
        # IMPORTANT: Always use Timestamp for date and time - this is the standard source
        dt = parse_datetime(ts_raw)
        if not dt:
            errors.append(f"Row {idx}: invalid check-in timestamp {ts_raw!r}")
            continue
        
        # Log timestamp parsing for debugging
        if idx <= 3:
            logger.info("Row %d: Timestamp raw='%s', parsed=%s (date=%s, time=%s)", idx, ts_raw, dt, dt.date(), dt.time())

        email = normalise_email(record.get("Email address") or record.get("email address") or record.get("Email Address"))
        # Extract name using robust header matching
        name = _extract_name_from_record(record)
        
        if not name and idx <= 3:  # Log for first few rows only
            logger.warning("Row %d: No name found. Available keys: %s", idx, list(record.keys())[:10])
        elif name and idx <= 3:
            logger.info("Row %d: Extracted name '%s' for email '%s'", idx, name, email)
        
        record["_normalized_email"] = email
        record["_normalized_name"] = name
        
        # Always use timestamp date and time as the standard source
        record["_local_date"] = dt.date().isoformat()
        record["_timestamp"] = dt.isoformat()
        rows.append(record)
    return rows, errors


def parse_checkout_csv(data: bytes) -> Tuple[List[dict], List[str]]:
    rows: List[dict] = []
    errors: List[str] = []
    
    # Log headers for debugging
    first_row = None
    for row, idx in read_csv_bytes(data):
        if first_row is None:
            first_row = row
            logger.info("Check-out CSV headers found: %s", list(row.keys())[:10])
        record = deepcopy(row)
        ts_raw = record.get("Date & Time") or record.get("date & time") or record.get("Timestamp")
        
        # IMPORTANT: Always use Timestamp (Date & Time) for date and time - this is the standard source
        dt = parse_datetime(ts_raw)
        if not dt:
            errors.append(f"Row {idx}: invalid check-out timestamp {ts_raw!r}")
            continue
        
        # Log timestamp parsing for debugging
        if idx <= 3:
            logger.info("Row %d: Timestamp raw='%s', parsed=%s (date=%s, time=%s)", idx, ts_raw, dt, dt.date(), dt.time())

        email = normalise_email(record.get("Email address") or record.get("email address") or record.get("Email Address"))
        # Extract name using robust header matching
        name = _extract_name_from_record(record)
        
        if not name and idx <= 3:  # Log for first few rows only
            logger.warning("Row %d: No name found. Available keys: %s", idx, list(record.keys())[:10])
        elif name and idx <= 3:
            logger.info("Row %d: Extracted name '%s' for email '%s'", idx, name, email)
        
        record["_normalized_email"] = email
        record["_normalized_name"] = name
        
        # Always use timestamp date and time as the standard source
        record["_local_date"] = dt.date().isoformat()
        record["_timestamp"] = dt.isoformat()
        rows.append(record)
    return rows, errors


def _is_time_only(value: Optional[str]) -> bool:
    if not value:
        return False
    trimmed = value.strip()
    if not trimmed:
        return False
    # Time formats like HH:MM, HH:MM:SS, optionally with AM/PM
    if ":" in trimmed and not any(sep in trimmed for sep in ("/", "-", ".")):
        return True
    return False


def parse_breaks_csv(data: bytes) -> Tuple[List[dict], List[str]]:
    rows: List[dict] = []
    errors: List[str] = []
    for row, idx in read_csv_bytes(data):
        record = deepcopy(row)
        # Break submissions may be logged twice (start + end). Capture both and pair later.
        start_raw = record.get("Start Time of Break") or record.get("start time of break")
        end_raw = record.get("End Time of Break") or record.get("end time of break")
        timestamp_raw = record.get("Timestamp") or record.get("timestamp")
        date_raw = (
            record.get("  Date  ")
            or record.get("Date")
            or record.get("date")
            or record.get("  date  ")
        )

        submission_dt = parse_datetime(timestamp_raw)
        base_date = parse_datetime(date_raw) if date_raw else None

        start_dt = parse_datetime(start_raw, default_date=base_date) if start_raw else None
        end_dt = parse_datetime(end_raw, default_date=base_date) if end_raw else None

        if _is_time_only(start_raw) and base_date is None:
            errors.append(f"Row {idx}: break start provided without a date; skipping")
            start_dt = None
        if _is_time_only(end_raw) and base_date is None:
            errors.append(f"Row {idx}: break end provided without a date; skipping")
            end_dt = None

        # IMPORTANT: Allow records with only start OR only end time
        # The pairing logic in prepare_break_entries will match them up
        # This handles the case where start and end are in separate form submissions
        if not start_dt and not end_dt:
            errors.append(f"Row {idx}: missing break start/end times")
            continue

        # Only validate start < end if both are present in the same record
        # If they're in separate records, prepare_break_entries will handle the pairing
        if start_dt and end_dt and end_dt < start_dt:
            errors.append(f"Row {idx}: break end precedes start")
            continue

        email = normalise_email(record.get("Email address") or record.get("email address") or record.get("Email Address"))
        # Extract name using robust header matching
        name = _extract_name_from_record(record)
        
        if not name and idx == 2:  # Log only for first data row
            logger.warning("Row %d: No name found. Available keys: %s", idx, list(record.keys())[:10])

        record["_normalized_email"] = email
        record["_normalized_name"] = name
        record["_start_ts"] = start_dt.isoformat() if start_dt else None
        record["_end_ts"] = end_dt.isoformat() if end_dt else None
        record["_submission_ts"] = submission_dt.isoformat() if submission_dt else None

        local_date_source = start_dt or end_dt or base_date
        if local_date_source:
            record["_local_date"] = local_date_source.astimezone(TZ).date().isoformat()
        rows.append(record)
    return rows, errors


def parse_uploads(files: Dict[str, bytes]) -> ParsedUploads:
    """
    Parse uploaded files keyed by "checkin", "checkout", "breaks".
    """
    parsed = ParsedUploads(checkins=[], checkouts=[], breaks=[])
    for kind, content in files.items():
        if kind == "checkin":
            rows, errors = parse_checkin_csv(content)
            parsed.checkins = rows
        elif kind == "checkout":
            rows, errors = parse_checkout_csv(content)
            parsed.checkouts = rows
        elif kind == "breaks":
            rows, errors = parse_breaks_csv(content)
            parsed.breaks = rows
        else:
            logger.warning("Unknown file kind %s received; skipping", kind)
            continue

        for err in errors:
            logger.warning("Parsing %s CSV: %s", kind, err)

    return parsed


