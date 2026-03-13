from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, time
from typing import Iterable, Optional

from dateutil import parser
from zoneinfo import ZoneInfo

from .config import settings


TZ = ZoneInfo(settings.timezone)


def parse_datetime(value: str, default_date: Optional[datetime] = None) -> Optional[datetime]:
    """Parse a timestamp string into a timezone-aware datetime in IST."""
    if not value or not value.strip():
        return None

    value = value.strip()
    
    # Try explicit DD-MM-YYYY format first (Indian/European date format)
    # Pattern: DD-MM-YYYY or DD-MM-YYYY HH:MM:SS
    # Examples: "05-12-2025", "05-12-2025 09:34", "15-04-2025 9:21:59"
    dd_mm_yyyy_pattern = re.compile(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?')
    match = dd_mm_yyyy_pattern.match(value)
    if match:
        try:
            day_str, month_str, year_str = match.group(1), match.group(2), match.group(3)
            day, month, year = int(day_str), int(month_str), int(year_str)
            hour = int(match.group(4)) if match.group(4) else 0
            minute = int(match.group(5)) if match.group(5) else 0
            second = int(match.group(6)) if match.group(6) else 0
            
            # IMPORTANT: Always interpret as DD-MM-YYYY when pattern matches
            # This is the Indian/European date format convention
            if 1 <= day <= 31 and 1 <= month <= 12 and year >= 2000:
                # Create datetime with DD-MM-YYYY interpretation (day, month, year)
                # day is first group, month is second group
                dt = datetime(year, month, day, hour, minute, second)
                if dt.tzinfo is None:
                    if default_date:
                        dt = dt.replace(year=default_date.year, month=default_date.month, day=default_date.day)
                    dt = dt.replace(tzinfo=TZ)
                else:
                    dt = dt.astimezone(TZ)
                return dt
        except (ValueError, TypeError) as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to parse DD-MM-YYYY date '{value}': {e}")
            pass  # Fall through to dateutil parser
    
    # Fallback to dateutil parser with dayfirst=True (DD-MM-YYYY)
    # NOTE: dateutil with dayfirst=True interprets dates as DD-MM-YYYY
    try:
        dt = parser.parse(value, dayfirst=True)
    except (ValueError, TypeError):
        try:
            dt = parser.parse(value)
        except (ValueError, TypeError):
            return None

    if dt.tzinfo is None:
        if default_date:
            dt = dt.replace(year=default_date.year, month=default_date.month, day=default_date.day)
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt


def parse_date(value: str) -> Optional[datetime]:
    dt = parse_datetime(value)
    if dt:
        return dt.date()
    return None


def clamp_time_range(start: datetime, end: datetime, window_start: time, window_end: time) -> int:
    """
    Return overlap minutes between a datetime span and a daily time window.
    """
    if start >= end:
        return 0

    window_start_dt = start.replace(hour=window_start.hour, minute=window_start.minute, second=0, microsecond=0)
    window_end_dt = start.replace(hour=window_end.hour, minute=window_end.minute, second=0, microsecond=0)
    # adjust if window crosses midnight (not expected here but safe)
    if window_end_dt <= window_start_dt:
        window_end_dt = window_end_dt.replace(day=window_end_dt.day + 1)

    overlap_start = max(start, window_start_dt)
    overlap_end = min(end, window_end_dt)

    if overlap_end <= overlap_start:
        return 0
    return int((overlap_end - overlap_start).total_seconds() // 60)


def minutes_between(start: datetime, end: datetime) -> int:
    if end <= start:
        return 0
    return int((end - start).total_seconds() // 60)


def normalise_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip().lower()


def normalise_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return " ".join(value.split())


def read_csv_bytes(data: bytes) -> Iterable[dict]:
    """Read CSV bytes with multiple encoding and delimiter attempts."""
    # Try multiple encodings
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]
    text = None
    used_encoding = None
    
    for encoding in encodings:
        try:
            text = data.decode(encoding)
            used_encoding = encoding
            break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if text is None:
        raise ValueError("Could not decode CSV file with any supported encoding (utf-8, latin-1, cp1252, iso-8859-1)")
    
    # Try multiple delimiters (comma, semicolon, tab)
    delimiters = [",", ";", "\t"]
    reader = None
    used_delimiter = None
    
    for delimiter in delimiters:
        try:
            test_reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
            # Check if we got reasonable number of columns (at least 3)
            if test_reader.fieldnames and len(test_reader.fieldnames) >= 3:
                reader = test_reader
                used_delimiter = delimiter
                break
        except Exception:
            continue
    
    # Fallback to default comma delimiter
    if reader is None:
        reader = csv.DictReader(io.StringIO(text))
        used_delimiter = ","
    
    if reader.fieldnames:
        reader.fieldnames = [header.strip() if header else header for header in reader.fieldnames]
    
    for idx, row in enumerate(reader, start=2):
        yield row, idx  # include 1-based row index (including header) for logging


def dump_json(path, payload) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


