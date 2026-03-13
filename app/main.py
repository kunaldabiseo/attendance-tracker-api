from __future__ import annotations

import csv
import io
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import Body, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from .config import settings
from .logic import (
    aggregate_lateness,
    apply_calendar_filters,
    build_person_days,
    compute_kpis,
    compute_person_summaries,
    filter_person_days,
    finalise_person_day,
)
from .models import ComputeRequest, ComputeResponse, KPIResponse, PersonDay, UploadResponse
from .parsers import detect_file_kind, parse_uploads, parse_checkin_csv, parse_checkout_csv, parse_breaks_csv
from .storage import store

logger = logging.getLogger("attendance_tracker")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors with detailed messages."""
    logger.error("Request validation error: %s", exc.errors())
    errors = []
    for error in exc.errors():
        loc = " -> ".join(str(x) for x in error.get("loc", []))
        msg = error.get("msg", "Unknown error")
        errors.append(f"{loc}: {msg}")
    return JSONResponse(
        status_code=422,
        content={"detail": f"Validation error: {'; '.join(errors)}"}
    )




@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/debug/dates")
def debug_dates():
    """Return date distribution of stored check-ins to diagnose sync issues."""
    checkins = store.get("checkin")
    by_month: Dict[str, int] = {}
    for r in checkins:
        d = r.get("_local_date")
        if not d:
            continue
        month = d[:7] if len(d) >= 7 else d  # YYYY-MM
        by_month[month] = by_month.get(month, 0) + 1
    months = sorted(by_month.keys())
    return {
        "total_checkins": len(checkins),
        "checkins_with_date": sum(by_month.values()),
        "by_month": {m: by_month[m] for m in months},
        "date_range": {"min": months[0], "max": months[-1]} if months else None,
    }


@app.get("/clear-cache")
@app.post("/clear-cache")
def clear_cache():
    """Clear the cached upload data. Useful after fixing date parsing issues."""
    store.clear()
    logger.info("Cache cleared")
    return {"status": "ok", "message": "Cache cleared successfully"}


def _extract_csv_headers(content: bytes) -> List[str]:
    """Extract CSV headers with multiple encoding and delimiter attempts."""
    # Try multiple encodings
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]
    text = None
    
    for encoding in encodings:
        try:
            text = content.decode(encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if text is None:
        raise ValueError("Could not decode CSV file with any supported encoding")
    
    # Try multiple delimiters
    delimiters = [",", ";", "\t"]
    headers = []
    
    for delimiter in delimiters:
        try:
            reader = csv.reader(io.StringIO(text), delimiter=delimiter)
            headers = next(reader)
            if headers and len(headers) >= 3:  # Reasonable number of columns
                break
        except (StopIteration, Exception):
            continue
    
    # Fallback to default
    if not headers:
        reader = csv.reader(io.StringIO(text))
        try:
            headers = next(reader)
        except StopIteration:
            return []
    
    return headers


def _convert_excel_to_csv_bytes(content: bytes) -> Tuple[List[str], bytes]:
    df = pd.read_excel(io.BytesIO(content), dtype=str)
    df = df.fillna("")
    headers = [str(col) for col in df.columns]
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return headers, buffer.getvalue().encode("utf-8")


def _prepare_file_content(content: bytes, filename: str) -> Tuple[List[str], bytes]:
    lower_name = (filename or "").lower()
    if lower_name.endswith((".xlsx", ".xls")):
        try:
            return _convert_excel_to_csv_bytes(content)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to read Excel file %s: %s", filename, exc)
            raise HTTPException(status_code=400, detail=f"Could not read Excel file {filename}") from exc
    return _extract_csv_headers(content), content


def _filter_rows_by_date(rows: List[dict], filter_from: Optional[str], filter_to: Optional[str]) -> List[dict]:
    """Filter rows by _local_date within [filter_from, filter_to] inclusive."""
    if not filter_from and not filter_to:
        return rows
    filtered = []
    for r in rows:
        d = r.get("_local_date")
        if not d:
            continue
        if filter_from and d < filter_from:
            continue
        if filter_to and d > filter_to:
            continue
        filtered.append(r)
    return filtered


@app.post("/upload-paste", response_model=UploadResponse)
async def upload_pasted_data(payload: dict = Body(...)):
    """Handle pasted CSV data from frontend."""
    if not payload or "data" not in payload:
        raise HTTPException(status_code=400, detail="No data provided")
    
    data_list = payload.get("data", [])
    if not data_list:
        raise HTTPException(status_code=400, detail="No data provided")
    
    filter_from = payload.get("filter_from")  # YYYY-MM-DD
    filter_to = payload.get("filter_to")  # YYYY-MM-DD
    
    parsed_bytes: Dict[str, bytes] = {}
    found_counts = {"checkin": 0, "checkout": 0, "breaks": 0}
    
    for item in data_list:
        data_type = item.get("type")
        csv_text = item.get("csv", "")
        
        if not data_type or not csv_text:
            continue
        
        if data_type not in ("checkin", "checkout", "breaks"):
            logger.warning("Unknown data type: %s", data_type)
            continue
        
        # Convert CSV text to bytes
        csv_bytes = csv_text.encode("utf-8")
        
        # Extract headers to verify it's valid CSV
        try:
            headers = _extract_csv_headers(csv_bytes)
            if not headers:
                logger.warning("No headers found in pasted %s data", data_type)
                continue
        except Exception as e:
            logger.warning("Failed to parse pasted %s data: %s", data_type, e)
            continue
        
        parsed_bytes[data_type] = csv_bytes
        found_counts[data_type] += 1
    
    if not parsed_bytes:
        raise HTTPException(status_code=400, detail="Unable to parse pasted data")
    
    # Process the data (same logic as file upload)
    # IMPORTANT: Always clear ALL old data first to prevent mixing old and new data
    store.clear()
    
    row_counts = {"checkin": 0, "checkout": 0, "breaks": 0}
    
    # Then update with new data
    if "checkin" in parsed_bytes:
        parsed_checkins, errors = parse_checkin_csv(parsed_bytes["checkin"])
        if errors:
            logger.warning("Errors parsing check-in data: %s", errors[:5])
        parsed_checkins = _filter_rows_by_date(parsed_checkins, filter_from, filter_to)
        store.update("checkin", parsed_checkins)
        row_counts["checkin"] = len(parsed_checkins)
        logger.info("Uploaded %d check-in records", len(parsed_checkins))
    
    if "checkout" in parsed_bytes:
        parsed_checkouts, errors = parse_checkout_csv(parsed_bytes["checkout"])
        if errors:
            logger.warning("Errors parsing check-out data: %s", errors[:5])
        parsed_checkouts = _filter_rows_by_date(parsed_checkouts, filter_from, filter_to)
        store.update("checkout", parsed_checkouts)
        row_counts["checkout"] = len(parsed_checkouts)
        logger.info("Uploaded %d check-out records", len(parsed_checkouts))
    
    if "breaks" in parsed_bytes:
        parsed_breaks, errors = parse_breaks_csv(parsed_bytes["breaks"])
        if errors:
            logger.warning("Errors parsing breaks data: %s", errors[:5])
        parsed_breaks = _filter_rows_by_date(parsed_breaks, filter_from, filter_to)
        store.update("breaks", parsed_breaks)
        row_counts["breaks"] = len(parsed_breaks)
        logger.info("Uploaded %d break records", len(parsed_breaks))
    
    range_note = ""
    if filter_from or filter_to:
        range_note = f" (filtered: {filter_from or '...'} to {filter_to or '...'})"
    
    return UploadResponse(
        status="ok",
        found=row_counts,
        message=f"Successfully uploaded: {row_counts['checkin']} check-ins, {row_counts['checkout']} check-outs, {row_counts['breaks']} breaks{range_note}"
    )


@app.post("/upload", response_model=UploadResponse)
async def upload_csv(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    parsed_bytes: Dict[str, bytes] = {}
    found_counts = {"checkin": 0, "checkout": 0, "breaks": 0}

    for file in files:
        content = await file.read()
        headers, normalized = _prepare_file_content(content, file.filename or "")
        kind = detect_file_kind(headers)
        if not kind:
            logger.warning("Could not detect CSV type for %s", file.filename)
            continue
        parsed_bytes[kind] = normalized
        found_counts[kind] += 1

    if not parsed_bytes:
        raise HTTPException(status_code=400, detail="Unable to classify uploaded files")

    parsed = parse_uploads(parsed_bytes)
    
    # IMPORTANT: Always clear ALL old data first to prevent mixing old and new data
    # This ensures a clean state when uploading files
    store.clear()
    
    row_counts = {"checkin": 0, "checkout": 0, "breaks": 0}
    
    # Then update with new data
    if parsed.checkins:
        store.update("checkin", parsed.checkins)
        row_counts["checkin"] = len(parsed.checkins)
        logger.info("Uploaded %d check-in records", len(parsed.checkins))
    
    if parsed.checkouts:
        store.update("checkout", parsed.checkouts)
        row_counts["checkout"] = len(parsed.checkouts)
        logger.info("Uploaded %d check-out records", len(parsed.checkouts))
    
    if parsed.breaks:
        store.update("breaks", parsed.breaks)
        row_counts["breaks"] = len(parsed.breaks)
        logger.info("Uploaded %d break records", len(parsed.breaks))

    return UploadResponse(
        status="ok",
        found=row_counts,
        message=f"Successfully uploaded: {row_counts['checkin']} check-ins, {row_counts['checkout']} check-outs, {row_counts['breaks']} breaks",
    )


def _prepare_person_days() -> List[PersonDay]:
    try:
        checkins = store.get("checkin")
        checkouts = store.get("checkout")
        breaks = store.get("breaks")

        logger.info("Data counts: checkins=%d, checkouts=%d, breaks=%d", len(checkins), len(checkouts), len(breaks))

        if not any((checkins, checkouts, breaks)):
            logger.info("No data available, returning empty list")
            return []

        logger.info("Building person days...")
        accumulators = build_person_days(checkins, checkouts, breaks)
        logger.info("Built %d person day accumulators", len(accumulators))
        
        logger.info("Finalising person days...")
        days = []
        for acc in accumulators.values():
            try:
                day = finalise_person_day(acc)
                days.append(day)
            except Exception as e:
                logger.exception("Error finalising person day for %s on %s: %s", acc.person_id, acc.date, e)
                raise
        
        days.sort(key=lambda d: (d.date, d.person_id))
        logger.info("Prepared %d person days", len(days))
        return days
    except Exception as e:
        logger.exception("Error in _prepare_person_days: %s", e)
        import traceback
        logger.error("Full traceback: %s", traceback.format_exc())
        raise


def _filter_request_days(request: ComputeRequest, base_days: List[PersonDay]) -> List[PersonDay]:
    calendar_filtered = apply_calendar_filters(
        base_days,
        include_sundays=request.include_sundays,
        include_holidays=request.include_holidays,
        holidays=request.holidays,
    )
    filtered = filter_person_days(calendar_filtered, request.filters)
    filtered.sort(key=lambda d: (d.date, d.person_id))
    return filtered


@app.post("/compute", response_model=ComputeResponse)
def compute_attendance(payload: ComputeRequest = Body(...)):
    try:
        logger.info("Compute request received")
        logger.info("Request payload: include_sundays=%s, include_holidays=%s", payload.include_sundays, payload.include_holidays)
        logger.info("Holidays count: %d", len(payload.holidays))
        if payload.holidays:
            logger.info("Sample holiday: %s", payload.holidays[0] if payload.holidays else None)
        logger.info("Filters: %s", payload.filters)
        
        base_days = _prepare_person_days()
        logger.info("Prepared %d base days", len(base_days))
        all_people = compute_person_summaries(base_days)
        logger.info("Computed %d person summaries", len(all_people))

        # Log filter details for debugging
        if payload.filters:
            logger.info(
                "Filtering with start_date=%s, end_date=%s, candidates=%s",
                payload.filters.start_date,
                payload.filters.end_date,
                payload.filters.candidates,
            )
            logger.info("Total base days before filtering: %d", len(base_days))
            
            # Log date range of all base days
            if base_days:
                all_dates = sorted(set(d.date for d in base_days))
                logger.info("Date range in base data: %s to %s (%d unique dates)", 
                    all_dates[0] if all_dates else "N/A",
                    all_dates[-1] if all_dates else "N/A",
                    len(all_dates))
            
            if payload.filters.candidates:
                # Log sample person_ids and emails for debugging
                candidate_set = {c.lower() for c in payload.filters.candidates}
                sample_days = [d for d in base_days if (d.email and d.email.lower() in candidate_set) or d.person_id.lower() in candidate_set]
                if sample_days:
                    sample_dates = sorted(set(d.date.isoformat() for d in sample_days))
                    logger.info("Sample matching days (before date filter): person_id=%s, email=%s, date_count=%d, dates=%s", 
                        sample_days[0].person_id, sample_days[0].email, len(sample_dates),
                        sample_dates[:15])

        filtered_days = _filter_request_days(payload, base_days)
        
        if payload.filters:
            logger.info("Total days after filtering: %d", len(filtered_days))
            if filtered_days:
                date_range = (min(d.date for d in filtered_days), max(d.date for d in filtered_days))
                logger.info("Filtered date range: %s to %s", date_range[0], date_range[1])
                # Log actual dates returned
                actual_dates = sorted(set(d.date.isoformat() for d in filtered_days))
                logger.info("Actual dates in filtered results: %s", actual_dates[:20])
                
                # Log all unique person_ids in filtered results to detect false matches
                if payload.filters.candidates:
                    # Handle None email values for sorting
                    unique_people = sorted(set((d.person_id, d.email or "") for d in filtered_days))
                    logger.info("Unique people in filtered results: %d people", len(unique_people))
                    if len(unique_people) > 1:
                        logger.warning("Multiple people in filtered results (expected 1): %s", unique_people[:5])
            elif payload.filters.candidates:
                # If no results but candidates were specified, log why
                candidate_set = {c.lower() for c in payload.filters.candidates}
                matching_people = [d for d in base_days if (d.email and d.email.lower() in candidate_set) or d.person_id.lower() in candidate_set]
                logger.info("No filtered results. Found %d base days matching candidates (before date filter)", len(matching_people))
                if matching_people:
                    candidate_dates = sorted(set(d.date.isoformat() for d in matching_people))
                    logger.info("Candidate dates (before date filter): %s", candidate_dates[:20])
        
        try:
            kpis = compute_kpis(filtered_days)
        except Exception as e:
            logger.exception("Error computing KPIs: %s", e)
            # Return default KPIs if computation fails
            kpis = KPIResponse(
                total_candidates=0,
                total_days_counted=0,
                late_days=0,
                under8h_days=0,
                avg_net_minutes=None,
                break_minutes_total=0,
            )

        try:
            response = ComputeResponse(
                people=all_people,
                days=filtered_days,
                kpis=kpis,
            )
            logger.info("Successfully created compute response")
            return response
        except Exception as e:
            logger.exception("Error creating ComputeResponse: %s", e)
            import traceback
            logger.error("Full traceback: %s", traceback.format_exc())
            raise
    except ValidationError as e:
        logger.exception("Validation error in compute_attendance")
        error_details = []
        for err in e.errors():
            loc = " -> ".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "Unknown error")
            error_details.append(f"{loc}: {msg}")
        error_msg = f"Validation error: {'; '.join(error_details)}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        logger.exception("Error in compute_attendance: %s", e)
        import traceback
        tb_str = traceback.format_exc()
        logger.error("Full traceback:\n%s", tb_str)
        # Include more details in the error message for debugging
        error_detail = f"Internal server error: {type(e).__name__}: {str(e)}"
        raise HTTPException(status_code=500, detail=error_detail)


def _parse_holidays_from_query(holidays_str: Optional[str]) -> list:
    """Parse '2025-01-01|New Year,2025-01-26|Republic Day' into Holiday dicts."""
    if not holidays_str or not holidays_str.strip():
        return []
    result = []
    for part in holidays_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "|" in part:
            date_part, name_part = part.split("|", 1)
            result.append({"date": date_part.strip(), "name": name_part.strip() or ""})
        else:
            result.append({"date": part, "name": ""})
    return result


def _days_for_export(
    include_sundays: bool = False,
    include_holidays: bool = False,
    holidays: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    candidates: Optional[List[str]] = None,
) -> List[PersonDay]:
    filters_payload = None
    if any([start_date, end_date, candidates]):
        filters_payload = {
            "start_date": start_date,
            "end_date": end_date,
            "candidates": candidates or [],
        }
    request = ComputeRequest(
        include_sundays=include_sundays,
        include_holidays=include_holidays,
        holidays=_parse_holidays_from_query(holidays),
        filters=filters_payload,
    )
    base_days = _prepare_person_days()
    return _filter_request_days(request, base_days)


def _attendance_dataframe(days: List[PersonDay]) -> pd.DataFrame:
    records = []
    for day in days:
        records.append(
            {
                "Date": day.date.isoformat(),
                "Name": day.name,
                "Email": day.email,
                "Shift": day.shift,
                "First Check-in": day.first_check_in_ts.isoformat() if day.first_check_in_ts else "",
                "Last Check-out": day.last_check_out_ts.isoformat() if day.last_check_out_ts else "",
                "Gross Minutes": day.gross_minutes if day.gross_minutes is not None else "",
                "Break Minutes": day.break_minutes,
                "Lunch Deduct Minutes": day.lunch_auto_deduct_minutes,
                "Net Minutes": day.net_minutes if day.net_minutes is not None else "",
                "Late Category": day.late_category,
                "Under 8 Hours": "Yes" if day.is_under_8h else "No",
                "Incomplete": "Yes" if day.is_incomplete else "No",
                "Notes": day.notes or "",
            }
        )
    return pd.DataFrame(records)


def _late_summary_dataframe(days: List[PersonDay]) -> pd.DataFrame:
    df = aggregate_lateness(days)
    if df.empty:
        return df
    df["late_rate"] = (df["late_rate"] * 100).round(2)
    df["under8_rate"] = (df["under8_rate"] * 100).round(2)
    return df[
        [
            "person_id",
            "name",
            "email",
            "late_days",
            "total_days",
            "late_rate",
            "under8_count",
            "under8_rate",
        ]
    ]


def _net_hours_dataframe(days: List[PersonDay]) -> pd.DataFrame:
    df = aggregate_lateness(days)
    if df.empty:
        return df
    df["avg_net"] = df["avg_net"].round(2)
    df["under8_rate"] = (df["under8_rate"] * 100).round(2)
    return df[
        [
            "person_id",
            "name",
            "email",
            "avg_net",
            "under8_count",
            "total_days",
            "under8_rate",
        ]
    ]


def _stream_dataframe(df: pd.DataFrame, filename: str, format: str = "csv") -> StreamingResponse:
    if format == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return StreamingResponse(buffer, media_type=media_type, headers={"Content-Disposition": f"attachment; filename={filename}.xlsx"})

    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue().encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}.csv"},
    )


@app.get("/export/attendance_daily.csv")
def export_attendance_daily(
    include_sundays: bool = Query(False),
    include_holidays: bool = Query(False),
    holidays: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    candidates: Optional[List[str]] = Query(None),
    format: str = Query("csv"),
):
    days = _days_for_export(
        include_sundays=include_sundays,
        include_holidays=include_holidays,
        holidays=holidays,
        start_date=start_date,
        end_date=end_date,
        candidates=candidates,
    )
    df = _attendance_dataframe(days)
    return _stream_dataframe(df, "attendance_daily", format=format)


@app.get("/export/late_summary.csv")
def export_late_summary(
    include_sundays: bool = Query(False),
    include_holidays: bool = Query(False),
    holidays: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    candidates: Optional[List[str]] = Query(None),
    format: str = Query("csv"),
):
    days = _days_for_export(
        include_sundays=include_sundays,
        include_holidays=include_holidays,
        holidays=holidays,
        start_date=start_date,
        end_date=end_date,
        candidates=candidates,
    )
    df = _late_summary_dataframe(days)
    return _stream_dataframe(df, "late_summary", format=format)


@app.get("/export/net_hours_summary.csv")
def export_net_hours_summary(
    include_sundays: bool = Query(False),
    include_holidays: bool = Query(False),
    holidays: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    candidates: Optional[List[str]] = Query(None),
    format: str = Query("csv"),
):
    days = _days_for_export(
        include_sundays=include_sundays,
        include_holidays=include_holidays,
        holidays=holidays,
        start_date=start_date,
        end_date=end_date,
        candidates=candidates,
    )
    df = _net_hours_dataframe(days)
    return _stream_dataframe(df, "net_hours_summary", format=format)

