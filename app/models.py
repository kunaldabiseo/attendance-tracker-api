from __future__ import annotations

from datetime import date, datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


ShiftCode = Literal["A", "B", "Unknown"]
LateCategory = Literal["On time", "Late", "Severely Late"]


class BreakEntry(BaseModel):
    start_ts: datetime
    end_ts: datetime
    minutes: int
    break_type: Optional[str] = None
    comments: Optional[str] = None


class PersonDay(BaseModel):
    date: date
    person_id: str
    name: Optional[str]
    email: Optional[str]
    first_check_in_ts: Optional[datetime] = None
    last_check_out_ts: Optional[datetime] = None
    breaks: List[BreakEntry] = Field(default_factory=list)
    gross_minutes: Optional[int] = None
    break_minutes: int = 0
    lunch_auto_deduct_minutes: int = 0
    net_minutes: Optional[int] = None
    shift: ShiftCode = "Unknown"
    is_late: bool = False
    late_category: LateCategory = "On time"
    is_under_8h: bool = False
    is_incomplete: bool = False
    notes: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class PersonSummary(BaseModel):
    id: str
    name: Optional[str]
    email: Optional[str]


class KPIResponse(BaseModel):
    total_candidates: int
    total_days_counted: int
    late_days: int
    under8h_days: int
    avg_net_minutes: Optional[float]
    break_minutes_total: int


class Holiday(BaseModel):
    date: date
    name: Optional[str] = None


class Filters(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    candidates: List[str] = Field(default_factory=list)


class ComputeRequest(BaseModel):
    include_sundays: bool = False
    include_holidays: bool = False
    holidays: List[Holiday] = Field(default_factory=list)
    filters: Optional[Filters] = None


class ComputeResponse(BaseModel):
    people: List[PersonSummary]
    days: List[PersonDay]
    kpis: KPIResponse


class UploadResponse(BaseModel):
    status: Literal["ok", "error"]
    found: dict
    message: Optional[str] = None


