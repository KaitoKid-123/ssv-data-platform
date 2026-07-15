"""Calendar anchors for a VN business day (replaces Util.util.generate_calendar_date)."""
from dataclasses import dataclass
from datetime import datetime, timedelta
 
from ssv_data.config import VN_TZ_OFFSET_HOURS
 
 
@dataclass(frozen=True)
class RunWindow:
    report_date: str        # 'YYYY-MM-DD', the VN day being (re)built
    utc_lo: datetime        # UTC lower bound covering that VN day
    utc_hi: datetime        # UTC upper bound (half-open)
    cover_lo: datetime      # wider lower bound for late-arriving lookbacks
    cover_hi: datetime      # wider upper bound (post-midnight enrichment on backfill)
 
 
def get_run_window(running_date: str,
                   tz_offset_hours: int = VN_TZ_OFFSET_HOURS,
                   cover_days: int = 1) -> RunWindow:
    """VN day [running_date 00:00, +1d) expressed as a UTC half-open window.
 
    NOTE: confirm this reproduces the original generate_calendar_date() exactly
    (esp. the _cover / _9hour variants) before go-live.
    """
    d = datetime.strptime(running_date, "%Y-%m-%d")
    off = timedelta(hours=tz_offset_hours)
    utc_lo = d - off
    utc_hi = d + timedelta(days=1) - off
    return RunWindow(
        report_date=running_date,
        utc_lo=utc_lo,
        utc_hi=utc_hi,
        cover_lo=utc_lo - timedelta(days=cover_days),
        cover_hi=utc_hi + timedelta(days=cover_days),
    )