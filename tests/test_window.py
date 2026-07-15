"""RunWindow — VN business day -> UTC bounds, incl. the cover window used by PG extraction."""
from datetime import datetime

from ssv_data.runtime.window import get_run_window


def test_run_window_bounds_for_a_vn_day():
    w = get_run_window("2025-11-17")
    assert w.report_date == "2025-11-17"
    assert w.utc_lo == datetime(2025, 11, 16, 17, 0, 0)   # 00:00 VN 17th
    assert w.utc_hi == datetime(2025, 11, 17, 17, 0, 0)   # 00:00 VN 18th (half-open)


def test_cover_window_is_symmetric_one_day_by_default():
    w = get_run_window("2025-11-17")
    assert w.cover_lo == datetime(2025, 11, 15, 17, 0, 0)  # utc_lo - 1d
    assert w.cover_hi == datetime(2025, 11, 18, 17, 0, 0)  # utc_hi + 1d


def test_cover_days_is_configurable():
    w = get_run_window("2025-11-17", cover_days=3)
    assert w.cover_lo == datetime(2025, 11, 13, 17, 0, 0)
    assert w.cover_hi == datetime(2025, 11, 20, 17, 0, 0)


def test_cover_window_contains_the_utc_dateparts_needed_by_the_canceled_path():
    # gold's canceled branch keeps rows with to_date(created_at) == run day (UTC date-part),
    # i.e. created_at in [D 00:00, D+1 00:00 UTC). The cover window must contain that range.
    w = get_run_window("2025-11-17")
    assert w.cover_lo <= datetime(2025, 11, 17, 0, 0, 0)
    assert w.cover_hi >= datetime(2025, 11, 18, 0, 0, 0)
