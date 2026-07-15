"""Windowed JDBC SQL builders + Readers wiring — no Spark needed.

The builders are pure (window -> SQL string); Readers is exercised with a fake
SparkSession that records the JDBC options it would submit.
"""
from ssv_data.io.readers import Readers, parent_windowed_select, windowed_select
from ssv_data.runtime.window import get_run_window

W = get_run_window("2025-11-17")  # cover: [2025-11-15 17:00, 2025-11-18 17:00) UTC


# ---- pure builders ----
def test_windowed_select_renders_cover_bounds():
    sql = windowed_select("public.delivery_orders", "created_at", W)
    assert sql == (
        "SELECT * FROM public.delivery_orders "
        "WHERE created_at >= TIMESTAMP '2025-11-15 17:00:00' "
        "AND created_at < TIMESTAMP '2025-11-18 17:00:00'"
    )


def test_parent_windowed_select_semijoins_on_the_windowed_parent():
    sql = parent_windowed_select("public.delivery_order_audits", "public.delivery_orders",
                                 "order_id", "created_at", W)
    assert sql == (
        "SELECT c.* FROM public.delivery_order_audits c WHERE EXISTS ("
        "SELECT 1 FROM public.delivery_orders p "
        "WHERE p.order_id = c.order_id "
        "AND p.created_at >= TIMESTAMP '2025-11-15 17:00:00' "
        "AND p.created_at < TIMESTAMP '2025-11-18 17:00:00')"
    )


def test_windows_are_half_open():
    # >= on the lower bound, < on the upper — a row exactly at cover_hi belongs to the next window.
    sql = windowed_select("t", "ts", W)
    assert ">= TIMESTAMP '2025-11-15 17:00:00'" in sql
    assert "< TIMESTAMP '2025-11-18 17:00:00'" in sql
    assert "<=" not in sql


# ---- Readers wiring (fake Spark: records options, returns them from load()) ----
class _FakeReader:
    def __init__(self):
        self.opts = {}

    def format(self, fmt):
        self.opts["format"] = fmt
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def load(self):
        return self.opts


class _FakeSpark:
    @property
    def read(self):
        return _FakeReader()


class _FakeCtx:
    spark = _FakeSpark()
    window = W

    @staticmethod
    def secret(key):
        return f"jdbc:postgresql://fake/{key}"


def test_jdbc_windowed_delegates_to_jdbc_with_the_windowed_query():
    opts = Readers(_FakeCtx()).jdbc_windowed("pg_jdbc", "public.sale_transactions", "created_at")
    assert opts["format"] == "jdbc"
    assert opts["url"] == "jdbc:postgresql://fake/pg_jdbc"
    assert opts["query"] == windowed_select("public.sale_transactions", "created_at", W)


def test_jdbc_parent_windowed_delegates_with_the_semijoin_query():
    opts = Readers(_FakeCtx()).jdbc_parent_windowed(
        "pg_jdbc", "public.delivery_order_details", "public.delivery_orders", "order_id", "created_at")
    assert opts["query"] == parent_windowed_select(
        "public.delivery_order_details", "public.delivery_orders", "order_id", "created_at", W)
