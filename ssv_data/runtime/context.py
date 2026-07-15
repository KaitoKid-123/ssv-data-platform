"""PipelineContext — the one object threaded through a run.

Light OOP on purpose: it only bundles spark + window + secret-resolver +
table-naming so transforms stay pure functions and don't take 6 args each.
DataFrames never live on this object as state.
"""
from dataclasses import dataclass, field
from typing import Any, Callable

from ssv_data.config import BRONZE, SILVER, GOLD
from ssv_data.runtime.window import RunWindow


def _no_secret(key: str) -> str:  # default resolver; override in Fabric
    raise RuntimeError(f"No secret resolver configured (asked for '{key}'). "
                       f"Pass secret=mssparkutils.credentials.getSecret.")


@dataclass
class PipelineContext:
    spark: Any
    window: RunWindow
    secret: Callable[[str], str] = field(default=_no_secret)
    schema_enabled: bool = True
    table_format: str = "delta"   # "delta" in Fabric; "parquet" for local dev/test without Delta
    logger: Any = None

    def table(self, layer: str, name: str) -> str:
        return f"{layer}.{name}" if self.schema_enabled else f"{layer}_{name}"

    def bronze(self, name: str) -> str:
        return self.table(BRONZE, name)

    def silver(self, name: str) -> str:
        return self.table(SILVER, name)

    def gold(self, name: str) -> str:
        return self.table(GOLD, name)

    @property
    def running_date(self) -> str:
        return self.window.report_date

    @property
    def replace_where(self) -> str:
        """Standard per-day idempotency predicate for write_delta."""
        return f"report_date = '{self.window.report_date}'"