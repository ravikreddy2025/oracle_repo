"""High-water mark storage and the arithmetic around it.

Two rules govern this module, both from DESIGN 4:

1. A watermark advances **only** after the data write for that run succeeded.
   The caller enforces the ordering; this module enforces that the advance is
   fenced by ``run_id`` and monotonic.
2. Monotonicity is checked in SQL, not just in Python. A read-then-write check
   in the driver would race two concurrent runs of the same table; the
   comparison lives in the MERGE's ``WHEN MATCHED AND ...`` clause so the
   engine arbitrates.

Watermark values are stored as canonical strings with a type tag next to them,
so one column serves timestamp, date, and numeric/SCN watermarks. Typed
comparison happens explicitly, never as an accident of string ordering.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .schema import WATERMARKS, qualify
from .sql_client import SqlClient

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATE_FORMAT = "%Y-%m-%d"

TIMESTAMP_TYPES = {"timestamp", "date"}


class WatermarkError(ValueError):
    """Raised when a watermark value cannot be interpreted for its declared type."""


@dataclass(frozen=True)
class WatermarkRecord:
    table_fqn: str
    env: str
    watermark_column: str | None
    watermark_type: str
    watermark_value: str | None
    previous_value: str | None = None
    run_id: str | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "WatermarkRecord":
        return cls(
            table_fqn=row["table_fqn"],
            env=row["env"],
            watermark_column=row.get("watermark_column"),
            watermark_type=row.get("watermark_type") or "timestamp",
            watermark_value=row.get("watermark_value"),
            previous_value=row.get("previous_value"),
            run_id=row.get("run_id"),
            updated_at=row.get("updated_at"),
        )


@dataclass(frozen=True)
class WatermarkWindow:
    """The lower bound an incremental extract should use for this run."""

    lower_bound: str | None
    stored_value: str | None
    overlap_applied: bool
    is_first_run: bool

    @property
    def has_bound(self) -> bool:
        return self.lower_bound is not None


# -- pure value handling ----------------------------------------------------


def canonicalize(value: Any, watermark_type: str) -> str | None:
    """Render a watermark value as its canonical string form."""
    if value is None:
        return None
    wtype = watermark_type.lower()
    if wtype == "number":
        try:
            return str(Decimal(str(value)))
        except InvalidOperation as exc:
            raise WatermarkError(f"{value!r} is not a valid numeric watermark") from exc
    if wtype == "date":
        parsed = value if isinstance(value, date) and not isinstance(value, datetime) else _to_datetime(value).date()
        return parsed.strftime(DATE_FORMAT)
    if wtype == "timestamp":
        return _to_datetime(value).strftime(TIMESTAMP_FORMAT)
    raise WatermarkError(f"unknown watermark_type {watermark_type!r}")


def parse(value: str | None, watermark_type: str) -> Any:
    """Turn a canonical string back into a comparable Python value."""
    if value is None:
        return None
    wtype = watermark_type.lower()
    if wtype == "number":
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise WatermarkError(f"{value!r} is not a valid numeric watermark") from exc
    if wtype == "date":
        return _to_datetime(value).date()
    if wtype == "timestamp":
        return _to_datetime(value)
    raise WatermarkError(f"unknown watermark_type {watermark_type!r}")


def should_advance(current: str | None, candidate: str | None, watermark_type: str) -> bool:
    """True when ``candidate`` is a strictly newer watermark than ``current``.

    A batch that returns no rows yields no candidate, and the watermark holds
    where it is -- that is a no-op, not a regression.
    """
    if candidate is None:
        return False
    if current is None:
        return True
    return parse(candidate, watermark_type) > parse(current, watermark_type)


def compute_window(
    record: WatermarkRecord | None,
    *,
    watermark_type: str,
    overlap: timedelta = timedelta(0),
    lower_bound_default: Any = None,
) -> WatermarkWindow:
    """Work out the lower bound for the next incremental extract.

    On the first run there is no stored mark, so the configured default is
    used. Afterwards the stored mark is rewound by ``overlap`` to re-scan for
    late-arriving updates -- safe because the load path is idempotent.

    Overlap is a duration, so it applies to time-typed watermarks only. For a
    numeric or SCN watermark it is not applied, and the window says so rather
    than silently pretending it was.
    """
    stored = record.watermark_value if record else None
    if stored is None:
        return WatermarkWindow(
            lower_bound=canonicalize(lower_bound_default, watermark_type)
            if lower_bound_default is not None
            else None,
            stored_value=None,
            overlap_applied=False,
            is_first_run=True,
        )

    if overlap and watermark_type.lower() in TIMESTAMP_TYPES:
        rewound = parse(stored, watermark_type)
        if watermark_type.lower() == "date":
            rewound = datetime.combine(rewound, datetime.min.time()) - overlap
            return WatermarkWindow(
                lower_bound=canonicalize(rewound.date(), watermark_type),
                stored_value=stored,
                overlap_applied=True,
                is_first_run=False,
            )
        return WatermarkWindow(
            lower_bound=canonicalize(rewound - overlap, watermark_type),
            stored_value=stored,
            overlap_applied=True,
            is_first_run=False,
        )

    return WatermarkWindow(
        lower_bound=stored, stored_value=stored, overlap_applied=False, is_first_run=False
    )


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip().replace("T", " ")
    for fmt in (TIMESTAMP_FORMAT, "%Y-%m-%d %H:%M:%S", DATE_FORMAT):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise WatermarkError(
        f"{value!r} is not a recognised timestamp; expected 'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'"
    )


# -- comparison pushed into SQL --------------------------------------------

_CAST_BY_TYPE = {
    "timestamp": "TO_TIMESTAMP({expr})",
    "date": "TO_DATE({expr})",
    "number": "CAST({expr} AS DECIMAL(38,0))",
}


def advance_guard_sql(watermark_type: str) -> str:
    """The ``WHEN MATCHED AND ...`` predicate that makes an advance monotonic."""
    template = _CAST_BY_TYPE.get(watermark_type.lower())
    if template is None:
        raise WatermarkError(f"unknown watermark_type {watermark_type!r}")
    new = template.format(expr="s.watermark_value")
    old = template.format(expr="t.watermark_value")
    # A NULL stored value means 'never loaded', which any candidate beats.
    return f"(t.watermark_value IS NULL OR {new} > {old})"


# -- store ------------------------------------------------------------------


class WatermarkStore:
    """Reads and advances watermarks in the control plane."""

    def __init__(self, client: SqlClient, catalog: str, schema: str) -> None:
        self._client = client
        self._table = qualify(catalog, schema, WATERMARKS.name)

    @property
    def table(self) -> str:
        return self._table

    def get(self, table_fqn: str, env: str) -> WatermarkRecord | None:
        rows = self._client.query(
            f"SELECT * FROM {self._table} WHERE table_fqn = :table_fqn AND env = :env",
            {"table_fqn": table_fqn, "env": env},
        )
        return WatermarkRecord.from_row(rows[0]) if rows else None

    def window(
        self,
        table_fqn: str,
        env: str,
        *,
        watermark_type: str,
        overlap: timedelta = timedelta(0),
        lower_bound_default: Any = None,
    ) -> WatermarkWindow:
        return compute_window(
            self.get(table_fqn, env),
            watermark_type=watermark_type,
            overlap=overlap,
            lower_bound_default=lower_bound_default,
        )

    def advance(
        self,
        table_fqn: str,
        env: str,
        *,
        new_value: Any,
        run_id: str,
        watermark_column: str | None,
        watermark_type: str,
        updated_at: datetime,
    ) -> bool:
        """Advance the mark if the new value is strictly newer. Returns whether it moved.

        Call this only after the data write for ``run_id`` has committed. The
        monotonic comparison is evaluated by the engine inside the MERGE, so two
        concurrent runs cannot interleave a read and a write to move it backwards.
        """
        canonical = canonicalize(new_value, watermark_type)
        if canonical is None:
            return False

        guard = advance_guard_sql(watermark_type)
        statement = f"""
MERGE INTO {self._table} AS t
USING (
  SELECT :table_fqn AS table_fqn,
         :env AS env,
         :watermark_column AS watermark_column,
         :watermark_type AS watermark_type,
         :watermark_value AS watermark_value,
         :run_id AS run_id,
         :updated_at AS updated_at
) AS s
ON t.table_fqn = s.table_fqn AND t.env = s.env
WHEN MATCHED AND {guard} THEN UPDATE SET
  t.watermark_column = s.watermark_column,
  t.watermark_type = s.watermark_type,
  t.previous_value = t.watermark_value,
  t.watermark_value = s.watermark_value,
  t.run_id = s.run_id,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
  table_fqn, env, watermark_column, watermark_type, watermark_value,
  previous_value, run_id, updated_at
) VALUES (
  s.table_fqn, s.env, s.watermark_column, s.watermark_type, s.watermark_value,
  NULL, s.run_id, s.updated_at
)
""".strip()

        self._client.execute(
            statement,
            {
                "table_fqn": table_fqn,
                "env": env,
                "watermark_column": watermark_column,
                "watermark_type": watermark_type,
                "watermark_value": canonical,
                "run_id": run_id,
                "updated_at": updated_at,
            },
        )
        # Report whether this call was the one that moved it, for logs/metrics.
        current = self.get(table_fqn, env)
        return bool(current and current.watermark_value == canonical and current.run_id == run_id)

    def force_set(
        self,
        table_fqn: str,
        env: str,
        *,
        value: Any,
        watermark_type: str,
        watermark_column: str | None,
        run_id: str,
        updated_at: datetime,
    ) -> None:
        """Set the mark unconditionally, including backwards.

        Only for deliberate operator action -- a backfill or a correction. The
        caller is expected to write an audit event alongside it; the normal
        ingest path must use :meth:`advance`.
        """
        canonical = canonicalize(value, watermark_type)
        statement = f"""
MERGE INTO {self._table} AS t
USING (
  SELECT :table_fqn AS table_fqn, :env AS env
) AS s
ON t.table_fqn = s.table_fqn AND t.env = s.env
WHEN MATCHED THEN UPDATE SET
  t.previous_value = t.watermark_value,
  t.watermark_value = :watermark_value,
  t.watermark_column = :watermark_column,
  t.watermark_type = :watermark_type,
  t.run_id = :run_id,
  t.updated_at = :updated_at
WHEN NOT MATCHED THEN INSERT (
  table_fqn, env, watermark_column, watermark_type, watermark_value,
  previous_value, run_id, updated_at
) VALUES (
  s.table_fqn, s.env, :watermark_column, :watermark_type, :watermark_value,
  NULL, :run_id, :updated_at
)
""".strip()
        self._client.execute(
            statement,
            {
                "table_fqn": table_fqn,
                "env": env,
                "watermark_value": canonical,
                "watermark_column": watermark_column,
                "watermark_type": watermark_type,
                "run_id": run_id,
                "updated_at": updated_at,
            },
        )
