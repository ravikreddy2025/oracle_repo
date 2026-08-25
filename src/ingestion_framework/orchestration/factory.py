"""Assemble a live Engine from a Spark session.

Kept apart from the runners so that everything above this line is testable with
fakes, and everything Databricks-specific (SparkSession, dbutils secrets) is in
one place that only runs on a cluster.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ..control.audit import AuditLog
from ..control.control_store import ControlStore
from ..control.sql_client import SparkSqlClient
from ..control.watermark import WatermarkStore
from ..engine.extractor import OracleExtractor
from ..engine.loader import DeltaLoader
from ..engine.reconciler import Reconciler
from ..engine.run_spec import RunSpec
from ..engine.transformer import Transformer
from ..observability.alerts import AlertDispatcher
from ..observability.logger import StructuredLogger, get_logger
from .runner import Engine


class DbutilsSecrets:
    """Secret provider backed by ``dbutils.secrets``."""

    def __init__(self, dbutils: Any) -> None:
        self._dbutils = dbutils

    def get(self, scope: str, key: str) -> str:
        return self._dbutils.secrets.get(scope=scope, key=key)


def get_spark() -> Any:
    """The active SparkSession. On Databricks one always exists."""
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


def get_dbutils(spark: Any) -> Any | None:
    """``dbutils`` if we are on Databricks, else None."""
    try:  # pragma: no cover - only meaningful on a cluster
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        return globals().get("dbutils")


def build_engine(
    spec: RunSpec,
    *,
    spark: Any | None = None,
    secrets: Any | None = None,
    config_root: str | Path | None = None,
    run_id: str | None = None,
    actor: str | None = None,
    logger: StructuredLogger | None = None,
) -> Engine:
    """Wire the live components for one environment.

    The control catalog/schema come from the spec, so every component points at
    the same control plane the config declared.
    """
    spark = spark or get_spark()
    if secrets is None:
        dbutils = get_dbutils(spark)
        secrets = DbutilsSecrets(dbutils) if dbutils is not None else None

    client = SparkSqlClient(spark)
    log = logger or get_logger(spec.log_level, env=spec.env)

    return Engine(
        extractor=OracleExtractor(spark, secrets=secrets, config_root=config_root),
        transformer=Transformer(spark),
        loader=DeltaLoader(spark),
        reconciler=Reconciler(spark),
        control=ControlStore(client, spec.control.catalog, spec.control.schema),
        watermarks=WatermarkStore(client, spec.control.catalog, spec.control.schema),
        audit=AuditLog(
            client,
            spec.control.catalog,
            spec.control.schema,
            run_id=run_id,
            env=spec.env,
            actor=actor or _current_user(spark),
        ),
        logger=log,
        alerts=AlertDispatcher(log),
        now=datetime.utcnow,
    )


def _current_user(spark: Any) -> str | None:
    """Who is running this, for the audit trail."""
    try:  # pragma: no cover - cluster-only
        return spark.sql("SELECT current_user()").collect()[0][0]
    except Exception:
        return None
