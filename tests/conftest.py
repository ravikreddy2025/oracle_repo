from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SHIPPED_CONFIG = REPO_ROOT / "config"


@pytest.fixture(scope="session")
def shipped_config() -> Path:
    """The real config tree that ships with the repo."""
    return SHIPPED_CONFIG


@pytest.fixture
def config_tree(tmp_path: Path) -> Path:
    """A minimal but valid config tree that tests can mutate freely.

    The schema lives in the repo, so tests point at the shipped one rather
    than duplicating it.
    """
    root = tmp_path / "config"
    (root / "environments").mkdir(parents=True)
    (root / "tables" / "finance").mkdir(parents=True)
    (root / "schema").mkdir(parents=True)
    (root / "schema" / "config.schema.json").write_text(
        (SHIPPED_CONFIG / "schema" / "config.schema.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    (root / "defaults.yaml").write_text(
        (SHIPPED_CONFIG / "defaults.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )

    (root / "environments" / "dev.yaml").write_text(
        """
version: 1
env: dev
source:
  secret_scope: oracle-dev
  jdbc:
    url: "jdbc:oracle:thin:@//dev:1521/DEV"
target:
  catalog: dev_lakehouse
  schema: bronze
control:
  catalog: dev_lakehouse
  schema: control
overrides: {}
""".strip(),
        encoding="utf-8",
    )

    (root / "tables" / "finance" / "widgets.yaml").write_text(
        """
version: 1
table:
  domain: finance
  source_schema: FINOWNER
  source_object: WIDGETS
  business_key: [WIDGET_ID]
extraction:
  mode: full
  columns: "*"
target:
  write_mode: merge
  merge_guard: none
""".strip(),
        encoding="utf-8",
    )
    return root


def write_yaml(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")
    return path


# -- local Spark ------------------------------------------------------------
#
# On Windows, Hadoop's local filesystem shells out to winutils.exe / hadoop.dll
# for four operations. Rather than install an unsigned third-party binary, the
# shim in tools/localspark answers those four with plain java.io. See
# tools/localspark/README.md for exactly what it does and does not change.

_SHIM_SOURCES = REPO_ROOT / "tools" / "localspark"
_SHIM_CONFIG = {
    "spark.hadoop.fs.file.impl": "local.fs.NoChmodLocalFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.file.impl": "local.fs.NoChmodLocalFs",
}


def _hadoop_client_jar() -> str | None:
    import glob
    import os

    try:
        import pyspark
    except ImportError:
        return None
    jars = glob.glob(
        os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar")
    )
    return jars[0] if jars else None


def _build_shim(out_dir: Path) -> Path | None:
    """Compile the local-filesystem shim. Returns the classes dir, or None."""
    import shutil
    import subprocess

    if shutil.which("javac") is None:
        return None
    jar = _hadoop_client_jar()
    if jar is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    sources = sorted(str(p) for p in _SHIM_SOURCES.glob("*.java"))
    result = subprocess.run(
        ["javac", "-cp", jar, "-d", str(out_dir), *sources],
        capture_output=True,
        text=True,
    )
    return out_dir if result.returncode == 0 else None


def _delta_jars() -> list[str]:
    """Delta jars from the Ivy cache, so the session needs no jar staging."""
    import glob
    import os

    for cache in ("~/.ivy2.5.2/jars", "~/.ivy2/jars"):
        jars = glob.glob(os.path.expanduser(os.path.join(cache, "*.jar")))
        if any("delta-spark" in j for j in jars):
            return [os.path.abspath(j) for j in jars]
    return []


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """A local Spark session with Delta, or skip.

    Delta semantics cannot be proved from SQL text, so these tests need a real
    engine. Everything Windows-specific is contained in the shim; on Linux and
    on Databricks the extra config is inert.
    """
    import os
    import sys

    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")

    workdir = tmp_path_factory.mktemp("spark")
    config = {}

    if sys.platform == "win32":
        classes = _build_shim(workdir / "shim")
        if classes is None:
            pytest.skip("Spark on Windows needs the localspark shim; no JDK/javac found")
        jars = _delta_jars()
        if not jars:
            pytest.skip("Delta jars not in the Ivy cache; run once with network access")
        # HADOOP_HOME merely has to exist: unset is fatal, missing winutils is a warning.
        hadoop_home = workdir / "hadoop"
        (hadoop_home / "bin").mkdir(parents=True, exist_ok=True)
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["SPARK_DIST_CLASSPATH"] = os.pathsep.join([str(classes), *jars])
        config.update(_SHIM_CONFIG)

    try:
        builder = (
            pyspark.sql.SparkSession.builder.master("local[2]")
            .appName("ingestion-framework-tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", str(workdir / "warehouse"))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        for key, value in config.items():
            builder = builder.config(key, value)
        if sys.platform != "win32":
            import delta

            builder = delta.configure_spark_with_delta_pip(builder)
        session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"no working local Spark/Delta: {type(exc).__name__}: {exc}")
    yield session
    session.stop()
