"""Dump the control-plane DDL to sql/control/ for review outside Python.

The generated files are derived artefacts -- schema.py is the source of truth.
Regenerate with:

    python tools/emit_control_ddl.py --catalog prod_lakehouse --schema control
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.control.schema import ALL_TABLES, ddl_statements  # noqa: E402

HEADER = (
    "-- GENERATED FILE -- do not edit.\n"
    "-- Source: src/ingestion_framework/control/schema.py\n"
    "-- Regenerate: python tools/emit_control_ddl.py --catalog {catalog} --schema {schema}\n\n"
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="${catalog}", help="Unity Catalog name")
    parser.add_argument("--schema", default="control", help="Control schema name")
    parser.add_argument("--out", default="sql/control", help="Output directory")
    args = parser.parse_args(argv)

    # Placeholder catalogs are the point for a checked-in artefact, but the
    # identifier validator would reject them -- render with a safe stand-in and
    # substitute afterwards.
    literal_catalog, rendered_catalog = args.catalog, "CATALOG_PLACEHOLDER"
    use_placeholder = not literal_catalog.replace("_", "").isalnum()
    catalog = rendered_catalog if use_placeholder else literal_catalog

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    statements = ddl_statements(catalog, args.schema)
    header = HEADER.format(catalog=literal_catalog, schema=args.schema)

    combined = [header + statements[0] + ";"]
    for table, statement in zip(ALL_TABLES, statements[1:]):
        text = statement if not use_placeholder else statement.replace(rendered_catalog, literal_catalog)
        path = out_dir / f"{table.name}.sql"
        path.write_text(header + text + ";\n", encoding="utf-8")
        print(f"wrote {path}")
        combined.append(text + ";")

    all_path = out_dir / "_all.sql"
    text = "\n\n".join(combined)
    if use_placeholder:
        text = text.replace(rendered_catalog, literal_catalog)
    all_path.write_text(text + "\n", encoding="utf-8")
    print(f"wrote {all_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
