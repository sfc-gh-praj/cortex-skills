#!/usr/bin/env python3
"""
parse_bim.py - Parse an SSAS Tabular model file into a structured inventory JSON.

Accepts both:
  - model.bim  : plain JSON (Visual Studio / SSDT format, compat 1200+)
  - model.xmla : SOAP/XMLA envelope produced by SSMS "Script Database As → CREATE TO → File"
                 The XMLA wrapper is stripped and the embedded TMSL JSON is parsed identically.

Compatibility levels 1100/1103 use XML/ASSL format — this script will error and print
export instructions.

Usage:
    # .bim file
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/parse_bim.py \
        --bim-path /path/to/model.bim --output inventory.json

    # .xmla file (SSMS export)
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/parse_bim.py \
        --bim-path /path/to/model.xmla --output inventory.json
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_str(val) -> str:
    if isinstance(val, list):
        return "\n".join(str(v) for v in val).strip()
    return (str(val) if val else "").strip()


# ---------------------------------------------------------------------------
# Column / measure / partition parsers
# ---------------------------------------------------------------------------

def parse_column(col: dict) -> dict:
    return {
        "name": col.get("name", ""),
        "source_column": col.get("sourceColumn"),
        "data_type": col.get("dataType", "string"),
        "is_hidden": col.get("isHidden", False),
        "is_calculated": col.get("type") == "calculated",
        "expression": _to_str(col.get("expression", "")) if col.get("type") == "calculated" else None,
        "format_string": col.get("formatString"),
        "description": col.get("description"),
        "is_key": col.get("isKey", False),
        "sort_by_column": col.get("sortByColumn"),
    }


def parse_measure(m: dict) -> dict:
    return {
        "name": m.get("name", ""),
        "expression": _to_str(m.get("expression", "")),
        "format_string": m.get("formatString"),
        "description": m.get("description"),
        "kpi": parse_kpi(m.get("kpi")) if m.get("kpi") else None,
    }


def _expr_str(val) -> str:
    """Normalise a BIM expression field that may be a string or list of strings."""
    if isinstance(val, list):
        return "\n".join(val).strip()
    return (val or "").strip()


def parse_kpi(kpi: dict) -> dict:
    if not kpi:
        return None
    return {
        "target_expression": _expr_str(kpi.get("targetExpression")),
        "status_expression": _expr_str(kpi.get("statusExpression")),
        "trend_expression":  _expr_str(kpi.get("trendExpression")),
        "status_graphic":    kpi.get("statusGraphic"),
    }


def parse_hierarchy(h: dict) -> dict:
    return {
        "name": h.get("name", ""),
        "levels": [
            {
                "name":    lv.get("name", ""),
                "column":  lv.get("column", ""),
                "ordinal": lv.get("ordinal", i),
            }
            for i, lv in enumerate(h.get("levels", []))
        ],
    }


def parse_partition(p: dict) -> dict:
    source = p.get("source", {})
    expr = source.get("expression")
    return {
        "name":        p.get("name", ""),
        "mode":        p.get("mode", "import"),   # import | directQuery | push
        "source_type": source.get("type", "unknown"),
        # M query (Power Query) expression — list of strings in compat 1400+
        "expression":  "\n".join(expr) if isinstance(expr, list) else (expr or "").strip(),
        "query":       (source.get("query") or "").strip(),
    }


# ---------------------------------------------------------------------------
# Calculation group parser (compat 1500+)
# ---------------------------------------------------------------------------

def parse_calculation_group(t: dict) -> dict | None:
    cg = t.get("calculationGroup")
    if not cg:
        return None
    items = []
    for i, item in enumerate(cg.get("calculationItems", [])):
        items.append({
            "name":                    item.get("name", f"Item{i}"),
            "expression":              _to_str(item.get("expression", "")),
            "format_string_expression": _to_str(item.get("formatStringExpression", "")),
            "ordinal":                 item.get("ordinal", i),
        })
    # The column that holds calculation item names (visible in reports)
    cg_col = next(
        (c.get("name") for c in t.get("columns", [])
         if c.get("type") == "calculationGroupContent"),
        "Calculation",
    )
    return {
        "precedence":   cg.get("precedence", 0),
        "column_name":  cg_col,
        "items":        items,
    }


# ---------------------------------------------------------------------------
# Calculated-table detection
# ---------------------------------------------------------------------------

def _is_calculated_table(t: dict) -> bool:
    """
    A calculated table has a single partition whose source type is 'calculated',
    or carries a top-level 'expression' key (older compat levels).
    """
    if t.get("expression"):
        return True
    partitions = t.get("partitions", [])
    return (
        len(partitions) == 1
        and (partitions[0].get("source", {}).get("type") == "calculated"
             or partitions[0].get("mode") == "calculated")
    )


# ---------------------------------------------------------------------------
# Table parser
# ---------------------------------------------------------------------------

def parse_table(t: dict) -> dict:
    columns = [parse_column(c) for c in t.get("columns", []) if c.get("type") != "rowNumber"]
    calculated_cols = [c for c in columns if c["is_calculated"]]
    regular_cols    = [c for c in columns if not c["is_calculated"]]
    return {
        "name":               t.get("name", ""),
        "description":        t.get("description"),
        "is_hidden":          t.get("isHidden", False),
        # Storage mode: import | directQuery | dual
        "storage_mode":       t.get("storageMode", "import"),
        # isDateTable: True means this table is the model's date/calendar dimension
        "is_date_table":      t.get("isDateTable", False) or t.get("showAsVariationsOnly", False),
        # Calculated tables are defined entirely by a DAX expression
        "is_calculated_table": _is_calculated_table(t),
        # Calculation group (compat 1500+) — None for normal tables
        "calculation_group":  parse_calculation_group(t),
        "columns":            regular_cols,
        "calculated_columns": calculated_cols,
        "measures":           [parse_measure(m) for m in t.get("measures", [])],
        "hierarchies":        [parse_hierarchy(h) for h in t.get("hierarchies", [])],
        "partitions":         [parse_partition(p) for p in t.get("partitions", [])],
    }


# ---------------------------------------------------------------------------
# Relationship parser
# ---------------------------------------------------------------------------

def parse_relationship(r: dict) -> dict:
    return {
        "name":            r.get("name", ""),
        "from_table":      r.get("fromTable", ""),
        "from_column":     r.get("fromColumn", ""),
        "to_table":        r.get("toTable", ""),
        "to_column":       r.get("toColumn", ""),
        "is_active":       r.get("isActive", True),
        "cross_filtering": r.get("crossFilteringBehavior", "oneDirection"),
        "cardinality":     r.get("fromCardinality", "many") + "_to_" + r.get("toCardinality", "one"),
    }


# ---------------------------------------------------------------------------
# Role parser — includes OLS (table-level + column-level)
# ---------------------------------------------------------------------------

def parse_role(role: dict) -> dict:
    permissions = []
    for tp in role.get("tablePermissions", []):
        col_perms = [
            {
                "name":                cp.get("name", ""),
                "metadata_permission": cp.get("metadataPermission", "read"),
            }
            for cp in tp.get("columnPermissions", [])
        ]
        permissions.append({
            "table":              tp.get("name", ""),
            "filter_expression":  (tp.get("filterExpression") or "").strip(),
            # OLS: "none" means hide the table entirely (metadata + data)
            "metadata_permission": tp.get("metadataPermission", "read"),
            # Column-level OLS: individual columns hidden
            "column_permissions": col_perms,
        })
    return {
        "name":             role.get("name", ""),
        "description":      role.get("description"),
        "model_permission": role.get("modelPermission", "read"),
        "table_permissions": permissions,
    }


# ---------------------------------------------------------------------------
# Perspective parser
# ---------------------------------------------------------------------------

def parse_perspective(p: dict) -> dict:
    tables = []
    for pt in p.get("perspectiveTables", []):
        tables.append({
            "name":     pt.get("name", ""),
            "columns":  [c.get("name") for c in pt.get("columns", [])],
            "measures": [m.get("name") for m in pt.get("measures", [])],
        })
    return {
        "name":   p.get("name", ""),
        "tables": tables,
    }


# ---------------------------------------------------------------------------
# Summary + complexity scoring
# ---------------------------------------------------------------------------

def score_complexity(inventory: dict) -> str:
    tables             = inventory["tables"]
    measure_count      = sum(len(t["measures"]) for t in tables)
    role_count         = len(inventory["roles"])
    relationship_count = len(inventory["relationships"])
    calc_group_count   = sum(1 for t in tables if t.get("calculation_group"))

    complex_dax_kw = [
        "CALCULATE", "FILTER", "RELATED", "ALL", "ALLEXCEPT",
        "TOTALYTD", "SAMEPERIODLASTYEAR", "DATEADD", "RANKX", "TOPN",
        "SELECTEDMEASURE",
    ]
    expressions = [
        m["expression"]
        for t in tables for m in t["measures"]
    ] + [
        c["expression"]
        for t in tables for c in t["calculated_columns"] if c["expression"]
    ]
    complex_count = sum(
        1 for e in expressions if any(kw in e.upper() for kw in complex_dax_kw)
    )

    if measure_count > 50 or complex_count > 20 or role_count > 5 or calc_group_count > 0:
        return "complex"
    elif measure_count > 15 or complex_count > 5 or relationship_count > 10:
        return "moderate"
    return "simple"


def build_summary(inventory: dict) -> dict:
    tables         = inventory["tables"]
    measure_count  = sum(len(t["measures"]) for t in tables)
    calc_col_count = sum(len(t["calculated_columns"]) for t in tables)
    hierarchy_count = sum(len(t["hierarchies"]) for t in tables)
    kpi_count      = sum(1 for t in tables for m in t["measures"] if m.get("kpi"))
    partition_count = sum(len(t["partitions"]) for t in tables)

    has_ols = any(
        tp.get("metadata_permission") == "none" or tp.get("column_permissions")
        for r in inventory["roles"]
        for tp in r["table_permissions"]
    )
    storage_modes   = sorted({t["storage_mode"] for t in tables})
    date_table_names = [t["name"] for t in tables if t.get("is_date_table")]
    calc_group_count = sum(1 for t in tables if t.get("calculation_group"))
    calc_table_count = sum(1 for t in tables if t.get("is_calculated_table"))

    return {
        "table_count":            len(tables),
        "measure_count":          measure_count,
        "calculated_column_count": calc_col_count,
        "relationship_count":     len(inventory["relationships"]),
        "role_count":             len(inventory["roles"]),
        "hierarchy_count":        hierarchy_count,
        "kpi_count":              kpi_count,
        "partition_count":        partition_count,
        "calculation_group_count": calc_group_count,
        "calculated_table_count": calc_table_count,
        "perspective_count":      len(inventory["perspectives"]),
        "has_ols":                has_ols,
        "storage_modes":          storage_modes,
        "date_table_names":       date_table_names,
        "complexity":             score_complexity(inventory),
    }


# ---------------------------------------------------------------------------
# XMLA / BIM file loader
# ---------------------------------------------------------------------------

def _extract_json_from_xmla(content: str, path: Path) -> dict:
    """
    Extract and parse the TMSL JSON from an XMLA SOAP envelope.

    SSMS "Script Database As → CREATE TO → File" produces:

        <Envelope xmlns="...">
          <Body>
            <Execute xmlns="...">
              <Command>
                <Statement><![CDATA[{ ... TMSL JSON ... }]]></Statement>
              </Command>
            </Execute>
          </Body>
        </Envelope>

    SSMS CDATA-escapes any ]]> sequences inside the JSON as ]]]]><![CDATA[> so the
    XML is always well-formed. ElementTree stitches these back into a single text
    value automatically.

    The JSON inside <Statement> is identical to a .bim file and is parsed the same way.
    """
    # --- Strategy 1: proper XML parse (handles CDATA correctly) ---
    try:
        root = ET.fromstring(content)
        for elem in root.iter():
            local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if local == "Statement":
                text = (elem.text or "").strip()
                if text.startswith("{"):
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError as e:
                        print(f"ERROR: <Statement> content in XMLA is not valid JSON: {e}", file=sys.stderr)
                        print("  Ensure the XMLA was exported from SSAS Tabular compat 1200+ via SSMS.", file=sys.stderr)
                        sys.exit(1)
        # XML parsed OK but no JSON Statement — check for old ASSL
        for elem in root.iter():
            local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if local in ("Database", "ObjectDefinition", "MajorObject"):
                print("ERROR: XMLA file appears to use ASSL/XML format (compat < 1200).", file=sys.stderr)
                print("  parse_bim.py requires TMSL JSON format (compat 1200+).", file=sys.stderr)
                print("  In SSMS: right-click model → Script → CREATE TO → File (.xmla)", file=sys.stderr)
                print("  Or upgrade compatibility level to 1200+ in Visual Studio.", file=sys.stderr)
                sys.exit(1)
    except ET.ParseError:
        # XML parsing failed — fall through to regex strategy
        pass

    # --- Strategy 2: regex text scan (handles malformed / partially-escaped XML) ---
    # Extract text between <Statement> tags (including CDATA wrappers), then strip
    # CDATA markers to recover the raw JSON.
    import re
    # Match <Statement> ... </Statement> across newlines
    m = re.search(r"<Statement[^>]*>(.*?)</Statement>", content, re.DOTALL | re.IGNORECASE)
    if m:
        inner = m.group(1)
        # Strip all CDATA markers: <![CDATA[  and  ]]>
        inner = re.sub(r"<!\[CDATA\[", "", inner)
        inner = re.sub(r"\]\]>", "", inner)
        inner = inner.strip()
        if inner.startswith("{"):
            try:
                return json.loads(inner)
            except json.JSONDecodeError as e:
                print(f"ERROR: Extracted <Statement> content is not valid JSON: {e}", file=sys.stderr)
                print("  The XMLA may have been corrupted or use an unsupported format.", file=sys.stderr)
                sys.exit(1)

    print("ERROR: Could not find TMSL JSON content in XMLA file.", file=sys.stderr)
    print(f"  File: {path}", file=sys.stderr)
    print("  Expected SSMS 'Script Database As → CREATE TO → File (.xmla)' format.", file=sys.stderr)
    sys.exit(1)


def _load_raw_json(path: Path) -> dict:
    """
    Load the raw TMSL/model JSON dict from either a .bim or .xmla file.

    .bim  → plain JSON, read directly.
    .xmla → SOAP XML envelope; the TMSL JSON is extracted from <Statement>.
    Any other extension is treated as .bim (JSON) by default.
    """
    try:
        content = path.read_text(encoding="utf-8-sig").strip()
    except OSError as e:
        print(f"ERROR: Cannot read file: {e}", file=sys.stderr)
        sys.exit(1)

    # XMLA: XML content (starts with '<' or BOM-stripped '<?xml')
    if content.startswith("<"):
        return _extract_json_from_xmla(content, path)

    # BIM / plain JSON
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        print("ERROR: File is not valid JSON.", file=sys.stderr)
        print("  If this is a .xmla file, ensure it was exported from SSMS as a SOAP envelope.", file=sys.stderr)
        print("  Compatibility levels 1100/1103 use XML/ASSL format — not supported directly.", file=sys.stderr)
        print("  Export to JSON via SSMS:", file=sys.stderr)
        print("    Right-click database → Script Database as → CREATE TO → File", file=sys.stderr)
        print("  Or upgrade the model to compat level 1200+ in Visual Studio.", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_bim(bim_path: str) -> dict:
    path = Path(bim_path)
    if not path.exists():
        print(f"ERROR: File not found: {bim_path}", file=sys.stderr)
        sys.exit(1)

    raw = _load_raw_json(path)

    # Detect XML disguised as a file with .bim extension (legacy guard)
    if isinstance(raw, str) or (isinstance(raw, dict) and raw.get("xmlVersion")):
        print("ERROR: This appears to be an XML/ASSL model (compat < 1200).", file=sys.stderr)
        print("  Use SSMS to upgrade or export to JSON format.", file=sys.stderr)
        sys.exit(1)

    # Resolve the model object and compatibility level from three possible structures:
    #
    #  Structure A (SSMS XMLA / VS create script):
    #    { "create": { "database": { "compatibilityLevel": 1500, "model": { ... } } } }
    #
    #  Structure B (older .bim with top-level "model" key):
    #    { "model": { ... } }
    #
    #  Structure C (model dict directly at root):
    #    { "tables": [...], "relationships": [...], ... }

    database = raw.get("create", {}).get("database", {})
    if database.get("model"):
        # Structure A
        model        = database["model"]
        compat_level = database.get("compatibilityLevel", model.get("compatibilityLevel", 0))
        model_name   = database.get("name", path.stem)
    elif raw.get("model"):
        # Structure B
        model        = raw["model"]
        compat_level = raw.get("compatibilityLevel", model.get("compatibilityLevel", 0))
        model_name   = raw.get("name", path.stem)
    else:
        # Structure C — model is the root object itself
        model        = raw
        compat_level = raw.get("compatibilityLevel", 0)
        model_name   = raw.get("name", path.stem)
    try:
        compat_int = int(str(compat_level))
    except (ValueError, TypeError):
        compat_int = 0

    if compat_int and compat_int < 1200:
        print(f"ERROR: Compatibility level {compat_level} uses XML/ASSL format.", file=sys.stderr)
        print("  parse_bim.py requires JSON format (compat 1200+).", file=sys.stderr)
        print("  Export steps:", file=sys.stderr)
        print("    SSMS → right-click model → Script → CREATE TO → File (.bim)", file=sys.stderr)
        print("    Or open model in Visual Studio → change Compatibility Level → Save", file=sys.stderr)
        sys.exit(1)

    tables        = [parse_table(t) for t in model.get("tables", [])]
    relationships = [parse_relationship(r) for r in model.get("relationships", [])]
    roles         = [parse_role(r) for r in model.get("roles", [])]
    perspectives  = [parse_perspective(p) for p in model.get("perspectives", [])]

    inventory = {
        "model_name":     model_name,
        "compatibility_level": compat_level,
        "default_power_bi_data_source_version": model.get("defaultPowerBIDataSourceVersion"),
        "tables":         tables,
        "relationships":  relationships,
        "roles":          roles,
        "perspectives":   perspectives,
    }
    inventory["summary"] = build_summary(inventory)
    return inventory


def _run_snow_query(sql: str, connection: str, timeout: int = 30) -> list[dict] | None:
    """Run a SQL query via the snow CLI and return parsed JSON rows, or None on failure."""
    import subprocess
    try:
        result = subprocess.run(
            ["snow", "sql", "-q", sql, "--connection", connection, "--format", "json"],
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception:
        pass
    return None


def _get_sf_columns(source_db: str, source_schema: str, table_name: str,
                    connection: str) -> list[str]:
    """Return list of Snowflake physical column names (uppercase) for a table."""
    sql = (
        f"SELECT COLUMN_NAME "
        f"FROM {source_db.upper()}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{source_schema.upper()}' "
        f"AND TABLE_NAME = '{table_name.upper()}' "
        f"ORDER BY ORDINAL_POSITION;"
    )
    rows = _run_snow_query(sql, connection)
    if rows:
        return [r.get("COLUMN_NAME") or r.get("column_name", "") for r in rows]
    return []


def _parse_related_expr(expression: str) -> tuple[str, str] | None:
    """Parse RELATED('Table'[Column]) → (table_name, column_name) or None."""
    import re
    m = re.match(
        r"^\s*RELATED\s*\(\s*'?(\w[\w\s]*?)'?\s*\[(\w+)\]\s*\)\s*$",
        expression, re.I
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None


def _reconcile_columns(table: dict, sf_columns: list[str],
                       relationships: list[dict]) -> dict:
    """
    Build a column mapping between SSAS model names and Snowflake physical names.

    Returns dict with:
      sf_column_map:     {ssas_col_name: sf_physical_name} (case-insensitive match)
      sf_missing_columns: SSAS columns with no Snowflake match (renamed in BIM)
      sf_extra_columns:  Snowflake columns not exposed in the SSAS model
      calculated_column_resolution: per calc col, how to resolve it
    """
    tname = table["name"]

    # Build case-insensitive lookup: UPPER(sf_col) → sf_col
    sf_upper = {c.upper(): c for c in sf_columns}

    # Map each SSAS regular column → Snowflake physical name
    col_map = {}
    missing = []
    ssas_upper_set = set()
    for col in table["columns"]:
        ssas_name = col["name"]
        ssas_upper = ssas_name.upper()
        ssas_upper_set.add(ssas_upper)
        if ssas_upper in sf_upper:
            col_map[ssas_name] = sf_upper[ssas_upper]
        else:
            col_map[ssas_name] = None
            missing.append(ssas_name)

    # Snowflake columns not in the SSAS model
    extra = [c for c in sf_columns if c.upper() not in ssas_upper_set]

    # Resolve calculated columns
    calc_resolution = []
    for cc in table["calculated_columns"]:
        entry = {
            "name": cc["name"],
            "original_dax": cc.get("expression", ""),
            "strategy": "inline_sql",  # default: translate DAX inline
        }
        # Check for RELATED() cross-table pattern
        related = _parse_related_expr(cc.get("expression", ""))
        if related:
            rel_table, rel_column = related
            entry["strategy"] = "left_join"
            entry["related_table"] = rel_table
            entry["related_column"] = rel_column
            # Find the join path from relationships
            for r in relationships:
                if (r["from_table"].upper() == tname.upper()
                        and r["to_table"].upper() == rel_table.upper()):
                    entry["join_from_column"] = r["from_column"]
                    entry["join_to_column"] = r["to_column"]
                    break
                elif (r["to_table"].upper() == tname.upper()
                      and r["from_table"].upper() == rel_table.upper()):
                    entry["join_from_column"] = r["to_column"]
                    entry["join_to_column"] = r["from_column"]
                    break
        calc_resolution.append(entry)

    return {
        "sf_column_map": col_map,
        "sf_missing_columns": missing,
        "sf_extra_columns": extra,
        "calculated_column_resolution": calc_resolution,
    }


def enrich_with_snowflake(inventory: dict, source_db: str, source_schema: str, connection: str) -> dict:
    """
    Enrich each table in the inventory with actual row counts, byte sizes,
    column name reconciliation, and calculated column resolution strategies
    by querying the already-migrated Snowflake tables.
    Requires `snow` CLI on PATH and a valid --connection name.
    Falls back gracefully if a table is not found or snow is unavailable.
    """
    import shutil

    if not shutil.which("snow"):
        print("  [enrich] 'snow' CLI not found — skipping Snowflake enrichment.")
        return inventory

    db_schema = f"{source_db.upper()}.{source_schema.upper()}"
    print(f"\n  Enriching inventory from {db_schema} (connection: {connection}) ...")

    for table in inventory["tables"]:
        tname = table["name"].upper()

        # Row count + byte size from INFORMATION_SCHEMA
        size_sql = (
            f"SELECT ROW_COUNT, BYTES "
            f"FROM {source_db.upper()}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{source_schema.upper()}' "
            f"AND TABLE_NAME = '{tname}';"
        )
        rows = _run_snow_query(size_sql, connection)
        if rows:
            table["sf_row_count"] = rows[0].get("ROW_COUNT") or rows[0].get("row_count", 0)
            table["sf_bytes"]     = rows[0].get("BYTES") or rows[0].get("bytes", 0)
            mb = round((table["sf_bytes"] or 0) / 1_048_576, 2)
            print(f"    {tname:35s} rows={table['sf_row_count']:>10,}  size={mb:>7.2f} MB")
        else:
            print(f"    {tname:35s} not found in {db_schema} — skipping")
            continue

        # Column reconciliation: map SSAS names → Snowflake physical names
        sf_columns = _get_sf_columns(source_db, source_schema, tname, connection)
        if sf_columns:
            recon = _reconcile_columns(table, sf_columns, inventory["relationships"])
            table["sf_column_map"]                 = recon["sf_column_map"]
            table["sf_missing_columns"]            = recon["sf_missing_columns"]
            table["sf_extra_columns"]              = recon["sf_extra_columns"]
            table["calculated_column_resolution"]  = recon["calculated_column_resolution"]
            n_mapped = sum(1 for v in recon["sf_column_map"].values() if v is not None)
            n_miss   = len(recon["sf_missing_columns"])
            n_extra  = len(recon["sf_extra_columns"])
            n_join   = sum(1 for c in recon["calculated_column_resolution"]
                          if c["strategy"] == "left_join")
            print(f"      columns: {n_mapped} mapped, {n_miss} missing, "
                  f"{n_extra} sf-only, {n_join} cross-table JOIN(s)")

    inventory["source_db"]     = source_db
    inventory["source_schema"] = source_schema
    # Flag: DDL phase should emit CREATE VIEW … AS SELECT FROM source rather than CREATE TABLE
    inventory["data_already_in_snowflake"] = True
    print()
    return inventory


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Parse an SSAS Tabular model file (.bim or .xmla) to inventory JSON (compat 1200+).\n"
            "Accepts both Visual Studio .bim files and SSMS XMLA SOAP exports."
        )
    )
    parser.add_argument(
        "--bim-path", required=True,
        help="Path to model file: model.bim (Visual Studio) or model.xmla (SSMS export)",
    )
    parser.add_argument("--output",         required=True,  help="Output inventory JSON file path")
    parser.add_argument("--source-db",      required=False, default=None,
                        help="Snowflake DB containing already-migrated source tables (e.g. ADVENTUREWORKSDW2022)")
    parser.add_argument("--source-schema",  required=False, default="DBO",
                        help="Schema within --source-db (default: DBO)")
    parser.add_argument("--connection",     required=False, default="COCO_JK",
                        help="snow CLI connection name for Snowflake enrichment (default: COCO_JK)")
    args = parser.parse_args()

    inventory = parse_bim(args.bim_path)

    if args.source_db:
        inventory = enrich_with_snowflake(
            inventory, args.source_db, args.source_schema, args.connection
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2, default=str)

    s = inventory["summary"]
    print(f"Parsed: {inventory['model_name']}  (compat {inventory['compatibility_level']})")
    print(f"  Tables:              {s['table_count']}  {('(incl. ' + str(s['calculated_table_count']) + ' calculated)') if s['calculated_table_count'] else ''}")
    print(f"  Measures:            {s['measure_count']}")
    print(f"  Calculated cols:     {s['calculated_column_count']}")
    print(f"  Relationships:       {s['relationship_count']}")
    print(f"  Roles (RLS):         {s['role_count']}")
    print(f"  Object-level sec:    {'YES' if s['has_ols'] else 'no'}")
    print(f"  Hierarchies:         {s['hierarchy_count']}")
    print(f"  KPIs:                {s['kpi_count']}")
    print(f"  Partitions:          {s['partition_count']}")
    print(f"  Calculation groups:  {s['calculation_group_count']}")
    print(f"  Perspectives:        {s['perspective_count']}")
    print(f"  Storage modes:       {', '.join(s['storage_modes'])}")
    if s["date_table_names"]:
        print(f"  Date tables:         {', '.join(s['date_table_names'])}")
    print(f"  Complexity:          {s['complexity'].upper()}")
    print(f"  Output:              {args.output}")


if __name__ == "__main__":
    main()
