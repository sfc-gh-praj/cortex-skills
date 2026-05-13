#!/usr/bin/env python3
"""
generate_ddl.py - Generate Snowflake DDL from an SSAS Tabular inventory JSON.

Without --assessment:  emits CREATE TABLE + calculated column VIEWs.
With    --assessment:  routes each table to INTERACTIVE TABLE, REGULAR TABLE,
                       or CALCULATED VIEW based on the workload scoring output
                       of assess_deployment.py.

Usage:
    # Basic
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_ddl.py \
        --inventory inventory.json \
        --target-schema MY_DB.MY_SCHEMA \
        --output ddl.sql

    # With workload assessment
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_ddl.py \
        --inventory inventory.json \
        --assessment deployment_assessment.json \
        --measures-json measures_translated.json \
        --target-schema MY_DB.MY_SCHEMA \
        --output ddl.sql
"""

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

TYPE_MAP = {
    "int64":    "BIGINT",
    "int":      "BIGINT",
    "integer":  "BIGINT",
    "double":   "FLOAT",
    "decimal":  "NUMBER(38, 10)",
    "currency": "NUMBER(19, 4)",
    "string":   "VARCHAR",
    "text":     "VARCHAR",
    "datetime": "TIMESTAMP_NTZ",
    "date":     "DATE",
    "time":     "TIME",
    "boolean":  "BOOLEAN",
    "bool":     "BOOLEAN",
    "binary":   "BINARY",
    "variant":  "VARIANT",
}
DEFAULT_TYPE = "VARCHAR"


def snowflake_type(data_type: str) -> str:
    return TYPE_MAP.get((data_type or "string").lower(), DEFAULT_TYPE)


def quote_name(name: str) -> str:
    if re.search(r"[\s\-\/\(\)]", name) or name.upper() in (
        "DATE", "TIME", "YEAR", "MONTH", "DAY", "NAME", "VALUE", "KEY", "INDEX",
        "ORDER", "GROUP", "SELECT", "FROM", "WHERE", "TABLE",
    ):
        return f'"{name}"'
    return name.upper().replace(" ", "_")


def _select_col_expr(col: dict) -> str:
    src = col.get("source_column")
    ssas_name = col["name"]
    if col.get("is_calculated"):
        return quote_name(ssas_name)
    if src and src != ssas_name:
        return f"{quote_name(src)} AS {quote_name(ssas_name)}"
    return quote_name(ssas_name)


# ---------------------------------------------------------------------------
# Partition → clustering key inference (used when no assessment provided)
# ---------------------------------------------------------------------------

def infer_clustering(partitions: list[dict]) -> list[str]:
    cluster_cols = []
    date_patterns = [
        r"\[(\w*[Dd]ate\w*)\]",
        r"WHERE\s+(\w*[Dd]ate\w*)\s*[><=]",
        r"(\w*[Yy]ear\w*|\w*[Mm]onth\w*|\w*[Pp]eriod\w*)",
    ]
    for p in partitions:
        expr = (p.get("expression") or "") + (p.get("query") or "")
        for pat in date_patterns:
            for hit in re.findall(pat, expr):
                col = quote_name(hit)
                if col not in cluster_cols:
                    cluster_cols.append(col)
    return cluster_cols[:2]


# ---------------------------------------------------------------------------
# DDL generators
# ---------------------------------------------------------------------------

def _col_defs(table: dict) -> list[str]:
    col_defs = []
    for col in table["columns"]:
        cname = quote_name(col["name"])
        ctype = snowflake_type(col["data_type"])
        nn    = " NOT NULL" if col.get("is_key") else ""
        col_defs.append(f"    {cname} {ctype}{nn}")
    return col_defs or ["    _placeholder VARCHAR  -- no regular columns found"]


def _calc_view_cols(table: dict, measures_map: dict) -> list[str]:
    view_cols = []
    for c in table["calculated_columns"]:
        item = measures_map.get((table["name"], c["name"]))
        if item and item.get("sql_translation"):
            view_cols.append(
                f"    {item['sql_translation']} AS {quote_name(c['name'])}"
            )
        else:
            dax = (c.get("expression") or "").replace("\n", " ")[:120]
            view_cols.append(
                f"    NULL::VARCHAR AS {quote_name(c['name'])}"
                f"  /* TODO DAX: {dax} */"
            )
    return view_cols


def table_ddl_regular(table: dict, schema: str, measures_map: dict,
                      cluster_cols: list[str]) -> str:
    tname     = quote_name(table["name"])
    full_name = f"{schema}.{tname}"
    lines     = []

    lines.append(f"CREATE TABLE IF NOT EXISTS {full_name} (")
    lines.append(",\n".join(_col_defs(table)))
    if cluster_cols:
        lines.append(f") CLUSTER BY ({', '.join(cluster_cols)});")
    else:
        lines.append(");")

    # Calculated columns view
    if table["calculated_columns"]:
        view_name = f"{schema}.{quote_name(table['name'] + '_V')}"
        base_cols = ", ".join(quote_name(c["name"]) for c in table["columns"])
        view_cols = _calc_view_cols(table, measures_map)
        lines += [
            "",
            f"CREATE OR REPLACE VIEW {view_name} AS",
            "SELECT",
            f"    {base_cols}," if base_cols else "",
            ",\n".join(view_cols),
            f"FROM {full_name};",
        ]
    return "\n".join(l for l in lines if l is not None)


def table_ddl_view_over_source(table: dict, schema: str, source_db: str,
                                source_schema: str, measures_map: dict) -> str:
    """
    Emit a CREATE OR REPLACE VIEW in the target schema that wraps the already-migrated
    source table in source_db.source_schema. Uses sf_column_map for physical→SSAS aliasing
    and appends calculated columns as inline expressions.
    """
    tname        = quote_name(table["name"])
    full_name    = f"{schema}.{tname}"
    source_table = f"{source_db}.{source_schema}.{table['name'].upper()}"
    sf_col_map   = table.get("sf_column_map", {})   # SSAS name → Snowflake physical name

    select_cols = []
    for col in table["columns"]:
        ssas_name  = col["name"]
        sf_phys    = sf_col_map.get(ssas_name) or ssas_name.upper()  # None → use SSAS name uppercased
        quoted_src = quote_name(sf_phys)   # quote reserved words in source ref
        quoted_out = quote_name(ssas_name)
        # Add alias only when source identifier differs from output identifier
        if quoted_src.upper() != quoted_out.upper():
            select_cols.append(f"    {quoted_src} AS {quoted_out}")
        else:
            select_cols.append(f"    {quoted_src}")

    # Append calculated columns as NULL placeholders or translated SQL
    calc_cols = _calc_view_cols(table, measures_map)

    lines = [
        f"-- Source data already in Snowflake — wrapping {source_table} as a view",
        f"CREATE OR REPLACE VIEW {full_name} AS",
        "SELECT",
    ]
    all_col_lines = select_cols + ["    " + c.lstrip() for c in calc_cols]
    if all_col_lines:
        lines.append(",\n".join(all_col_lines))
    else:
        lines.append("    *")
    lines.append(f"FROM {source_table};")
    return "\n".join(lines)


def table_ddl_interactive(table: dict, schema: str, measures_map: dict,
                           cluster_cols: list[str], target_lag: str,
                           maint_wh: str = "maintenance_wh",
                           init_wh: str  = "xl_init_wh") -> str:
    tname     = quote_name(table["name"])
    full_name = f"{schema}.{tname}"
    cluster_expr = ", ".join(cluster_cols) if cluster_cols else "_placeholder"
    lines = [
        f"-- Interactive Table: optimised for high-concurrency selective queries",
        f"-- Cluster key: {cluster_expr}  |  Refresh lag: {target_lag}",
        f"CREATE INTERACTIVE TABLE IF NOT EXISTS {full_name}",
        f"  CLUSTER BY ({cluster_expr})",
        f"  TARGET_LAG = '{target_lag}'",
        f"  WAREHOUSE = {maint_wh}",
        f"  INITIALIZATION_WAREHOUSE = {init_wh}",
        f"AS SELECT",
        "    " + ", ".join(_select_col_expr(c) for c in table["columns"]) or "    *",
        f"FROM {full_name}_SOURCE;   -- replace _SOURCE with your actual source table",
        "",
    ]
    if table["calculated_columns"]:
        view_name = f"{schema}.{quote_name(table['name'] + '_V')}"
        base_cols = ", ".join(quote_name(c["name"]) for c in table["columns"])
        view_cols = _calc_view_cols(table, measures_map)
        lines += [
            f"CREATE OR REPLACE VIEW {view_name} AS",
            "SELECT",
            f"    {base_cols}," if base_cols else "",
            ",\n".join(view_cols),
            f"FROM {full_name};",
        ]
    return "\n".join(lines)


def table_ddl_calculated_view(table: dict, schema: str, measures_map: dict) -> str:
    """Calculated tables are entirely DAX-defined — emit as a Snowflake VIEW."""
    tname     = quote_name(table["name"])
    full_name = f"{schema}.{tname}"
    # Find the translated expression for the calculated table itself
    item = measures_map.get((table["name"], "__table__"))
    dax_hint = ""
    for p in table.get("partitions", []):
        if p.get("expression"):
            dax_hint = p["expression"].replace("\n", " ")[:200]
            break
    if item and item.get("sql_translation"):
        select_expr = item["sql_translation"]
    else:
        select_expr = (
            f"/* TODO: translate DAX expression:\n"
            f"   {dax_hint}\n"
            f"   to a SELECT statement */"
        )
    return (
        f"-- Calculated table: defined by a DAX expression in the SSAS model\n"
        f"CREATE OR REPLACE VIEW {full_name} AS\n"
        f"{select_expr};\n"
    )


# ---------------------------------------------------------------------------
# OLS (Object-Level Security) SQL generation
# ---------------------------------------------------------------------------

def ols_sql(roles: list[dict], schema: str) -> list[str]:
    """Generate REVOKE / masking policy hints for Object-Level Security."""
    lines = ["-- === Object-Level Security (OLS) ==="]
    found = False
    for role in roles:
        role_sf = role["name"].upper().replace(" ", "_") + "_ROLE"
        for tp in role["table_permissions"]:
            tname = quote_name(tp["table"])
            # Table-level OLS: metadataPermission = none → REVOKE SELECT
            if tp.get("metadata_permission") == "none":
                lines.append(f"-- Role '{role['name']}' cannot see table '{tp['table']}'")
                lines.append(
                    f"REVOKE SELECT ON {schema}.{tname} FROM ROLE {role_sf};"
                )
                found = True
            # Column-level OLS
            for cp in tp.get("column_permissions", []):
                if cp.get("metadata_permission") == "none":
                    col = quote_name(cp["name"])
                    lines.append(
                        f"-- Column-level OLS: '{cp['name']}' hidden from role '{role['name']}'"
                    )
                    lines.append(
                        f"-- Option A (masking policy): CREATE MASKING POLICY mask_{cp['name'].lower()} "
                        f"AS (val VARCHAR) RETURNS VARCHAR -> "
                        f"CASE WHEN IS_ROLE_IN_SESSION('{role_sf}') THEN '***' ELSE val END;"
                    )
                    lines.append(
                        f"-- Option B (view): Create a role-specific view omitting column {col}"
                    )
                    found = True
    if not found:
        lines.append("-- No OLS restrictions found in this model.")
    return lines


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate Snowflake DDL from SSAS Tabular inventory"
    )
    parser.add_argument("--inventory",    required=True, help="Path to inventory.json")
    parser.add_argument("--target-schema",required=True,
                        help="Snowflake target schema, e.g. MY_DB.MY_SCHEMA")
    parser.add_argument("--output",       required=True, help="Output .sql file path")
    parser.add_argument("--assessment",   default=None,
                        help="Path to deployment_assessment.json from assess_deployment.py")
    parser.add_argument("--measures-json",default=None,
                        help="Path to measures_translated.json to embed calculated column SQL")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    assessment_map: dict[str, dict] = {}
    interactive_warehouse = None
    if args.assessment:
        with open(args.assessment, "r", encoding="utf-8") as f:
            assessment = json.load(f)
        assessment_map = {t["name"]: t for t in assessment.get("tables", [])}
        interactive_warehouse = assessment.get("interactive_warehouse")

    measures_map: dict[tuple, dict] = {}
    if args.measures_json:
        with open(args.measures_json, "r", encoding="utf-8") as f:
            for item in json.load(f):
                measures_map[(item.get("table"), item.get("name"))] = item

    schema = args.target_schema.strip().rstrip("/")

    # Source DB details — present when parse_bim.py was run with --source-db
    source_db     = inventory.get("source_db")
    source_schema = inventory.get("source_schema")
    use_source_views = bool(source_db and source_schema)

    blocks = [
        f"-- Snowflake DDL — SSAS Tabular model: {inventory['model_name']}",
        f"-- Compatibility level: {inventory['compatibility_level']}",
        f"-- Target schema: {schema}",
        f"-- Generated by ssas-tabular-migration skill",
        f"-- Mode: {'CREATE OR REPLACE VIEW over ' + source_db + '.' + source_schema if use_source_views else 'CREATE TABLE (data not yet in Snowflake)'}",
        "",
        f"USE SCHEMA {schema};",
        "",
    ]

    regular_count = interactive_count = view_count = skipped_count = 0

    for table in inventory["tables"]:
        if table.get("is_hidden"):
            skipped_count += 1
            continue

        tinfo = assessment_map.get(table["name"], {})
        rec   = tinfo.get("recommendation", "REGULAR_TABLE")

        blocks.append(f"-- ── Table: {table['name']}  [{table.get('storage_mode','import')}]")

        if rec == "CALCULATED_VIEW" or table.get("is_calculated_table"):
            blocks.append(table_ddl_calculated_view(table, schema, measures_map))
            view_count += 1

        elif rec == "INTERACTIVE_TABLE":
            cluster_cols = [quote_name(c) for c in tinfo.get("cluster_by_columns", [])]
            if not cluster_cols:
                cluster_cols = infer_clustering(table.get("partitions", []))
            target_lag = tinfo.get("target_lag") or "1 day"
            blocks.append(table_ddl_interactive(
                table, schema, measures_map, cluster_cols, target_lag
            ))
            interactive_count += 1

        else:
            # REGULAR_TABLE or REGULAR_TABLE_WITH_CLUSTERING
            if tinfo.get("clustering_skipped_reason"):
                # Explicitly skipped due to row count — do NOT fall back to partition inference
                cluster_cols = []
            else:
                cluster_cols = [quote_name(c) for c in tinfo.get("cluster_by_columns", [])]
                if not cluster_cols:
                    cluster_cols = infer_clustering(table.get("partitions", []))

            if use_source_views:
                # Source data already in Snowflake — emit a view over source_db.source_schema
                blocks.append(table_ddl_view_over_source(
                    table, schema, source_db, source_schema, measures_map
                ))
            else:
                blocks.append(table_ddl_regular(table, schema, measures_map, cluster_cols))
            regular_count += 1

        blocks.append("")

    # Interactive warehouse block
    if interactive_warehouse and interactive_warehouse.get("tables_to_attach"):
        iw_size   = interactive_warehouse.get("recommended_size", "SMALL")
        iw_tables = ", ".join(
            quote_name(t) for t in interactive_warehouse["tables_to_attach"]
        )
        blocks += [
            "-- === Interactive Warehouse ===",
            f"-- Associates {len(interactive_warehouse['tables_to_attach'])} Interactive Tables",
            f"CREATE OR REPLACE INTERACTIVE WAREHOUSE bi_serving_wh",
            f"  TABLES ({iw_tables})",
            f"  WAREHOUSE_SIZE = '{iw_size}';",
            "",
            "ALTER WAREHOUSE bi_serving_wh RESUME IF SUSPENDED;",
            "",
            "-- Standard warehouse for initial loads and maintenance refreshes",
            "CREATE OR REPLACE WAREHOUSE maintenance_wh",
            "  WAREHOUSE_SIZE = 'SMALL'",
            "  AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;",
            "",
        ]

    # OLS section
    blocks += ols_sql(inventory.get("roles", []), schema)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(blocks))

    print(f"DDL generated: {args.output}")
    print(f"  Regular tables:      {regular_count}")
    print(f"  Interactive tables:  {interactive_count}")
    print(f"  Calculated views:    {view_count}")
    if skipped_count:
        print(f"  Hidden (skipped):    {skipped_count}")
    if interactive_count:
        print(f"\n  NOTE: Interactive Tables require a standard warehouse for CREATE,")
        print(f"        then an Interactive Warehouse for querying. See DDL footer.")


if __name__ == "__main__":
    main()
