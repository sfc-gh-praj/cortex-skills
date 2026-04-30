#!/usr/bin/env python3
"""
generate_powerbi_views.py - Generate Power BI zero-break migration views.

Creates CREATE OR REPLACE VIEW DDL that preserves exact SSAS table names and
column aliases so existing Power BI reports can reconnect without modification.

Key behaviors:
  - View names match SSAS table names exactly (e.g. DimDate, FactInternetSales)
  - Only columns exposed in the SSAS model are included (hidden / language-variant excluded)
  - Snowflake ALL-CAPS columns are aliased back to SSAS mixed-case names
  - Same-table calculated columns become inline SQL expressions
  - Cross-table RELATED() calculated columns become LEFT JOINs
  - Uses sf_column_map from enriched inventory for correct physical column references

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_powerbi_views.py \
        --inventory ./ssas_inventory.json \
        --measures ./ssas_measures_translated.json \
        --source-schema ADVENTUREWORKSDW2022.DBO \
        --target-schema ADVENTUREWORKSDW2022.PBI \
        --output ./powerbi_views.sql
"""

import argparse
import json
import re
import sys
from pathlib import Path


def quote_id(name: str) -> str:
    """Quote an identifier if it contains special characters or is a reserved word."""
    reserved = {
        "DATE", "TIME", "YEAR", "MONTH", "DAY", "NAME", "VALUE", "KEY",
        "INDEX", "ORDER", "GROUP", "SELECT", "FROM", "WHERE", "TABLE",
    }
    if re.search(r"[\s\-\/\(\)]", name) or name.upper() in reserved:
        return f'"{name}"'
    return name


def build_view_for_table(table: dict, source_schema: str, target_schema: str,
                         calc_cols_index: dict, relationships: list[dict]) -> str | None:
    """
    Build CREATE OR REPLACE VIEW DDL for a single table.

    Returns the SQL string, or None if the table should be skipped.
    """
    tname = table["name"]

    # Source table in Snowflake
    src_table = f"{source_schema}.{tname.upper().replace(' ', '_')}"

    # Target view — exact SSAS table name
    view_name = f"{target_schema}.{tname.upper().replace(' ', '_')}"

    # Column mapping from enriched inventory (if available)
    sf_col_map = table.get("sf_column_map", {})
    calc_resolution = table.get("calculated_column_resolution", [])
    calc_res_by_name = {cr["name"]: cr for cr in calc_resolution}

    # Build SELECT columns: only SSAS-exposed (non-hidden) regular columns
    select_parts = []
    for col in table["columns"]:
        if col.get("is_hidden"):
            continue
        ssas_name = col["name"]
        # Resolve Snowflake physical name
        sf_name = sf_col_map.get(ssas_name)
        if sf_name is None and sf_col_map:
            # Column exists in SSAS but not in Snowflake — skip with warning
            select_parts.append(f"    -- WARNING: {ssas_name} not found in Snowflake table")
            continue
        elif sf_name is None:
            # No column map available — use SSAS name uppercased
            sf_name = ssas_name.upper()

        # Check if alias is needed (SSAS name differs from Snowflake name)
        if sf_name.upper() != ssas_name.upper():
            # Different names — alias back to SSAS name
            select_parts.append(f"    {sf_name} AS \"{ssas_name}\"")
        elif sf_name != ssas_name and any(c.islower() for c in ssas_name):
            # Same name but different casing — Power BI may be case-sensitive
            select_parts.append(f"    {sf_name}")
        else:
            select_parts.append(f"    {sf_name}")

    # Determine which calculated columns need LEFT JOINs vs inline
    join_clauses = []
    join_aliases = set()  # track table aliases to avoid duplicates

    for cc in table["calculated_columns"]:
        if cc.get("is_hidden"):
            continue
        cc_name = cc["name"]
        cr = calc_res_by_name.get(cc_name, {})

        if cr.get("strategy") == "left_join":
            # Cross-table RELATED() — needs LEFT JOIN
            rel_table = cr.get("related_table", "")
            rel_column = cr.get("related_column", "")
            join_from = cr.get("join_from_column", "")
            join_to = cr.get("join_to_column", "")

            if rel_table and rel_column and join_from and join_to:
                # Build a unique alias for the joined table
                alias = rel_table.lower()[:3]
                counter = 1
                base_alias = alias
                while alias in join_aliases:
                    alias = f"{base_alias}{counter}"
                    counter += 1
                join_aliases.add(alias)

                join_target = f"{source_schema}.{rel_table.upper().replace(' ', '_')}"
                join_clauses.append(
                    f"LEFT JOIN {join_target} {alias}\n"
                    f"    ON p.{join_from.upper()} = {alias}.{join_to.upper()}"
                )
                select_parts.append(
                    f"    {alias}.{rel_column.upper()} AS \"{cc_name}\""
                )
            else:
                # Incomplete join info — add as NULL placeholder
                select_parts.append(
                    f"    NULL AS \"{cc_name}\"  "
                    f"/* TODO: RELATED('{cr.get('related_table', '?')}'[{cr.get('related_column', '?')}]) */"
                )
        else:
            # Inline SQL — look up translated expression
            item = calc_cols_index.get((tname, cc_name))
            if item and item.get("sql_translation"):
                select_parts.append(
                    f"    {item['sql_translation']} AS \"{cc_name}\""
                )
            else:
                # No translation available — NULL placeholder
                dax = (cc.get("expression") or "")[:100]
                select_parts.append(
                    f"    NULL AS \"{cc_name}\"  /* TODO DAX: {dax} */"
                )

    if not select_parts:
        return None

    # Build the full DDL
    lines = [f"CREATE OR REPLACE VIEW {view_name} AS"]
    lines.append("SELECT")
    lines.append(",\n".join(select_parts))

    if join_clauses:
        lines.append(f"FROM {src_table} p")
        for jc in join_clauses:
            lines.append(jc)
    else:
        lines.append(f"FROM {src_table}")

    return "\n".join(lines) + ";"


def main():
    parser = argparse.ArgumentParser(
        description="Generate Power BI zero-break migration views from SSAS inventory"
    )
    parser.add_argument("--inventory", required=True,
                        help="Path to inventory.json (enriched with --source-db)")
    parser.add_argument("--measures", required=True,
                        help="Path to measures_translated.json")
    parser.add_argument("--source-schema", required=True,
                        help="Snowflake source schema where base tables live, e.g. MY_DB.DBO")
    parser.add_argument("--target-schema", required=True,
                        help="Snowflake target schema for PBI views, e.g. MY_DB.PBI")
    parser.add_argument("--output", required=True,
                        help="Output .sql file path")
    parser.add_argument("--deploy", action="store_true",
                        help="Execute the DDL in Snowflake after generating")
    parser.add_argument("--connection", default="COCO_JK",
                        help="Snowflake connection name for --deploy (default: COCO_JK)")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    with open(args.measures, "r", encoding="utf-8") as f:
        measures_list = json.load(f)

    # Index calculated column translations by (table, name)
    calc_cols_index: dict[tuple[str, str], dict] = {}
    for item in measures_list:
        if item.get("type") == "calculated_column":
            calc_cols_index[(item.get("table", ""), item["name"])] = item

    source_schema = args.source_schema.strip().rstrip("/")
    target_schema = args.target_schema.strip().rstrip("/")

    # Header
    blocks = [
        f"-- Power BI Zero-Break Migration Views",
        f"-- Source: {source_schema} (Snowflake base tables)",
        f"-- Target: {target_schema} (Power BI DirectQuery views)",
        f"-- Generated from SSAS model: {inventory.get('model_name', 'Unknown')}",
        f"--",
        f"-- These views use exact SSAS table names and column aliases so that",
        f"-- existing Power BI reports can reconnect without modification.",
        f"-- Change Power BI data source from SSAS → Snowflake DirectQuery to {target_schema}",
        "",
        f"CREATE SCHEMA IF NOT EXISTS {target_schema};",
        "",
    ]

    view_count = 0
    join_count = 0
    alias_count = 0

    for table in inventory["tables"]:
        if table.get("is_hidden"):
            continue

        ddl = build_view_for_table(
            table, source_schema, target_schema,
            calc_cols_index, inventory.get("relationships", [])
        )
        if ddl:
            blocks.append(f"-- ── {table['name']}")
            blocks.append(ddl)
            blocks.append("")
            view_count += 1
            if "LEFT JOIN" in ddl:
                join_count += 1
            alias_count += ddl.count(' AS "')

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sql_text = "\n".join(blocks)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(sql_text)

    print(f"Generated Power BI views: {args.output}")
    print(f"  Views:          {view_count}")
    print(f"  With JOINs:     {join_count}")
    print(f"  Column aliases: {alias_count}")

    # Deploy if requested
    if args.deploy:
        print(f"\nDeploying to {target_schema} ...")
        try:
            import snowflake.connector
            conn = snowflake.connector.connect(connection_name=args.connection)
            cur = conn.cursor()

            # Execute each statement separately
            stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
            for stmt in stmts:
                if stmt.startswith("--"):
                    continue
                try:
                    cur.execute(stmt + ";")
                except Exception as e:
                    print(f"  WARNING: {e}", file=sys.stderr)

            conn.close()
            print(f"  Deployed {view_count} views to {target_schema}")
        except Exception as e:
            print(f"  ERROR: Deployment failed — {e}", file=sys.stderr)
            print(f"  Deploy manually: snow sql -f {args.output} --connection {args.connection}")
    else:
        print(f"\nDeploy with:")
        print(f"  snow sql -f {args.output} --connection {args.connection}")
        print(f"  Or re-run with --deploy --connection {args.connection}")


if __name__ == "__main__":
    main()
