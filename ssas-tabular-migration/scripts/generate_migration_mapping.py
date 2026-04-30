#!/usr/bin/env python3
"""
generate_migration_mapping.py - Generate a detailed SSAS-to-Snowflake migration mapping report.

Produces MIGRATION_MAPPING.md showing source SSAS Tabular objects vs their
Snowflake equivalents, what was migrated as-is, what required rewriting,
and post-migration steps.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_migration_mapping.py \
        --inventory ./ssas_inventory.json \
        --measures ./ssas_measures_translated.json \
        --target-schema ADVENTUREWORKSDW2022.DBO \
        --pbi-schema ADVENTUREWORKSDW2022.PBI \
        --output ./MIGRATION_MAPPING.md
"""

import argparse
import json
from pathlib import Path


def _esc(s: str) -> str:
    """Escape pipe characters for markdown table cells."""
    return (s or "").replace("|", "\\|").replace("\n", " ")


def section_tables(inventory: dict, target_schema: str) -> list[str]:
    """Section 1: Table mapping."""
    lines = [
        "## 1. Table Mapping",
        "",
        "| SSAS Table | Snowflake Table | Rows | Size (MB) | Status |",
        "|---|---|---:|---:|---|",
    ]
    for t in inventory["tables"]:
        if t.get("is_hidden"):
            continue
        sf_table = f"{target_schema}.{t['name'].upper().replace(' ', '_')}"
        rows = f"{t.get('sf_row_count', '—'):,}" if t.get("sf_row_count") else "—"
        mb = f"{round((t.get('sf_bytes', 0) or 0) / 1_048_576, 2):.2f}" if t.get("sf_bytes") else "—"
        status = "Data as-is" if not t.get("is_calculated_table") else "Calculated (VIEW)"
        lines.append(f"| {t['name']} | `{sf_table}` | {rows} | {mb} | {status} |")
    lines.append("")
    return lines


def section_column_mapping(inventory: dict) -> list[str]:
    """Section 2: Column mapping per table."""
    lines = [
        "## 2. Column Mapping",
        "",
    ]
    for t in inventory["tables"]:
        if t.get("is_hidden"):
            continue
        sf_col_map = t.get("sf_column_map", {})
        sf_missing = t.get("sf_missing_columns", [])
        sf_extra = t.get("sf_extra_columns", [])

        lines.append(f"### {t['name']}")
        lines.append("")

        if sf_col_map:
            has_mismatch = any(
                v and v.upper() != k.upper()
                for k, v in sf_col_map.items()
            )
            if has_mismatch or sf_missing or sf_extra:
                lines.append("| SSAS Column | Snowflake Column | Alias Needed | Notes |")
                lines.append("|---|---|---|---|")
                for ssas_name, sf_name in sf_col_map.items():
                    if sf_name is None:
                        lines.append(f"| {ssas_name} | — | — | **Not found** in Snowflake (renamed in BIM?) |")
                    elif sf_name.upper() != ssas_name.upper():
                        lines.append(f"| {ssas_name} | `{sf_name}` | Yes | Aliased in PBI view |")
                    else:
                        lines.append(f"| {ssas_name} | `{sf_name}` | No | Direct match |")
                lines.append("")

            if sf_extra:
                lines.append(f"**Snowflake-only columns** (not in SSAS model, excluded from PBI views):")
                lines.append("")
                for col in sf_extra:
                    lines.append(f"- `{col}`")
                lines.append("")
        else:
            n_cols = len(t["columns"])
            lines.append(f"{n_cols} columns — no enrichment data (run `parse_bim.py` with `--source-db` for column reconciliation).")
            lines.append("")

    return lines


def section_calculated_columns(inventory: dict, measures: list[dict]) -> list[str]:
    """Section 3: Calculated columns."""
    lines = [
        "## 3. Calculated Columns",
        "",
        "| Table | Column | Original DAX | SQL Translation | Strategy | Target Layer |",
        "|---|---|---|---|---|---|",
    ]
    calc_index = {}
    for item in measures:
        if item.get("type") == "calculated_column":
            calc_index[(item.get("table", ""), item["name"])] = item

    for t in inventory["tables"]:
        if t.get("is_hidden"):
            continue
        calc_res = t.get("calculated_column_resolution", [])
        calc_res_by_name = {cr["name"]: cr for cr in calc_res}

        for cc in t["calculated_columns"]:
            dax = _esc(cc.get("expression", "") or "")[:80]
            item = calc_index.get((t["name"], cc["name"]))
            sql = _esc(item.get("sql_translation", "") or "—") if item else "—"
            sql = sql[:80]
            cr = calc_res_by_name.get(cc["name"], {})
            strategy = cr.get("strategy", "inline_sql")
            if strategy == "left_join":
                strategy_label = f"LEFT JOIN → `{cr.get('related_table', '?')}`"
                layer = "PBI view only"
            elif item and item.get("sql_translation"):
                strategy_label = "Inline SQL"
                layer = "PBI view + Semantic view"
            else:
                strategy_label = "Manual review"
                layer = "—"
            lines.append(
                f"| {t['name']} | {cc['name']} | `{dax}` | `{sql}` | {strategy_label} | {layer} |"
            )

    lines.append("")
    return lines


def section_measures(measures: list[dict]) -> list[str]:
    """Section 4: Measures."""
    lines = [
        "## 4. Measures (DAX → SQL)",
        "",
        "| Table | Measure | Translation Method | SQL Expression | Notes |",
        "|---|---|---|---|---|",
    ]
    for item in measures:
        if item.get("type") != "measure":
            continue
        sql = _esc(item.get("sql_translation", "") or "—")[:80]
        method = item.get("method", "—")
        notes = _esc(item.get("notes", "") or "")[:60]
        lines.append(
            f"| {item.get('table', '?')} | {item['name']} | {method} | `{sql}` | {notes} |"
        )
    lines.append("")

    # Summary counts
    total = sum(1 for m in measures if m.get("type") == "measure")
    pattern_n = sum(1 for m in measures if m.get("type") == "measure" and m.get("method") == "pattern")
    llm_n = sum(1 for m in measures if m.get("type") == "measure" and m.get("method") == "llm")
    manual_n = sum(1 for m in measures if m.get("type") == "measure" and m.get("method") == "manual_review")
    lines.append(f"**Translation summary**: {total} measures — {pattern_n} pattern, {llm_n} LLM, {manual_n} manual review")
    lines.append("")
    return lines


def section_relationships(inventory: dict) -> list[str]:
    """Section 5: Relationships."""
    lines = [
        "## 5. Relationships",
        "",
        "| From | To | Active | Snowflake Handling |",
        "|---|---|---|---|",
    ]
    for r in inventory.get("relationships", []):
        active = "Yes" if r.get("is_active", True) else "No"
        if r.get("is_active", True):
            handling = "Semantic view join + PBI view available"
        else:
            handling = "Power BI model only (inactive — used with USERELATIONSHIP)"
        lines.append(
            f"| `{r['from_table']}.{r['from_column']}` | `{r['to_table']}.{r['to_column']}` "
            f"| {active} | {handling} |"
        )
    lines.append("")
    return lines


def section_hierarchies(inventory: dict) -> list[str]:
    """Section 6: Hierarchies."""
    lines = [
        "## 6. Hierarchies",
        "",
        "| Table | Hierarchy | Levels | Snowflake Handling |",
        "|---|---|---|---|",
    ]
    for t in inventory["tables"]:
        for h in t.get("hierarchies", []):
            levels = sorted(h.get("levels", []), key=lambda l: l.get("ordinal", 0))
            path = " → ".join(l.get("column", l.get("name", "?")) for l in levels)
            handling = "Semantic view metadata (description + synonyms); rebuild in Power BI Model view"
            lines.append(f"| {t['name']} | {h['name']} | {path} | {handling} |")
    lines.append("")
    return lines


def section_manual_review(measures: list[dict]) -> list[str]:
    """Section 7: Items requiring manual attention."""
    manual = [m for m in measures if m.get("method") == "manual_review"]
    lines = [
        "## 7. Items Requiring Manual Review",
        "",
    ]
    if not manual:
        lines.append("No items require manual review.")
        lines.append("")
        return lines

    lines.append("| Type | Table | Name | Original DAX | Notes |")
    lines.append("|---|---|---|---|---|")
    for m in manual:
        dax = _esc(m.get("original_dax", ""))[:80]
        notes = _esc(m.get("notes", ""))[:80]
        lines.append(
            f"| {m.get('type', '?')} | {m.get('table', '?')} | {m['name']} | `{dax}` | {notes} |"
        )
    lines.append("")
    return lines


def section_post_migration(inventory: dict, target_schema: str, pbi_schema: str) -> list[str]:
    """Section 8: Post-migration checklist."""
    model_name = inventory.get("model_name", "Unknown")
    n_hier = sum(len(t.get("hierarchies", [])) for t in inventory["tables"])
    n_rel_active = sum(1 for r in inventory.get("relationships", []) if r.get("is_active", True))
    n_rel_inactive = sum(1 for r in inventory.get("relationships", []) if not r.get("is_active", True))
    n_measures = sum(len(t["measures"]) for t in inventory["tables"])

    lines = [
        "## 8. Post-Migration Checklist",
        "",
        "### Power BI Connection Swap",
        "",
        f"1. Open the `.pbix` file in Power BI Desktop",
        f"2. Change data source: SSAS → Snowflake DirectQuery to `{pbi_schema}`",
        f"3. Verify all {n_rel_active} active relationships in Model view",
    ]
    if n_rel_inactive:
        lines.append(f"4. Verify {n_rel_inactive} inactive relationships (used with USERELATIONSHIP)")
    if n_hier:
        lines.append(f"5. Rebuild {n_hier} hierarchies in Power BI Model view (not auto-migrated)")
    lines += [
        f"6. Remove DAX calculated columns from Power BI model (now pre-computed in views)",
        f"7. Test all {n_measures} measures produce correct results",
        "",
        "### Cortex Analyst Semantic View",
        "",
        f"1. Deploy semantic view: `generate_semantic_view.py --deploy --connection <CONN>`",
        f"2. Verify: `SHOW SEMANTIC VIEWS IN SCHEMA {target_schema}`",
        f"3. Test: `cortex analyst query \"your question\" --view {target_schema}.<VIEW_NAME>`",
        "",
        "### Validation",
        "",
        "1. Run the same analytical queries against both SSAS (before decommission) and Snowflake",
        "2. Focus on time-intelligence measures — these have the most complex rewrites",
        "3. Spot-check calculated columns in PBI views match SSAS values",
        "",
        "### Ongoing",
        "",
        "- **Data refresh**: PBI views read from base tables — no processing step needed",
        "- **Performance**: Monitor DirectQuery latency via Snowflake query history",
        "- **SSAS decommission**: After all reports are validated, decommission the SSAS instance",
        "",
    ]
    return lines


def main():
    parser = argparse.ArgumentParser(
        description="Generate SSAS-to-Snowflake migration mapping report"
    )
    parser.add_argument("--inventory", required=True, help="Path to inventory.json")
    parser.add_argument("--measures", required=True, help="Path to measures_translated.json")
    parser.add_argument("--target-schema", required=True,
                        help="Snowflake target schema for base tables, e.g. MY_DB.DBO")
    parser.add_argument("--pbi-schema", default=None,
                        help="Snowflake schema for PBI views, e.g. MY_DB.PBI (default: target-schema)")
    parser.add_argument("--output", required=True, help="Output .md file path")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    with open(args.measures, "r", encoding="utf-8") as f:
        measures = json.load(f)

    target_schema = args.target_schema.strip()
    pbi_schema = (args.pbi_schema or target_schema).strip()
    model_name = inventory.get("model_name", "Unknown")
    compat = inventory.get("compatibility_level", "?")

    # Build the report
    report = []

    # Header
    report += [
        f"# SSAS → Snowflake Migration Mapping: {model_name}",
        "",
        f"- **Model**: {model_name}",
        f"- **Compatibility Level**: {compat}",
        f"- **Tables**: {len([t for t in inventory['tables'] if not t.get('is_hidden')])}",
        f"- **Measures**: {sum(len(t['measures']) for t in inventory['tables'])}",
        f"- **Calculated Columns**: {sum(len(t['calculated_columns']) for t in inventory['tables'])}",
        f"- **Relationships**: {len(inventory.get('relationships', []))}",
        f"- **Hierarchies**: {sum(len(t.get('hierarchies', [])) for t in inventory['tables'])}",
        f"- **Target Schema (base)**: `{target_schema}`",
        f"- **Target Schema (PBI views)**: `{pbi_schema}`",
        "",
        "---",
        "",
    ]

    # Sections
    report += section_tables(inventory, target_schema)
    report += section_column_mapping(inventory)
    report += section_calculated_columns(inventory, measures)
    report += section_measures(measures)
    report += section_relationships(inventory)
    report += section_hierarchies(inventory)
    report += section_manual_review(measures)
    report += section_post_migration(inventory, target_schema, pbi_schema)

    # Artifacts
    report += [
        "## 9. Artifacts Produced",
        "",
        "| File | Description |",
        "|---|---|",
        "| `ssas_inventory.json` | Full parsed model inventory with column reconciliation |",
        "| `ssas_measures_translated.json` | DAX → SQL translation results |",
        "| `ssas_semantic_view.yaml` | Cortex Analyst semantic view definition |",
        "| `powerbi_views.sql` | Power BI zero-break migration views |",
        "| `MIGRATION_MAPPING.md` | This report |",
        "",
    ]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    n_tables = len([t for t in inventory["tables"] if not t.get("is_hidden")])
    n_measures = sum(1 for m in measures if m.get("type") == "measure")
    n_calc = sum(1 for m in measures if m.get("type") == "calculated_column")
    n_manual = sum(1 for m in measures if m.get("method") == "manual_review")
    print(f"Migration mapping report: {args.output}")
    print(f"  Tables:     {n_tables}")
    print(f"  Measures:   {n_measures}")
    print(f"  Calc cols:  {n_calc}")
    if n_manual:
        print(f"  Manual review items: {n_manual}")


if __name__ == "__main__":
    main()
