#!/usr/bin/env python3
"""
generate_migration_plan.py - Generate MIGRATION_PLAN.md and initial migration_status.md.

Reads ssas_inventory.json + deployment_assessment.json and writes two files:
  MIGRATION_PLAN.md   — full plan for user review and approval (required before Phase 4)
  migration_status.md — living tracker updated each phase by update_migration_status.py

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_migration_plan.py \
        --inventory ./ssas_inventory.json \
        --assessment ./deployment_assessment.json \
        --target-schema MY_DB.MY_SCHEMA \
        [--output ./MIGRATION_PLAN.md] \
        [--status ./migration_status.md]
"""

import argparse
import json
import re
from datetime import date
from pathlib import Path


# ---------------------------------------------------------------------------
# DAX pattern detection helpers
# ---------------------------------------------------------------------------

_LLM_PATTERNS = re.compile(
    r"\b(CALCULATE|CALCULATETABLE|FILTER|RELATED|RELATEDTABLE|ALL\b|ALLEXCEPT|"
    r"ALLSELECTED|TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|"
    r"DATESBETWEEN|PARALLELPERIOD|RANKX|TOPN|CONCATENATEX|SELECTEDVALUE|HASONEVALUE|"
    r"SWITCH|USERELATIONSHIP|CROSSFILTER|SUMMARIZE|ADDCOLUMNS)\s*\(",
    re.IGNORECASE,
)

_TIME_INTEL = re.compile(
    r"\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|"
    r"DATESBETWEEN|PARALLELPERIOD)\s*\(",
    re.IGNORECASE,
)

_DATE_COL_KEYWORDS = re.compile(r"date|year|month|quarter|period|fiscal|day", re.IGNORECASE)

_DATE_COL_PAT = re.compile(r"\[(\w*[Dd]ate\w*|\w*[Yy]ear\w*|\w*[Mm]onth\w*)\]")


def _count_llm_measures(tables: list) -> tuple:
    """Return (pattern_count, llm_count) across all tables."""
    pattern, llm = 0, 0
    for t in tables:
        for m in t.get("measures", []):
            expr = m.get("expression", "")
            if _LLM_PATTERNS.search(expr):
                llm += 1
            else:
                pattern += 1
        for c in t.get("columns", []):
            if c.get("is_calculated") and c.get("expression"):
                if _LLM_PATTERNS.search(c["expression"]):
                    llm += 1
                else:
                    pattern += 1
    return pattern, llm


def _calc_group_expansions(tables: list) -> int:
    """Count N×M calculation group expansions."""
    base_measures = sum(
        len(t.get("measures", []))
        for t in tables
        if not t.get("is_calculated_table") and not t.get("calculation_group")
    )
    expansions = 0
    for t in tables:
        cg = t.get("calculation_group")
        if cg:
            expansions += len(cg.get("items", [])) * base_measures
    return expansions


def _time_intel_funcs_used(all_measures: list) -> list:
    """Collect all time intelligence function names used across all measures."""
    funcs = set()
    for m in all_measures:
        for f in _TIME_INTEL.findall(m.get("expression", "")):
            funcs.add(f.upper())
    return sorted(funcs)


def _cluster_by_reason(col: str, source: str, src_table: dict, all_measures: list) -> str:
    """
    Plain-English reason why this column is a CLUSTER BY candidate,
    tracing back to the equivalent DAX pattern in the source model.
    """
    if source == "partition":
        return (
            f"Source model partitions this table by `{col}` using a date-range expression. "
            "In SSAS, VertiPaq applies the partition filter at load time. "
            f"In Snowflake, `CLUSTER BY ({col})` achieves the same effect at query time "
            "via micro-partition pruning — Snowflake skips blocks whose min/max range "
            "does not intersect the query filter."
        )
    if source == "date_table":
        funcs = _time_intel_funcs_used(all_measures)
        func_list = ", ".join(funcs) if funcs else "time intelligence functions"
        return (
            f"This is the model's Date table. Every DAX time intelligence call "
            f"({func_list}) filters through this table's `{col}` column. "
            "VertiPaq resolves date filters via O(1) column dictionary lookup; "
            f"`CLUSTER BY ({col})` gives Snowflake equivalent micro-partition skip behaviour."
        )
    if source == "hierarchy":
        return (
            f"`{col}` is a hierarchy level column. Users drill down through hierarchy levels, "
            "so every query filters by this column at some point. "
            "VertiPaq uses RLE-compressed column dictionaries for hierarchy traversal; "
            f"`CLUSTER BY ({col})` keeps rows with the same level value in the same micro-partitions."
        )
    if source == "relationship":
        return (
            f"`{col}` is a foreign key used in `RELATED`/`RELATEDTABLE` DAX expressions. "
            "VertiPaq follows pre-built relationship indices on this column during filter propagation. "
            f"`CLUSTER BY ({col})` co-locates rows with the same FK value, "
            "reducing cross-partition scatter in Snowflake join execution."
        )
    if _DATE_COL_KEYWORDS.search(col):
        return (
            f"`{col}` matches a date/time naming pattern and is likely filtered by "
            "date range in most queries. `CLUSTER BY` enables micro-partition pruning."
        )
    return (
        f"Inferred from table structure. `CLUSTER BY ({col})` co-locates related rows "
        "to reduce partition scanning for equality and range filters on this column."
    )


# ---------------------------------------------------------------------------
# MIGRATION_PLAN.md section builders
# ---------------------------------------------------------------------------

def _section_executive_summary(inv: dict, asmnt: dict) -> str:
    s = inv.get("summary", {})
    tables = asmnt.get("tables", [])
    n_interactive = sum(1 for t in tables if t["recommendation"] == "INTERACTIVE_TABLE")
    n_regular_cl  = sum(1 for t in tables if t["recommendation"] == "REGULAR_TABLE_WITH_CLUSTERING")
    n_regular     = sum(1 for t in tables if t["recommendation"] == "REGULAR_TABLE")
    n_view        = sum(1 for t in tables if t["recommendation"] == "CALCULATED_VIEW")
    n_cost_warn   = sum(1 for t in tables if t.get("cost_warning"))
    modes         = ", ".join(s.get("storage_modes", ["import"]))

    rows = [
        ("Model", inv["model_name"]),
        ("Compatibility Level", str(inv.get("compatibility_level", "unknown"))),
        ("Complexity", s.get("complexity", "unknown").upper()),
        ("Tables (total)", str(s.get("table_count", 0))),
        ("→ Interactive Tables", str(n_interactive)),
        ("→ Regular (with clustering)", str(n_regular_cl)),
        ("→ Regular (no clustering)", str(n_regular)),
        ("→ Calculated Views", str(n_view)),
        ("Measures", str(s.get("measure_count", 0))),
        ("Calculated Columns", str(s.get("calculated_column_count", 0))),
        ("Calculation Groups", str(s.get("calculation_group_count", 0))),
        ("Relationships", str(s.get("relationship_count", 0))),
        ("RLS Roles", str(s.get("role_count", 0))),
        ("Has OLS", "Yes" if s.get("has_ols") else "No"),
        ("Storage Modes", modes),
        ("Cost Warnings", f"{n_cost_warn} table(s) flagged (Interactive Warehouse 24h min auto-suspend)"),
    ]
    lines = ["## Executive Summary\n", "| Item | Value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in rows]
    return "\n".join(lines)


def _section_table_plan(asmnt: dict) -> str:
    lines = [
        "## Table Migration Plan\n",
        "| Table | Source Mode | Snowflake Type | CLUSTER BY | TARGET_LAG | Cost Warning |",
        "|---|---|---|---|---|---|",
    ]
    for t in asmnt.get("tables", []):
        cluster = ", ".join(t.get("cluster_by_columns", [])) or "—"
        lag  = t.get("target_lag") or "—"
        warn = "⚠ Yes" if t.get("cost_warning") else "—"
        lines.append(
            f"| {t['name']} | {t.get('storage_mode', 'import')} "
            f"| {t['recommendation']} | {cluster} | {lag} | {warn} |"
        )
    return "\n".join(lines)


def _section_cluster_by_rationale(asmnt: dict, inv: dict) -> str:
    all_measures = [m for t in inv.get("tables", []) for m in t.get("measures", [])]
    table_map    = {t["name"]: t for t in inv.get("tables", [])}
    relationships = inv.get("relationships", [])

    lines = [
        "## CLUSTER BY Rationale — Mapping from Source DAX Query Patterns\n",
        "### Why CLUSTER BY matters\n",
        "In SSAS Tabular, the **VertiPaq** in-memory engine achieves fast response by storing each column",
        "as a compressed dictionary + run-length encoded value vector. When a DAX filter runs",
        "(e.g. `CALCULATE([Revenue], DimDate[Year] = 2024)`), VertiPaq looks up the value in the column",
        "dictionary — effectively **O(1), with no row scanning**.\n",
        "Snowflake's columnar storage achieves equivalent selectivity through **micro-partition pruning**.",
        "Snowflake divides tables into 16 MB micro-partitions and records the min/max value of each column",
        "per partition. When a table is `CLUSTER BY (col)`, rows with similar values are physically",
        "co-located, so Snowflake **skips entire partitions** that cannot satisfy a filter predicate.\n",
        "**The columns that VertiPaq filters via dictionary lookups are the same columns that should be",
        "Snowflake CLUSTER BY keys.** The sections below show which DAX patterns in the source model",
        "drive each column choice.\n",
        "### DAX Pattern → CLUSTER BY Reference\n",
        "| DAX Pattern in Source Model | CLUSTER BY Candidate | Reason |",
        "|---|---|---|",
        "| `TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD` | Date / OrderDate / FiscalDate | Time intelligence filters resolve through the date dimension. VertiPaq: O(1) dictionary lookup → Snowflake: date-range partition pruning |",
        "| `CALCULATE([M], Region = \"West\")` | Region, Territory, Country | Explicit filter predicate on categorical column — same column should be CLUSTER BY for micro-partition skip |",
        "| `FILTER(FactTable, [FK] = value)` | FK column | Row-level filter on fact FK — co-locating by FK reduces cross-partition scatter on joins |",
        "| Hierarchy Year→Quarter→Month→Day | Year (topmost level) | Drill-down queries filter the broadest level first. CLUSTER BY the topmost hierarchy column |",
        "| Date-range partitions in source | Partition date column | Source model already partitions by this column — same column drives Snowflake clustering |",
        "",
        "### Per-Table CLUSTER BY Explanation\n",
    ]

    has_any = False
    for t in asmnt.get("tables", []):
        cols = t.get("cluster_by_columns", [])
        if not cols or t["recommendation"] == "CALCULATED_VIEW":
            continue
        has_any = True
        lines.append(f"#### {t['name']}\n")
        src_table = table_map.get(t["name"], {})

        # Determine source for each inferred column
        part_cols = set()
        for p in src_table.get("partitions", []):
            for expr in [p.get("expression") or "", p.get("query") or ""]:
                part_cols.update(_DATE_COL_PAT.findall(expr))

        date_key_cols = set()
        if src_table.get("is_date_table"):
            date_key_cols = {c["name"] for c in src_table.get("columns", []) if c.get("is_key")}

        hierarchy_cols = {
            lv.get("column")
            for h in src_table.get("hierarchies", [])
            for lv in h.get("levels", [])
            if lv.get("column")
        }
        fk_cols = {r["from_column"] for r in relationships if r["from_table"] == t["name"]}

        for col in cols:
            if col in part_cols:
                src = "partition"
            elif col in date_key_cols:
                src = "date_table"
            elif col in hierarchy_cols:
                src = "hierarchy"
            elif col in fk_cols:
                src = "relationship"
            else:
                src = "inferred"
            reason = _cluster_by_reason(col, src, src_table, all_measures)
            lines.append(f"**`{col}`**: {reason}\n")

    if not has_any:
        lines.append("No tables in this migration have inferred CLUSTER BY columns.")

    return "\n".join(lines)


def _section_cost_warnings(asmnt: dict) -> str:
    warnings = [t for t in asmnt.get("tables", []) if t.get("cost_warning")]
    if not warnings:
        return ""
    lines = [
        "## Cost Warnings — Interactive Table Warehouse Constraints\n",
        "> Interactive Warehouse cannot auto-suspend before **24 hours** — always-on billing",
        "> regardless of query volume. Tables below may not justify this cost.\n",
        "| Table | Score | Warning | Recommended Alternative |",
        "|---|---|---|---|",
    ]
    for t in warnings:
        score = t.get("interactive_score", "—")
        warn_short = (t["cost_warning"] or "").split(".")[0]
        alt = t.get("cost_alternative", "—")
        lines.append(f"| {t['name']} | {score}% | {warn_short} | {alt} |")
    return "\n".join(lines)


def _section_dax_scope(inv: dict) -> str:
    pattern_count, llm_count = _count_llm_measures(inv.get("tables", []))
    expansions = _calc_group_expansions(inv.get("tables", []))
    s = inv.get("summary", {})
    # Rough: ~700 tokens per LLM call (500 input + 200 output); ~200 per expansion
    llm_tokens = llm_count * 700
    expansion_tokens = expansions * 200
    total_tokens = llm_tokens + expansion_tokens

    lines = [
        "## DAX Translation Scope\n",
        "> **Token estimates are ballpark figures only.**",
        "> Based on expression count × average complexity — NOT actual execution.",
        "> Do **NOT** use for cost estimation or invoicing.\n",
        "| Item | Count | Est. Tokens |",
        "|---|---|---|",
        f"| Measures / calc columns — pattern-matched (no LLM) | {pattern_count} | 0 |",
        f"| Measures / calc columns — Cortex LLM required | {llm_count} | ~{llm_tokens:,} |",
        f"| Calculation group expansions (N×M) | {expansions} | ~{expansion_tokens:,} |",
        f"| **Total estimate** | **{pattern_count + llm_count + expansions}** | **~{total_tokens:,}** |",
        "",
        f"Calculation groups: {s.get('calculation_group_count', 0)}  |  KPIs: {s.get('kpi_count', 0)}",
    ]
    return "\n".join(lines)


def _section_security_scope(inv: dict) -> str:
    s = inv.get("summary", {})
    roles = inv.get("roles", [])
    lines = [
        "## Security Migration Scope\n",
        "### Row-Level Security (RLS)\n",
        "| Role | Table | Filter Expression (preview) |",
        "|---|---|---|",
    ]
    has_rls = False
    for role in roles:
        for tp in role.get("table_permissions", []):
            if tp.get("filter_expression"):
                has_rls = True
                preview = (tp["filter_expression"] or "")[:60].replace("|", "\\|")
                lines.append(f"| {role['name']} | {tp['table']} | `{preview}` |")
    if not has_rls:
        lines.append("| — | No RLS defined in this model | — |")

    if s.get("has_ols"):
        lines += [
            "",
            "### Object-Level Security (OLS)\n",
            "| Role | Object | Level |",
            "|---|---|---|",
        ]
        for role in roles:
            for tp in role.get("table_permissions", []):
                if tp.get("metadata_permission") == "none":
                    lines.append(f"| {role['name']} | {tp['table']} | Table (hidden) |")
                for cp in tp.get("column_permissions", []):
                    if cp.get("metadata_permission") == "none":
                        lines.append(
                            f"| {role['name']} | {tp['table']}.{cp['name']} | Column (hidden) |"
                        )
    return "\n".join(lines)


def _section_output_files(target_schema: str) -> str:
    lines = [
        "## Output Files\n",
        "All files below will be generated in your working directory during migration:\n",
        "| File | Generated in Phase | Contents |",
        "|---|---|---|",
        "| `ssas_inventory.json` | Phase 1 | Complete parsed model inventory |",
        "| `deployment_assessment.json` | Phase 2 | Per-table type recommendations and scores |",
        "| `MIGRATION_PLAN.md` | Phase 3 | This file |",
        "| `migration_status.md` | Phase 3 | Living phase-by-phase status tracker |",
        "| `ssas_ddl.sql` | Phase 4 | CREATE TABLE / INTERACTIVE TABLE / VIEW DDL |",
        "| `ssas_ddl_with_views.sql` | Phase 5 | DDL + calculated column views with translated SQL |",
        "| `ssas_measures_translated.json` | Phase 5 | DAX → SQL translation results |",
        "| `ssas_semantic_view.yaml` | Phase 5 | Snowflake Semantic View YAML for Cortex Analyst |",
        "| `ssas_rls_policies.sql` | Phase 6 | Row Access Policy SQL |",
        "",
        f"All Snowflake objects will be created in schema: `{target_schema}`",
    ]
    return "\n".join(lines)


def _section_risks(inv: dict, asmnt: dict) -> str:
    s = inv.get("summary", {})
    risks = []

    if s.get("calculation_group_count", 0) > 0:
        n = s["calculation_group_count"]
        expansions = _calc_group_expansions(inv.get("tables", []))
        risks.append(
            f"**Calculation Groups ({n})**: Will be expanded N×M into individual Snowflake metrics "
            f"(~{expansions} total). Review expanded metric names for naming collisions before Phase 5."
        )

    bidi = [r for r in inv.get("relationships", []) if r.get("cross_filtering") == "bothDirections"]
    if bidi:
        pairs = ", ".join(f"{r['from_table']}→{r['to_table']}" for r in bidi[:3])
        suffix = " and more" if len(bidi) > 3 else ""
        risks.append(
            f"**Bidirectional Cross-Filters ({len(bidi)})**: {pairs}{suffix}. "
            "Bidirectional filters become INNER JOINs in Snowflake SQL. "
            "Verify result sets — INNER JOIN may exclude rows that a unidirectional filter would include."
        )

    dq_tables = [t["name"] for t in inv.get("tables", []) if t.get("storage_mode") == "directQuery"]
    if dq_tables:
        risks.append(
            f"**DirectQuery Tables ({len(dq_tables)})**: {', '.join(dq_tables[:5])}. "
            "These tables query the source live. Data must be loaded or replicated into Snowflake "
            "before the DDL is useful."
        )

    _, llm_count = _count_llm_measures(inv.get("tables", []))
    if llm_count > 0:
        manual_est = max(1, llm_count // 5)
        risks.append(
            f"**DAX Manual Review (~{manual_est} items estimated)**: "
            "Complex expressions go through Cortex LLM. Some may need manual SQL correction. "
            "These will be flagged as `manual_review` in Phase 5 output."
        )

    if s.get("has_ols"):
        risks.append(
            "**Object-Level Security (OLS)**: Column-level OLS requires either a masking policy "
            "(returns NULL) or a role-specific view (omits the column). "
            "Decide approach before Phase 6 — masking is simpler but NULL may confuse reports."
        )

    if not risks:
        risks.append("No significant risks detected for this model.")

    lines = ["## Risks & Items Requiring Attention\n"]
    for i, r in enumerate(risks, 1):
        lines.append(f"{i}. {r}\n")
    return "\n".join(lines)


def _section_approval() -> str:
    return (
        "## Approval\n\n"
        "Review the plan above carefully before proceeding. Pay particular attention to:\n"
        "- The CLUSTER BY column choices and whether they match your actual query patterns\n"
        "- Any cost warnings on Interactive Tables (24h min auto-suspend)\n"
        "- The estimated DAX manual review count\n"
        "- Risks listed above\n\n"
        "**Reply `approved` to begin Phase 4 (Schema DDL generation).**  \n"
        "**Reply `stop` to abort and revise inputs.**\n\n"
        "---\n"
        "_This plan was generated automatically from model.bim structural analysis._  \n"
        "_It reflects the model as-is and does not account for post-migration data loading._"
    )


def build_migration_plan(inv: dict, asmnt: dict, target_schema: str) -> str:
    today = date.today().isoformat()
    s = inv.get("summary", {})
    sections = [
        f"# Migration Plan: {inv['model_name']}\n",
        f"**Generated:** {today}  ",
        f"**Target schema:** `{target_schema}`  ",
        f"**Compatibility level:** {inv.get('compatibility_level', 'unknown')}  ",
        f"**Complexity:** {s.get('complexity', 'unknown').upper()}\n",
        "---\n",
        _section_executive_summary(inv, asmnt),
        "\n---\n",
        _section_table_plan(asmnt),
        "\n---\n",
        _section_cluster_by_rationale(asmnt, inv),
        "\n---\n",
    ]
    cost = _section_cost_warnings(asmnt)
    if cost:
        sections += [cost, "\n---\n"]
    sections += [
        _section_dax_scope(inv),
        "\n---\n",
        _section_security_scope(inv),
        "\n---\n",
        _section_output_files(target_schema),
        "\n---\n",
        _section_risks(inv, asmnt),
        "\n---\n",
        _section_approval(),
    ]
    return "\n".join(sections)


# ---------------------------------------------------------------------------
# migration_status.md builder
# ---------------------------------------------------------------------------

def build_status_file(inv: dict, asmnt: dict, target_schema: str) -> str:
    today = date.today().isoformat()
    s = inv.get("summary", {})
    tables = asmnt.get("tables", [])
    n_interactive = sum(1 for t in tables if t["recommendation"] == "INTERACTIVE_TABLE")
    n_regular = sum(1 for t in tables if t["recommendation"] in
                    ("REGULAR_TABLE_WITH_CLUSTERING", "REGULAR_TABLE"))
    n_view = sum(1 for t in tables if t["recommendation"] == "CALCULATED_VIEW")

    _, llm_count = _count_llm_measures(inv.get("tables", []))
    expansions = _calc_group_expansions(inv.get("tables", []))
    dax_tokens = llm_count * 700 + expansions * 200

    roles_with_rls = sum(
        1 for role in inv.get("roles", [])
        for tp in role.get("table_permissions", [])
        if tp.get("filter_expression")
    )
    security_tokens = roles_with_rls * 500

    lines = [
        f"# Migration Status: {inv['model_name']}",
        "",
        f"**Generated:** {today}  ",
        f"**Target schema:** `{target_schema}`",
        "",
        "> **Token estimates are ballpark figures only.**",
        "> They are NOT actual usage and must NOT be used for cost estimation.",
        "> Estimate assumes ~700 tokens per LLM-translated DAX expression.",
        "",
        "| Phase | Status | Objects Created in Snowflake | Est. Tokens | Notes |",
        "|---|---|---|---|---|",
        f"| Phase 1 — Assess | ✅ Completed | — | 0 | {s.get('table_count', 0)} tables, "
        f"{s.get('measure_count', 0)} measures, complexity: {s.get('complexity', '?').upper()} |",
        f"| Phase 2 — Workload Assessment | ✅ Completed | — | 0 | "
        f"{n_interactive} interactive, {n_regular} regular, {n_view} view(s) |",
        "| Phase 3 — Migration Plan | ✅ Completed | — | 0 | "
        "MIGRATION_PLAN.md written, awaiting approval |",
        "| Phase 4 — Schema DDL | ⏳ Pending | — | 0 | — |",
        f"| Phase 5 — DAX Translation | ⏳ Pending | — | ~{dax_tokens:,} | "
        f"~{llm_count} LLM calls + {expansions} calc group expansions (estimate) |",
        f"| Phase 6 — Security | ⏳ Pending | — | ~{security_tokens:,} | "
        f"~{roles_with_rls} RLS role(s) (estimate) |",
        "| Phase 7 — Validate | ⏳ Pending | — | 0 | — |",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate MIGRATION_PLAN.md and initial migration_status.md"
    )
    parser.add_argument("--inventory",     required=True, help="Path to ssas_inventory.json")
    parser.add_argument("--assessment",    required=True, help="Path to deployment_assessment.json")
    parser.add_argument("--target-schema", required=True, help="Target Snowflake schema (DB.SCHEMA)")
    parser.add_argument("--output", default="./MIGRATION_PLAN.md",
                        help="Output path for MIGRATION_PLAN.md (default: ./MIGRATION_PLAN.md)")
    parser.add_argument("--status", default="./migration_status.md",
                        help="Output path for migration_status.md (default: ./migration_status.md)")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inv = json.load(f)
    with open(args.assessment, "r", encoding="utf-8") as f:
        asmnt = json.load(f)

    plan_path = Path(args.output)
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(build_migration_plan(inv, asmnt, args.target_schema), encoding="utf-8")

    status_path = Path(args.status)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(build_status_file(inv, asmnt, args.target_schema), encoding="utf-8")

    print(f"\nMigration plan   → {plan_path.resolve()}")
    print(f"Status tracker   → {status_path.resolve()}")
    print(f"\n{'=' * 62}")
    print("  APPROVAL REQUIRED BEFORE PROCEEDING TO PHASE 4")
    print(f"{'=' * 62}")
    print(f"  Review {plan_path.name} carefully, then reply:")
    print("    'approved'  → proceed to Phase 4 (Schema DDL)")
    print("    'stop'      → abort migration")
    print(f"{'=' * 62}")


if __name__ == "__main__":
    main()
