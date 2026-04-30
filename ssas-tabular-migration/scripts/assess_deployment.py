#!/usr/bin/env python3
"""
assess_deployment.py - Workload assessment for SSAS Tabular → Snowflake migration.

Combines BIM structural signals with 4 user questions to produce a per-table
recommendation: INTERACTIVE_TABLE | REGULAR_TABLE_WITH_CLUSTERING | REGULAR_TABLE

Outputs assessment.json consumed by generate_ddl.py --assessment flag.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/assess_deployment.py \
        --inventory inventory.json \
        --output deployment_assessment.json
"""

import argparse
import json
import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Workload questionnaire
# ---------------------------------------------------------------------------

QUESTIONS = [
    {
        "key": "concurrent_users",
        "text": "1. Concurrent users hitting this model at peak:",
        "options": {
            "a": "Fewer than 10",
            "b": "10 – 100",
            "c": "100 – 1,000",
            "d": "More than 1,000",
        },
    },
    {
        "key": "query_pattern",
        "text": "2. Typical query pattern:",
        "options": {
            "a": "Most queries filter by date range, region, or category  (selective)",
            "b": "Many queries aggregate ALL company data without filters  (full scans)",
            "c": "Mix of both",
        },
    },
    {
        "key": "latency_sla",
        "text": "3. Required dashboard response time:",
        "options": {
            "a": "Sub-second  (< 1 s)",
            "b": "1 – 3 seconds",
            "c": "3+ seconds acceptable",
        },
    },
    {
        "key": "refresh_cadence",
        "text": "4. Data refresh cadence:",
        "options": {
            "a": "Real-time / near-real-time",
            "b": "Hourly",
            "c": "Daily",
            "d": "Weekly / ad-hoc",
        },
    },
]

TARGET_LAG_MAP = {
    "a": "60 seconds",   # real-time (minimum allowed)
    "b": "1 hour",
    "c": "1 day",
    "d": "7 days",
}

# ---------------------------------------------------------------------------
# Snowflake feature recommendation thresholds
# ---------------------------------------------------------------------------

# Minimum rows before CLUSTER BY has measurable impact on query performance.
# Below these thresholds tables typically fit in 1–2 micro-partitions; clustering
# adds reclustering cost with no pruning benefit.
CLUSTER_FACT_MIN_ROWS = 10_000_000   # 10M rows for wide fact tables
CLUSTER_DIM_MIN_ROWS  =  1_000_000   # 1M rows for narrow dimension tables
SOS_MIN_ROWS          =    100_000   # 100K rows minimum for SOS to be cost-effective
AUTO_CLUSTER_MIN_ROWS = 50_000_000   # 50M rows before automatic clustering is worthwhile


def ask_questionnaire() -> dict:
    print("\n" + "=" * 60)
    print("  Workload Assessment")
    print("  (Determines Interactive vs Regular Table recommendation)")
    print("=" * 60 + "\n")

    answers = {}
    for q in QUESTIONS:
        print(q["text"])
        for key, label in q["options"].items():
            print(f"   {key}) {label}")
        while True:
            val = input("   Enter choice: ").strip().lower()
            if val in q["options"]:
                answers[q["key"]] = val
                break
            print(f"   Please enter one of: {', '.join(q['options'].keys())}")
        print()
    return answers


# ---------------------------------------------------------------------------
# BIM signal analysis helpers
# ---------------------------------------------------------------------------

def _has_date_partitions(table: dict) -> bool:
    """Return True if any partition expression mentions a date column or date range."""
    date_pat = re.compile(r"date|year|month|period|fiscal", re.I)
    for p in table.get("partitions", []):
        if date_pat.search(p.get("expression") or "") or \
           date_pat.search(p.get("query") or ""):
            return True
    return False


def _is_dimension(table: dict, relationships: list[dict]) -> bool:
    """
    A table is a dimension if it only appears on the 'to' (one) side of relationships
    and never on the 'from' (many) side.
    """
    name = table["name"]
    on_from = any(r["from_table"] == name for r in relationships)
    on_to   = any(r["to_table"]   == name for r in relationships)
    return on_to and not on_from


def _has_bidirectional_filter(table: dict, relationships: list[dict]) -> bool:
    name = table["name"]
    return any(
        (r["from_table"] == name or r["to_table"] == name)
        and r.get("cross_filtering") == "bothDirections"
        for r in relationships
    )


def _infer_cluster_columns(table: dict, relationships: list[dict]) -> list[str]:
    """
    Return candidate CLUSTER BY columns in priority order:
    1. Date partition columns
    2. is_date_table → the date column (unique key column)
    3. Hierarchy level columns
    4. Relationship join columns used as foreign key on this table
    """
    candidates = []

    # 1. Columns referenced in partition expressions
    date_col_pat = re.compile(r"\[(\w*[Dd]ate\w*|\w*[Yy]ear\w*|\w*[Mm]onth\w*)\]")
    for p in table.get("partitions", []):
        for expr in [p.get("expression") or "", p.get("query") or ""]:
            for col in date_col_pat.findall(expr):
                if col not in candidates:
                    candidates.append(col)

    # 2. Date table: the key column is the primary cluster candidate
    if table.get("is_date_table"):
        key_cols = [c["name"] for c in table.get("columns", []) if c.get("is_key")]
        for c in key_cols:
            if c not in candidates:
                candidates.append(c)

    # 3. Hierarchy level columns
    for h in table.get("hierarchies", []):
        for lv in h.get("levels", []):
            col = lv.get("column")
            if col and col not in candidates:
                candidates.append(col)

    # 4. Foreign key columns (from_column on this table's relationships)
    for r in relationships:
        if r["from_table"] == table["name"]:
            col = r["from_column"]
            if col and col not in candidates:
                candidates.append(col)

    return candidates[:3]  # max 3 clustering columns


# ---------------------------------------------------------------------------
# Snowflake feature recommendations
# ---------------------------------------------------------------------------

def _recommend_snowflake_features(
    table: dict,
    relationships: list,
    answers: dict,
    row_count: "int | None",
) -> "tuple[list[dict], bool]":
    """
    Return (features_list, clustering_viable).

    features_list: recommended Snowflake performance features with rationale + SQL hint.
    clustering_viable: False when row_count is too small for CLUSTER BY to have any effect.
    """
    features = []
    is_dim = _is_dimension(table, relationships)
    cluster_threshold = CLUSTER_DIM_MIN_ROWS if is_dim else CLUSTER_FACT_MIN_ROWS

    # ── Clustering viability check ────────────────────────────────────────────
    if row_count is not None and row_count < cluster_threshold:
        clustering_viable = False
        features.append({
            "feature": "RESULT CACHE",
            "priority": "HIGH",
            "reason": (
                f"Table has {row_count:,} rows — fits in 1–2 micro-partitions. "
                "CLUSTER BY adds reclustering cost with no pruning benefit at this size. "
                "Snowflake result cache (free, 24h TTL) handles repeated BI queries."
            ),
            "action": "No action needed — result cache is always on.",
        })
        return features, False

    clustering_viable = True

    # ── Search Optimization Service (SOS) ────────────────────────────────────
    # Best for: equality / point-lookup predicates on high-cardinality columns.
    # Cost: fixed monthly per-table fee + credits to build the access path.
    if row_count is None or row_count >= SOS_MIN_ROWS:
        if answers.get("query_pattern") == "a":  # selective filters
            key_cols = [c["name"] for c in table.get("columns", []) if c.get("is_key")]
            fk_cols  = [r["from_column"] for r in relationships
                        if r["from_table"] == table["name"]]
            lookup_cols = (key_cols + fk_cols)[:3]
            if lookup_cols:
                col_list = ", ".join(lookup_cols)
                features.append({
                    "feature": "SEARCH OPTIMIZATION",
                    "priority": "MEDIUM",
                    "reason": (
                        f"Selective equality/IN/range filters on high-cardinality columns "
                        f"({col_list}). SOS builds a per-column access path for O(1) "
                        "point lookups — ideal for customer/order ID drill-throughs."
                    ),
                    "action": (
                        f"ALTER TABLE <schema>.{table['name']} "
                        f"ADD SEARCH OPTIMIZATION ON EQUALITY({col_list});"
                    ),
                })

    # ── Query Acceleration Service (QAS) ────────────────────────────────────
    # Best for: ad-hoc full/mixed scans with variable query complexity.
    # Offloads eligible scan partials to serverless nodes; per-credit cost.
    if answers.get("query_pattern") in ("b", "c"):
        features.append({
            "feature": "QUERY ACCELERATION SERVICE",
            "priority": "MEDIUM",
            "reason": (
                "Full-table or mixed scan pattern detected. QAS offloads eligible "
                "scan partials to serverless compute, reducing warehouse queue latency "
                "for ad-hoc / variable-complexity queries (e.g. Excel Analyze-in-Excel)."
            ),
            "action": (
                "ALTER WAREHOUSE <wh> SET ENABLE_QUERY_ACCELERATION = TRUE "
                "QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;"
            ),
        })

    # ── Result Cache ──────────────────────────────────────────────────────────
    # Always-on, free, 24h TTL. BI tools (Power BI, Tableau) benefit most.
    features.append({
        "feature": "RESULT CACHE",
        "priority": "HIGH",
        "reason": (
            "Identical queries return instantly (free, 24h TTL). Power BI and Tableau "
            "re-execute the same SQL frequently — result cache provides sub-millisecond "
            "response for these repeat hits."
        ),
        "action": "No action needed — result cache is always on.",
    })

    # ── Materialized Views ────────────────────────────────────────────────────
    # Best for: large facts with repeated GROUP BY aggregation patterns.
    if not is_dim and (row_count is None or row_count > 1_000_000):
        features.append({
            "feature": "MATERIALIZED VIEW",
            "priority": "LOW",
            "reason": (
                "Pre-compute common GROUP BY aggregations (e.g. SUM sales by month/region) "
                "to serve repeated dashboard queries from a pre-aggregated result set. "
                "Snowflake auto-maintains the MV as base data changes."
            ),
            "action": (
                f"CREATE MATERIALIZED VIEW <schema>.{table['name']}_AGG AS "
                f"SELECT <dim_cols>, SUM(<metric>) FROM <schema>.{table['name']} GROUP BY <dim_cols>;"
            ),
        })

    # ── Automatic Clustering (large INSERT-disordered tables) ─────────────────
    # Async background service keeps CLUSTER BY keys aligned as data grows.
    if row_count is not None and row_count >= AUTO_CLUSTER_MIN_ROWS:
        features.append({
            "feature": "AUTOMATIC CLUSTERING",
            "priority": "HIGH",
            "reason": (
                f"Table has {row_count:,} rows. Automatic clustering maintains CLUSTER BY "
                "alignment as new data is appended, preventing micro-partition overlap "
                "drift and preserving pruning efficiency over time."
            ),
            "action": (
                f"ALTER TABLE <schema>.{table['name']} CLUSTER BY (<cols>); "
                "-- Snowflake reclusters automatically in the background"
            ),
        })

    return features, clustering_viable


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

USER_SCORE_MAP = {
    "concurrent_users": {"a": 0, "b": 10, "c": 30, "d": 50},
    "query_pattern":    {"a": 25, "b": -20, "c": 5},
    "latency_sla":      {"a": 20, "b": 5,  "c": 0},
    "refresh_cadence":  {"a": 10, "b": 8,  "c": 3, "d": 0},
}


def score_table(table: dict, relationships: list, answers: dict,
                row_count: "int | None" = None) -> dict:
    score = 0
    reasoning = []
    cost_warning = None
    cost_alternative = None

    # BIM signals
    if table.get("storage_mode") == "directQuery":
        score += 40
        reasoning.append("DirectQuery mode: live low-latency queries expected (+40)")

    if table.get("is_date_table"):
        score += 15
        reasoning.append("Marked as Date Table: always used as filter (+15)")

    if _has_date_partitions(table):
        score += 20
        reasoning.append("Date-range partitions: date column is primary filter (+20)")

    if _is_dimension(table, relationships):
        score += 15
        reasoning.append("Dimension table: always used as filter/slicer (+15)")

    if table.get("hierarchies"):
        score += 10
        reasoning.append(f"{len(table['hierarchies'])} hierarchy(ies): drill-down filter columns (+10)")

    if _has_bidirectional_filter(table, relationships):
        score -= 15
        reasoning.append("Bidirectional cross-filter: complex join pattern (−15)")

    is_large_fact = (
        len(table.get("columns", [])) > 15
        and len(table.get("partitions", [])) <= 1
        and not _is_dimension(table, relationships)
    )
    if is_large_fact:
        score -= 10
        reasoning.append("Large fact table, no partitions: likely full-table scans (−10)")

    # User answers
    for key, val_map in USER_SCORE_MAP.items():
        answer = answers.get(key, "c")
        delta  = val_map.get(answer, 0)
        if delta != 0:
            label = QUESTIONS[[q["key"] for q in QUESTIONS].index(key)]["options"][answer]
            sign  = "+" if delta > 0 else "−"
            reasoning.append(f"User: {label.strip()} ({sign}{abs(delta)})")
        score += delta

    score = max(0, min(100, score))

    if score >= 70:
        recommendation = "INTERACTIVE_TABLE"
    elif score >= 40:
        recommendation = "REGULAR_TABLE_WITH_CLUSTERING"
    else:
        recommendation = "REGULAR_TABLE"

    # Calculated tables are always VIEWs regardless of score
    if table.get("is_calculated_table"):
        recommendation = "CALCULATED_VIEW"
        reasoning.insert(0, "Calculated table: always emitted as VIEW")

    cluster_cols = _infer_cluster_columns(table, relationships)
    target_lag   = TARGET_LAG_MAP.get(answers.get("refresh_cadence", "c"), "1 day")

    # Snowflake feature recommendations + clustering viability check
    sf_features, clustering_viable = _recommend_snowflake_features(
        table, relationships, answers, row_count
    )

    # Override clustering/interactive recommendation when row count is below threshold.
    # Tables this small fit in 1–2 micro-partitions — neither CLUSTER BY nor Interactive
    # Tables provide measurable benefit; result cache handles repeated BI queries for free.
    clustering_skipped_reason = None
    if not clustering_viable and recommendation in ("REGULAR_TABLE_WITH_CLUSTERING", "INTERACTIVE_TABLE"):
        is_dim = _is_dimension(table, relationships)
        threshold = CLUSTER_DIM_MIN_ROWS if is_dim else CLUSTER_FACT_MIN_ROWS
        recommendation = "REGULAR_TABLE"
        cluster_cols = []
        cost_warning = None
        cost_alternative = None
        clustering_skipped_reason = (
            f"Row count ({row_count:,}) is below the clustering threshold "
            f"({threshold:,} for {'dimension' if is_dim else 'fact'} tables). "
            "CLUSTER BY and Interactive Table omitted — result cache provides sufficient performance at this size."
        )
        reasoning.append(f"Clustering/Interactive skipped: {row_count:,} rows < {threshold:,} threshold")

    return {
        "name":                      table["name"],
        "storage_mode":              table.get("storage_mode", "import"),
        "row_count":                 row_count,
        "interactive_score":         score,
        "recommendation":            recommendation,
        "cluster_by_columns":        cluster_cols,
        "clustering_skipped_reason": clustering_skipped_reason,
        "target_lag":                target_lag if recommendation == "INTERACTIVE_TABLE" else None,
        "reasoning":                 reasoning,
        "cost_warning":              cost_warning,
        "cost_alternative":          cost_alternative,
        "snowflake_features":        sf_features,
    }


# ---------------------------------------------------------------------------
# Interactive warehouse sizing
# ---------------------------------------------------------------------------

def recommend_warehouse_size(answers: dict, interactive_tables: list) -> str:
    n = len(interactive_tables)
    users = answers.get("concurrent_users", "b")
    if users == "d" or n > 20:
        return "MEDIUM"
    if users == "c" or n > 5:
        return "SMALL"
    return "XSMALL"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Workload assessment: score tables for Interactive vs Regular Table"
    )
    parser.add_argument("--inventory", required=True, help="Path to inventory.json")
    parser.add_argument("--output",    required=True, help="Output assessment.json path")
    parser.add_argument("--answers",   default=None,
                        help="Pre-supplied answers as JSON string, e.g. "
                             "'{\"concurrent_users\":\"c\",\"query_pattern\":\"a\","
                             "\"latency_sla\":\"a\",\"refresh_cadence\":\"b\"}' "
                             "(skips interactive questionnaire)")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    # Collect answers
    if args.answers:
        answers = json.loads(args.answers)
        print("Using pre-supplied workload answers.")
    else:
        answers = ask_questionnaire()

    relationships = inventory.get("relationships", [])
    table_results = []

    for table in inventory["tables"]:
        row_count = table.get("sf_row_count")  # None if --source-db not used in Phase 1
        result = score_table(table, relationships, answers, row_count=row_count)
        table_results.append(result)

    interactive_tables = [t["name"] for t in table_results
                          if t["recommendation"] == "INTERACTIVE_TABLE"]
    wh_size = recommend_warehouse_size(answers, interactive_tables)

    # Collect all cost warnings across tables (deduplicated by message)
    cost_warnings = []
    seen_warnings = set()
    for t in table_results:
        if t.get("cost_warning") and t["cost_warning"] not in seen_warnings:
            cost_warnings.append({
                "table":       t["name"],
                "warning":     t["cost_warning"],
                "alternative": t.get("cost_alternative"),
            })
            seen_warnings.add(t["cost_warning"])

    assessment = {
        "model_name":   inventory["model_name"],
        "user_answers": answers,
        "tables":       table_results,
        "interactive_warehouse": {
            "recommended_size":  wh_size,
            "tables_to_attach":  interactive_tables,
        },
        "target_lag_default": TARGET_LAG_MAP.get(answers.get("refresh_cadence", "c"), "1 day"),
        "cost_warnings": cost_warnings,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(assessment, f, indent=2)

    # Print recommendation table
    print(f"\nAssessment complete for: {inventory['model_name']}")
    print(f"\n{'Table':<30} {'Rows':>10}  {'Score':>6}  {'Recommendation':<32}  {'Cluster By'}")
    print("-" * 100)
    for t in table_results:
        cluster  = ", ".join(t["cluster_by_columns"]) or ("— (skipped: too small)" if t.get("clustering_skipped_reason") else "—")
        flag     = " ⚠" if t.get("cost_warning") else ""
        rows_str = f"{t['row_count']:,}" if t.get("row_count") is not None else "unknown"
        print(f"{t['name']:<30} {rows_str:>10}  {t['interactive_score']:>5}%  {t['recommendation']:<32}  {cluster}{flag}")

    print(f"\nInteractive warehouse: {wh_size}  ({len(interactive_tables)} tables attached)")

    # Print clustering skip notes
    skipped = [t for t in table_results if t.get("clustering_skipped_reason")]
    if skipped:
        print("\n" + "=" * 70)
        print("  CLUSTERING SKIPPED (tables too small)")
        print("=" * 70)
        for t in skipped:
            print(f"  {t['name']}: {t['clustering_skipped_reason']}")
        print("=" * 70)

    # Print Snowflake feature recommendations
    print("\n" + "=" * 70)
    print("  SNOWFLAKE PERFORMANCE FEATURE RECOMMENDATIONS")
    print("=" * 70)
    all_features: dict[str, list[str]] = {}
    for t in table_results:
        for f in t.get("snowflake_features", []):
            feat = f["feature"]
            if feat not in all_features:
                all_features[feat] = []
            all_features[feat].append(t["name"])
    if all_features:
        for feat, tables in all_features.items():
            print(f"\n  [{feat}]  →  {', '.join(tables)}")
            # Print reason from first table mentioning this feature
            for t in table_results:
                for f in t.get("snowflake_features", []):
                    if f["feature"] == feat:
                        print(f"  Reason : {f['reason']}")
                        if f.get("action") and f["action"] != "No action needed — result cache is always on.":
                            print(f"  Action : {f['action']}")
                        break
                else:
                    continue
                break
    print("=" * 70)

    # Print cost warnings section
    if cost_warnings:
        print("\n" + "=" * 70)
        print("  INTERACTIVE TABLE COST WARNINGS")
        print("=" * 70)
        print("  Interactive Warehouse cannot auto-suspend before 24 hours.")
        print("  This means always-on billing regardless of query volume.\n")
        warned_tables = [t["name"] for t in table_results if t.get("cost_warning")]
        for t in table_results:
            if not t.get("cost_warning"):
                continue
            print(f"  Table: {t['name']}")
            # Print the first sentence of the warning only (concise)
            print(f"  {t['cost_warning']}")
            if t.get("cost_alternative"):
                print(f"  Alternative: {t['cost_alternative']}")
            print()
        print("  Tables flagged (⚠): " + ", ".join(warned_tables))
        print("  Tip: REGULAR_TABLE_WITH_CLUSTERING auto-suspends in 1–5 minutes")
        print("       and Snowflake result cache handles repeated identical queries for free.")
        print("=" * 70)

    print(f"\nOutput: {args.output}")
    print("\nNext: pass --assessment to generate_ddl.py to emit the correct table types.")


if __name__ == "__main__":
    main()
