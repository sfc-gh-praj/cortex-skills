#!/usr/bin/env python3
"""
convert_dax.py - Translate DAX measures, calculated columns, and calculation group items
to Snowflake SQL.

Strategy:
  1. Pattern matching  — regex rules for ~25 simple DAX functions.
  2. LLM fallback      — SNOWFLAKE.CORTEX.COMPLETE for complex expressions.
  3. Manual review     — flag anything unresolvable.
  4. Calc group expand — N calculation items × M base measures → individual metrics.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/convert_dax.py \
        --inventory inventory.json \
        --output measures_translated.json \
        --connection COCO_JK
"""

import argparse
import json
import re
import sys
from pathlib import Path

import snowflake.connector


# ---------------------------------------------------------------------------
# LLM output cleaning
# ---------------------------------------------------------------------------

def _clean_llm_sql(raw: str) -> str:
    """
    Strip markdown code fences and LLM preamble from a Cortex COMPLETE response.

    SNOWFLAKE.CORTEX.COMPLETE sometimes wraps SQL in ```sql...``` fences or
    prefixes the response with "Wait, let me reconsider…" / "Actually…" text.
    This function extracts the last clean code block, or returns the raw text
    trimmed if no fences are present.
    """
    blocks = re.findall(r"```(?:sql)?\s*\n(.*?)```", raw, re.DOTALL)
    if blocks:
        clean = [
            b.strip() for b in blocks
            if b.strip() and not re.match(
                r"^(wait|let me|actually|hmm|here|note:|sorry)", b.strip(), re.I
            )
        ]
        return clean[-1] if clean else blocks[-1].strip()
    return raw.strip()


# ---------------------------------------------------------------------------
# Pattern-based translations (first match wins)
# ---------------------------------------------------------------------------

PATTERNS = [
    # Aggregations
    (re.compile(r"^\s*SUM\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"SUM({m.group(1)})"),
    (re.compile(r"^\s*COUNT\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"COUNT({m.group(1)})"),
    (re.compile(r"^\s*COUNTA\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"COUNT({m.group(1)})"),
    (re.compile(r"^\s*DISTINCTCOUNT\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"COUNT(DISTINCT {m.group(1)})"),
    (re.compile(r"^\s*AVERAGE\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"AVG({m.group(1)})"),
    (re.compile(r"^\s*MAX\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"MAX({m.group(1)})"),
    (re.compile(r"^\s*MIN\s*\(\s*\w+\[(\w+)\]\s*\)\s*$", re.I),
     lambda m: f"MIN({m.group(1)})"),
    (re.compile(r"^\s*COUNTROWS\s*\(\s*\w+\s*\)\s*$", re.I),
     lambda _: "COUNT(*)"),

    # Calculation group identity item
    (re.compile(r"^\s*SELECTEDMEASURE\s*\(\s*\)\s*$", re.I),
     lambda _: "__SELECTEDMEASURE__"),   # placeholder resolved during expansion

    # DIVIDE
    (re.compile(r"^\s*DIVIDE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)\s*$", re.I | re.DOTALL),
     lambda m: f"IFF(({m.group(2)}) = 0, NULL, ({m.group(1)}) / ({m.group(2)}))"),
    (re.compile(r"^\s*DIVIDE\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)\s*$", re.I | re.DOTALL),
     lambda m: f"IFF(({m.group(2)}) = 0, ({m.group(3)}), ({m.group(1)}) / ({m.group(2)}))"),

    # Literals
    (re.compile(r"^\s*BLANK\s*\(\s*\)\s*$", re.I),  lambda _: "NULL"),
    (re.compile(r"^\s*TRUE\s*\(\s*\)\s*$",  re.I),  lambda _: "TRUE"),
    (re.compile(r"^\s*FALSE\s*\(\s*\)\s*$", re.I),  lambda _: "FALSE"),

    # Math
    (re.compile(r"^\s*ABS\s*\(\s*(.+)\s*\)\s*$",            re.I), lambda m: f"ABS({m.group(1)})"),
    (re.compile(r"^\s*ROUND\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)\s*$", re.I),
     lambda m: f"ROUND({m.group(1)}, {m.group(2)})"),
    (re.compile(r"^\s*SQRT\s*\(\s*(.+)\s*\)\s*$",  re.I), lambda m: f"SQRT({m.group(1)})"),
    (re.compile(r"^\s*INT\s*\(\s*(.+)\s*\)\s*$",   re.I), lambda m: f"FLOOR({m.group(1)})"),

    # Text
    (re.compile(r"^\s*LEN\s*\(\s*(.+)\s*\)\s*$",   re.I), lambda m: f"LENGTH({m.group(1)})"),
    (re.compile(r"^\s*UPPER\s*\(\s*(.+)\s*\)\s*$", re.I), lambda m: f"UPPER({m.group(1)})"),
    (re.compile(r"^\s*LOWER\s*\(\s*(.+)\s*\)\s*$", re.I), lambda m: f"LOWER({m.group(1)})"),
    (re.compile(r"^\s*TRIM\s*\(\s*(.+)\s*\)\s*$",  re.I), lambda m: f"TRIM({m.group(1)})"),

    # Date parts
    (re.compile(r"^\s*YEAR\s*\(\s*(.+)\s*\)\s*$",  re.I), lambda m: f"YEAR({m.group(1)})"),
    (re.compile(r"^\s*MONTH\s*\(\s*(.+)\s*\)\s*$", re.I), lambda m: f"MONTH({m.group(1)})"),
    (re.compile(r"^\s*DAY\s*\(\s*(.+)\s*\)\s*$",   re.I), lambda m: f"DAY({m.group(1)})"),
    (re.compile(r"^\s*TODAY\s*\(\s*\)\s*$",         re.I), lambda _: "CURRENT_DATE()"),
    (re.compile(r"^\s*NOW\s*\(\s*\)\s*$",           re.I), lambda _: "CURRENT_TIMESTAMP()"),
]

COMPLEX_KEYWORDS = [
    "CALCULATE", "FILTER", "RELATED", "RELATEDTABLE", "ALL(", "ALLEXCEPT",
    "ALLSELECTED", "TOTALYTD", "TOTALQTD", "TOTALMTD", "SAMEPERIODLASTYEAR",
    "PREVIOUSMONTH", "PREVIOUSQUARTER", "PREVIOUSYEAR", "DATEADD", "DATESMTD",
    "DATESQTD", "DATESYTD", "DATESINPERIOD", "RANKX", "TOPN", "CONCATENATEX",
    "MAXX", "MINX", "SUMX", "AVERAGEX", "COUNTX", "SELECTEDVALUE", "HASONEVALUE",
    "VALUES(", "USERELATIONSHIP", "CROSSFILTER", "KEEPFILTERS", "PARALLELPERIOD",
    "SELECTEDMEASURENAME", "ISSELECTEDMEASURE",
]


def try_pattern_match(expression: str) -> str | None:
    for pattern, transformer in PATTERNS:
        m = pattern.match(expression)
        if m:
            return transformer(m)
    return None


def is_complex(expression: str) -> bool:
    upper = expression.upper()
    return any(kw in upper for kw in COMPLEX_KEYWORDS)


# ---------------------------------------------------------------------------
# LLM fallback via Cortex COMPLETE
# ---------------------------------------------------------------------------

def build_llm_prompt(name: str, expression: str, context: dict,
                     extra_context: str = "") -> str:
    table_list = "\n".join(
        f"  - {t['name']}: {', '.join(c['name'] for c in t['columns'][:8])}"
        for t in context.get("tables", [])[:10]
    )
    rel_list = "\n".join(
        f"  - {r['from_table']}.{r['from_column']} → {r['to_table']}.{r['to_column']}"
        for r in context.get("relationships", [])[:10]
    )
    return f"""You are migrating an SSAS Tabular model to Snowflake SQL.

Convert this DAX expression to a Snowflake SQL expression usable in a view or semantic view metric.
{extra_context}
Name: {name}
DAX:
{expression}

Tables (partial):
{table_list}

Relationships:
{rel_list}

Rules:
- Return ONLY the SQL expression — no explanation, no markdown code fences.
- Strip Table[Column] to just the column name (unqualified).
- Snowflake syntax: IFF, CURRENT_USER(), DATEADD('unit', n, col), IFF(b=0, NULL, a/b).
- CALCULATE with filters → CASE WHEN or filtered aggregation.
- RELATED(Dim[Col]) → column available via JOIN in view context.
- Time intelligence → date-range WHERE conditions or window functions.
- SELECTEDMEASURE() inside a calc group item: replace with the base measure SQL already provided.
- If genuinely untranslatable, return: MANUAL_REVIEW: <short reason>
"""


def translate_via_llm(name: str, expression: str, context: dict, conn,
                      extra_context: str = "") -> tuple[str, str]:
    prompt = build_llm_prompt(name, expression, context, extra_context)
    try:
        cur = conn.cursor()
        # Use $$ dollar-quoting to avoid Python's %-formatting interference when
        # the connector converts ? → %s and runs `sql % (prompt,)`. Any % character
        # inside $$ is treated as literal text by Snowflake's SQL parser.
        # Fall back to single-quote escaping only if the prompt itself contains $$.
        if "$$" not in prompt:
            cur.execute(
                f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-5', $${prompt}$$) AS r"
            )
        else:
            # Prompt contains $$: escape single quotes + backslashes for SQL literal
            safe = prompt.replace("\\", "\\\\").replace("'", "''")
            cur.execute(
                f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-5', '{safe}') AS r"
            )
        row = cur.fetchone()
        result = _clean_llm_sql((row[0] or "").strip())
        if result.upper().startswith("MANUAL_REVIEW"):
            return result, "manual_review"
        return result, "llm"
    except Exception as e:
        return f"MANUAL_REVIEW: LLM call failed — {e}", "manual_review"


def translate_expression(name: str, expression: str, context: dict, conn,
                         extra_context: str = "") -> dict:
    if not expression or not expression.strip():
        return {"name": name, "original_dax": expression,
                "sql_translation": None, "method": "manual_review",
                "notes": "Empty expression"}

    sql = try_pattern_match(expression)
    if sql:
        return {"name": name, "original_dax": expression,
                "sql_translation": sql, "method": "pattern", "notes": None}

    if is_complex(expression) or conn is not None:
        if conn is None:
            return {"name": name, "original_dax": expression,
                    "sql_translation": None, "method": "manual_review",
                    "notes": "Complex DAX; no --connection provided for LLM fallback"}
        sql, method = translate_via_llm(name, expression, context, conn, extra_context)
        return {"name": name, "original_dax": expression,
                "sql_translation": sql if method != "manual_review" else None,
                "method": method,
                "notes": sql if method == "manual_review" else None}

    if conn:
        sql, method = translate_via_llm(name, expression, context, conn, extra_context)
        return {"name": name, "original_dax": expression,
                "sql_translation": sql if method != "manual_review" else None,
                "method": method,
                "notes": sql if method == "manual_review" else None}

    return {"name": name, "original_dax": expression,
            "sql_translation": None, "method": "manual_review",
            "notes": "Unrecognised pattern; provide --connection for LLM fallback"}


# ---------------------------------------------------------------------------
# Calculation group expansion: N items × M base measures
# ---------------------------------------------------------------------------

def expand_calculation_group(cg_table: dict, base_measures: list[dict],
                              context: dict, conn) -> list[dict]:
    """
    For each calculation item × each translated base measure, produce a concrete
    metric with SELECTEDMEASURE() replaced by the base measure's SQL.

    Returns list of result dicts with source="calc_group_expansion".
    """
    cg = cg_table.get("calculation_group")
    if not cg:
        return []

    results = []
    for item in cg["items"]:
        item_expr  = item["expression"]
        item_name  = item["name"]

        for bm in base_measures:
            base_sql = bm.get("sql_translation") or f"/* {bm['name']} — translate first */"
            expanded_name = f"{bm['name']} {item_name}"

            # Identity item: SELECTEDMEASURE() alone → just the base measure SQL
            if item_expr.strip() == "__SELECTEDMEASURE__" or \
               re.match(r"^\s*SELECTEDMEASURE\s*\(\s*\)\s*$", item_expr, re.I):
                results.append({
                    "name":            expanded_name,
                    "original_dax":    item_expr,
                    "sql_translation": base_sql,
                    "method":          "pattern",
                    "notes":           None,
                    "table":           cg_table["name"],
                    "type":            "calc_group_expansion",
                    "source":          "calc_group_expansion",
                    "base_measure":    bm["name"],
                    "calc_item":       item_name,
                    "format_string":   item.get("format_string_expression") or bm.get("format_string"),
                    "kpi":             None,
                })
                continue

            # Replace SELECTEDMEASURE() placeholder with base measure SQL
            expr_with_base = re.sub(
                r"SELECTEDMEASURE\s*\(\s*\)", base_sql, item_expr, flags=re.I
            )
            extra = (
                f"Context: this is a calculation group item '{item_name}' "
                f"applied to base measure '{bm['name']}' (SQL: {base_sql}). "
                "Replace any remaining SELECTEDMEASURE() references with that SQL."
            )
            r = translate_expression(expanded_name, expr_with_base, context, conn, extra)
            r["table"]       = cg_table["name"]
            r["type"]        = "calc_group_expansion"
            r["source"]      = "calc_group_expansion"
            r["base_measure"] = bm["name"]
            r["calc_item"]   = item_name
            r["format_string"] = item.get("format_string_expression") or bm.get("format_string")
            r["kpi"]         = None
            results.append(r)

    return results


# ---------------------------------------------------------------------------
# DAG-ordered translation
# ---------------------------------------------------------------------------

def _translate_with_dag(
    inventory: dict, dag_path: str, conn
) -> tuple[list[dict], dict, int, int, int]:
    """
    Translate DAX measures and calculated columns in DAG topological order.

    For each node, passes already-translated dependency SQL as extra LLM context
    so compound measures can reference their dependencies by value rather than
    by name, producing fully self-contained SQL expressions.

    Returns (results, updated_dag, pattern_count, llm_count, manual_count).
    The updated_dag has sql_expr and inline_sql filled in for each translated node.
    """
    with open(dag_path, "r", encoding="utf-8") as f:
        dag = json.load(f)

    dag_nodes: dict = dag["nodes"]
    topo_order: list = dag["topological_order"]

    # Translatable nodes: measures and calculated columns with DAX expressions
    translatable_ids = [
        nid for nid in topo_order
        if dag_nodes.get(nid, {}).get("type") in ("measure", "calculated_column")
        and dag_nodes.get(nid, {}).get("dax_expr")
    ]

    # Seed with physical/renamed columns (already have sql_expr)
    translated_sql: dict[str, str] = {
        nid: node["sql_expr"]
        for nid, node in dag_nodes.items()
        if node["type"] in ("physical_column", "renamed_column") and node.get("sql_expr")
    }

    results: list[dict] = []
    pattern_count = llm_count = manual_count = 0

    for nid in translatable_ids:
        node = dag_nodes[nid]
        tname = node["table"]
        name  = node["name"]
        expr  = node["dax_expr"]
        ntype = node["type"]

        # Build dependency context from already-translated nodes
        dep_ids = [e["to"] for e in dag["edges"] if e["from"] == nid]
        dep_lines = [
            f"  {dep_id.split('.', 1)[-1]} = {translated_sql[dep_id]}"
            for dep_id in dep_ids
            if dep_id in translated_sql
        ]
        extra_context = ""
        if dep_lines:
            extra_context = (
                "Dependency expressions already translated to Snowflake SQL "
                "(use these SQL values inline — do NOT reference them by name):\n"
                + "\n".join(dep_lines)
            )

        r = translate_expression(name, expr, inventory, conn, extra_context)
        r["table"] = tname
        r["type"]  = ntype

        # Enrich with inventory metadata
        inv_table = next(
            (t for t in inventory["tables"] if t["name"] == tname), None
        )
        if inv_table:
            if ntype == "measure":
                m_entry = next(
                    (m for m in inv_table.get("measures", []) if m["name"] == name), None
                )
                if m_entry:
                    r["format_string"] = m_entry.get("format_string")
                    r["kpi"]           = m_entry.get("kpi")
            elif ntype == "calculated_column":
                r["data_type"] = node.get("data_type")

        results.append(r)

        if r["method"] == "pattern":
            pattern_count += 1
        elif r["method"] == "llm":
            llm_count += 1
        else:
            manual_count += 1

        # Write translation back to DAG node for downstream consumers
        if r.get("sql_translation"):
            dag_nodes[nid]["sql_expr"]   = r["sql_translation"]
            dag_nodes[nid]["inline_sql"] = r["sql_translation"]
            translated_sql[nid]          = r["sql_translation"]

    return results, dag, pattern_count, llm_count, manual_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Translate DAX measures to Snowflake SQL")
    parser.add_argument("--inventory",  required=True,
                        help="Path to inventory.json from parse_bim.py")
    parser.add_argument("--output",     required=True,
                        help="Output JSON file for translated measures")
    parser.add_argument("--connection", default=None,
                        help="Snowflake connection name for LLM fallback (e.g. COCO_JK)")
    parser.add_argument("--dag", default=None,
                        help="Path to dax_dag.json (from build_dax_dag.py); enables "
                             "topological translation order with dependency SQL context")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    conn = None
    if args.connection:
        try:
            conn = snowflake.connector.connect(connection_name=args.connection)
        except Exception as e:
            print(f"WARNING: Could not connect ({e}). LLM fallback disabled.", file=sys.stderr)

    results = []
    pattern_count = llm_count = manual_count = cg_expand_count = 0

    # --- DAG-ordered translation (when --dag is provided) ---
    _use_dag = bool(args.dag)
    if _use_dag:
        results, _upd_dag, pattern_count, llm_count, manual_count = \
            _translate_with_dag(inventory, args.dag, conn)
        # Calc group expansions are not in the DAG — handle them after DAG translation
        _base_measures = [
            r for r in results
            if r.get("type") == "measure" and r.get("sql_translation")
        ]
        for _cg_table in inventory["tables"]:
            if _cg_table.get("calculation_group"):
                _expanded = expand_calculation_group(
                    _cg_table, _base_measures, inventory, conn
                )
                results.extend(_expanded)
                cg_expand_count += len(_expanded)
                for r in _expanded:
                    if r["method"] == "pattern":   pattern_count += 1
                    elif r["method"] == "llm":     llm_count += 1
                    else:                          manual_count += 1
        # Write updated DAG (sql_expr / inline_sql filled in) back to file
        with open(args.dag, "w", encoding="utf-8") as _f:
            json.dump(_upd_dag, _f, indent=2, default=str)
        print(f"Updated DAG: {args.dag}")

    # --- Table-by-table translation path (when --dag is NOT provided) ---
    for table in (inventory["tables"] if not _use_dag else []):
        # --- Regular measures ---
        table_measures_translated = []
        for measure in table["measures"]:
            r = translate_expression(measure["name"], measure["expression"], inventory, conn)
            r.update({"table": table["name"], "type": "measure",
                      "format_string": measure.get("format_string"),
                      "kpi": measure.get("kpi")})
            results.append(r)
            table_measures_translated.append(r)
            if r["method"] == "pattern":   pattern_count += 1
            elif r["method"] == "llm":     llm_count += 1
            else:                          manual_count += 1

        # --- Calculated columns ---
        for calc_col in table["calculated_columns"]:
            if not calc_col.get("expression"):
                continue
            r = translate_expression(calc_col["name"], calc_col["expression"], inventory, conn)
            r.update({"table": table["name"], "type": "calculated_column",
                      "data_type": calc_col.get("data_type")})
            results.append(r)
            if r["method"] == "pattern":   pattern_count += 1
            elif r["method"] == "llm":     llm_count += 1
            else:                          manual_count += 1

        # --- Calculation group items (compat 1500+) ---
        if table.get("calculation_group"):
            # Find successfully translated base measures from ALL tables
            # to expand against this calculation group
            all_translated = [
                r for r in results
                if r.get("type") == "measure" and r.get("sql_translation")
            ]
            expanded = expand_calculation_group(table, all_translated, inventory, conn)
            results.extend(expanded)
            cg_expand_count += len(expanded)
            for r in expanded:
                if r["method"] == "pattern":   pattern_count += 1
                elif r["method"] == "llm":     llm_count += 1
                else:                          manual_count += 1

    if conn:
        conn.close()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    total = len(results)
    print(f"Translation complete: {total} expressions")
    print(f"  Pattern (auto):          {pattern_count} ({pattern_count*100//total if total else 0}%)")
    print(f"  LLM (Cortex):            {llm_count} ({llm_count*100//total if total else 0}%)")
    print(f"  Manual review:           {manual_count} ({manual_count*100//total if total else 0}%)")
    if cg_expand_count:
        print(f"  Calc group expansions:   {cg_expand_count}")
    if manual_count:
        print("\nExpressions requiring manual review:")
        for r in results:
            if r["method"] == "manual_review":
                print(f"  [{r.get('type','?')}] {r.get('table','?')}.{r['name']}: {r.get('notes','')}")
    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
