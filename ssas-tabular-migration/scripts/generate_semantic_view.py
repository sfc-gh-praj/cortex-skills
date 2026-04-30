#!/usr/bin/env python3
"""
generate_semantic_view.py - Generate a Snowflake Semantic View YAML from inventory + translated measures.

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_semantic_view.py \
        --inventory inventory.json \
        --measures measures_translated.json \
        --target-schema MY_DB.MY_SCHEMA \
        --output semantic_view.yaml
"""

import argparse
import json
import re
import sys
from pathlib import Path


TYPE_TO_SEMANTIC = {
    "int64":    "NUMBER",
    "int":      "NUMBER",
    "integer":  "NUMBER",
    "double":   "FLOAT",
    "decimal":  "FLOAT",
    "currency": "FLOAT",
    "string":   "TEXT",
    "text":     "TEXT",
    "datetime": "TIMESTAMP",
    "date":     "DATE",
    "time":     "TIME",
    "boolean":  "BOOLEAN",
    "bool":     "BOOLEAN",
    "binary":   "TEXT",
}

NUMERIC_TYPES = {"NUMBER", "FLOAT"}

# Snowflake keywords that are invalid as unquoted column aliases in views/semantic views
_SF_RESERVED_WORDS = frozenset({
    "DATE", "TIME", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "ORDER", "GROUP", "LEVEL", "RANK", "STATUS", "TYPE", "NAME", "VALUE",
    "USER", "ROLE", "TABLE", "SCHEMA", "DATABASE", "COLUMN", "INDEX",
    "KEY", "PRIMARY", "FOREIGN", "UNIQUE", "NULL", "TRUE", "FALSE",
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "HAVING", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT", "WITH",
    "CASE", "WHEN", "THEN", "ELSE", "BETWEEN", "LIKE", "EXISTS",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "INTERVAL", "START", "END",
})


def _deaggregate_fact_expr(expr: str) -> tuple[str, bool]:
    """
    Remove top-level aggregate function wrappers from a fact expression.

    Fact expressions must be row-level (no SUM/COUNT/AVG etc.) because they map
    to individual row values in a SELECT context.  LLMs often mistranslate DAX
    calculated columns as aggregate measures.

    Examples:
      SUM(SalesAmount) - SUM(TotalProductCost)  →  SalesAmount - TotalProductCost
      SUM(Quantity * UnitPrice)                 →  Quantity * UnitPrice

    Returns (cleaned_expr, was_changed).
    """
    cleaned = re.sub(
        r'\b(SUM|COUNT|AVG|MIN|MAX|MEDIAN|STDEV|VAR|VARIANCE|STDDEV)\s*\(([^()]+)\)',
        r'\2',
        expr,
        flags=re.I,
    )
    return cleaned.strip(), cleaned.strip() != expr.strip()


def _find_cross_table_refs(expr: str, known_table_names: set) -> list[str]:
    """
    Return 'TableName.Column' tokens in expr where TableName is a known table.

    These are invalid inside a semantic view metric expression — cross-table
    column references must be replaced with an equivalent column from the
    local fact table (e.g. reachable via a JOIN relationship).
    """
    return [
        m.group(0)
        for m in re.finditer(r'\b(\w+)\.(\w+)\b', expr)
        if m.group(1).lower() in known_table_names
    ]


def _find_metric_refs(expr: str, metric_safe_names: set) -> list[str]:
    """
    Return metric names referenced in expr via DAX bracket syntax [Name] or
    as bare identifiers matching known metric safe-names.

    These are invalid in a semantic view metric expr: metric names are not
    SQL identifiers and cannot be referenced from within another metric.
    """
    bracket_refs = re.findall(r'\[(\w+)\]', expr)
    bare_refs = [
        w for w in re.findall(r'\b([A-Za-z]\w+)\b', expr)
        if w.lower() in metric_safe_names
    ]
    return list(set(bracket_refs + bare_refs))


def safe_name(name: str) -> str:
    """Convert display name to a safe YAML identifier."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name).lower().strip("_")


def semantic_type(data_type: str) -> str:
    return TYPE_TO_SEMANTIC.get((data_type or "string").lower(), "TEXT")


def is_numeric(data_type: str) -> bool:
    return semantic_type(data_type) in NUMERIC_TYPES


def format_string_to_synonyms(format_string: str | None) -> list[str]:
    """Infer informal synonyms from a format string."""
    if not format_string:
        return []
    if "$" in format_string:
        return ["in dollars", "dollar amount"]
    if "%" in format_string:
        return ["percent", "percentage"]
    return []


def build_table_entry(table: dict, schema: str, measures_by_table: dict,
                      calc_cols_index: dict = None,
                      known_table_names: set | None = None,
                      all_metric_names: set | None = None) -> dict:
    tname = table["name"]
    safe = safe_name(tname)

    # base_table must be an object with database/schema/table keys
    parts = schema.split(".")
    db_name     = parts[0] if len(parts) >= 1 else schema
    schema_name = parts[1] if len(parts) >= 2 else "PUBLIC"
    base_table = {
        "database": db_name,
        "schema":   schema_name,
        "table":    tname.upper().replace(" ", "_"),
    }

    # Use sf_column_map if available (from enriched inventory) to resolve
    # Snowflake physical column names for expr fields.
    sf_col_map = table.get("sf_column_map", {})

    # Build index of RELATED() calc cols to skip from semantic view
    calc_resolution = table.get("calculated_column_resolution", [])
    cross_table_cols = {
        cr["name"] for cr in calc_resolution if cr.get("strategy") == "left_join"
    }

    dimensions = []
    facts = []
    metrics = []

    # Build hierarchy index: column_name → list of hierarchy descriptions
    hier_info: dict[str, list[str]] = {}  # col_name_lower → [hierarchy description, ...]
    hier_synonyms: dict[str, list[str]] = {}  # col_name_lower → [synonym, ...]
    for h in table.get("hierarchies", []):
        levels = sorted(h.get("levels", []), key=lambda l: l.get("ordinal", 0))
        level_names = [l.get("column", l.get("name", "?")) for l in levels]
        hier_name = h.get("name", "")
        for i, lev in enumerate(levels):
            col = lev.get("column", lev.get("name", ""))
            col_lower = col.lower()
            # Build description: position in hierarchy
            depth_label = "top" if i == 0 else ("leaf" if i == len(levels) - 1 else f"level {i+1}")
            path = " > ".join(level_names)
            desc = f"{hier_name} hierarchy ({depth_label}): {path}"
            hier_info.setdefault(col_lower, []).append(desc)
            # Add synonyms for drill-down context
            hier_synonyms.setdefault(col_lower, []).append(f"{hier_name.lower()} hierarchy")
            if i == 0:
                hier_synonyms[col_lower].append(f"drill down from {col}")
            if i == len(levels) - 1:
                hier_synonyms[col_lower].append(f"drill down to {col}")

    # Regular columns → dimensions (text/date/bool) or facts (numeric)
    for col in table["columns"]:
        col_name = safe_name(col["name"])
        stype = semantic_type(col["data_type"])
        # Use Snowflake physical name if available, otherwise SSAS name
        sf_physical = sf_col_map.get(col["name"])
        expr_name = sf_physical if sf_physical else col["name"]
        # Warn if the resolved column name is a Snowflake reserved word
        if expr_name.upper() in _SF_RESERVED_WORDS:
            print(
                f"  ⚠ RESERVED WORD: {tname}.{col['name']} — expr '{expr_name}' is a "
                f"Snowflake reserved word.\n"
                f"    Fix: set sf_column_map['{col['name']}'] = '<SAFE_ALIAS>' in the "
                f"inventory, rename the alias in the DDL view, then regenerate."
            )
        entry = {"name": col_name, "expr": expr_name, "data_type": stype}
        if col.get("description"):
            entry["description"] = col["description"]
        # Enrich with hierarchy metadata
        col_lower = col["name"].lower()
        if col_lower in hier_info:
            hier_descs = hier_info[col_lower]
            existing_desc = entry.get("description", "")
            hier_text = "; ".join(hier_descs)
            entry["description"] = f"{existing_desc}; {hier_text}" if existing_desc else hier_text
        if col_lower in hier_synonyms:
            entry.setdefault("synonyms", []).extend(
                s for s in hier_synonyms[col_lower] if s not in entry.get("synonyms", [])
            )
        if stype in NUMERIC_TYPES:
            facts.append(entry)
        else:
            dimensions.append(entry)

    # Translated measures → metrics
    for item in measures_by_table.get(tname, []):
        if item.get("type") != "measure":
            continue
        if not item.get("sql_translation"):
            continue  # skip manual_review items
        metric_name = safe_name(item["name"])
        expr = item["sql_translation"]

        # Safeguard: cross-table column references are invalid in metric exprs
        if known_table_names:
            xt_refs = _find_cross_table_refs(expr, known_table_names)
            if xt_refs:
                print(
                    f"  ⚠ CROSS-TABLE REF: {tname}.{metric_name} — metric references "
                    f"column(s) from another table: {', '.join(xt_refs[:3])}\n"
                    f"    Replace with the equivalent column from {tname} "
                    f"(accessible via the JOIN relationship, e.g. a local date column)"
                )

        # Safeguard: metric expressions cannot reference other metrics by identifier
        if all_metric_names:
            other_metrics = all_metric_names - {metric_name}
            mrefs = _find_metric_refs(expr, other_metrics)
            if mrefs:
                print(
                    f"  ⚠ METRIC REF: {tname}.{metric_name} — metric expr references "
                    f"other metric(s): {', '.join(mrefs[:3])}\n"
                    f"    Inline those metrics' SQL directly, or re-run convert_dax.py "
                    f"with --dag for automatic dependency-aware inlining"
                )

        synonyms = format_string_to_synonyms(item.get("format_string"))
        # Use original DAX name as an additional synonym hint
        if item["name"].lower() != metric_name:
            synonyms.insert(0, item["name"])
        metric = {
            "name": metric_name,
            "expr": expr,
            "data_type": "FLOAT",
        }
        if synonyms:
            metric["synonyms"] = synonyms
        if item.get("kpi"):
            kpi = item["kpi"]
            if kpi.get("target_expression"):
                metric["_kpi_target_dax"] = kpi["target_expression"]  # informational
        metrics.append(metric)

    # Calculated columns → additional dimensions/facts with SQL expressions
    # Skip cross-table RELATED() columns (resolved via JOINs in PBI views instead)
    if calc_cols_index is None:
        calc_cols_index = {}
    for col in table["calculated_columns"]:
        if col["name"] in cross_table_cols:
            continue  # RELATED() — handled by generate_powerbi_views.py via LEFT JOIN
        item = calc_cols_index.get((tname, col["name"]))
        if item and item.get("sql_translation"):
            col_name = safe_name(col["name"])
            stype = semantic_type(col.get("data_type", "string"))
            expr = item["sql_translation"]
            if stype in NUMERIC_TYPES:
                # Facts must be row-level expressions — strip any aggregate wrappers
                clean_expr, was_changed = _deaggregate_fact_expr(expr)
                if was_changed:
                    print(
                        f"  ⚠ FACT AGGREGATE STRIPPED: {tname}.{col_name} — "
                        f"removed aggregate wrapper (facts must be row-level expressions, "
                        f"not aggregations like SUM/COUNT)"
                    )
                entry = {"name": col_name, "expr": clean_expr, "data_type": stype}
                facts.append(entry)
            else:
                entry = {"name": col_name, "expr": expr, "data_type": stype}
                dimensions.append(entry)

    result = {
        "name": safe,
        "base_table": base_table,
    }

    # Primary key: first column marked is_key, or first column
    key_cols = [c for c in table["columns"] if c.get("is_key")]
    if key_cols:
        # primary_key must be an object with a "columns" array
        result["primary_key"] = {"columns": [safe_name(key_cols[0]["name"])]}

    if dimensions:
        result["dimensions"] = dimensions
    if facts:
        result["facts"] = facts
    if metrics:
        result["metrics"] = metrics

    return result


def build_joins(relationships: list[dict]) -> list[dict]:
    joins = []
    for r in relationships:
        if not r.get("is_active", True):
            continue  # skip inactive relationships
        joins.append({
            "name": f"{safe_name(r['from_table'])}_to_{safe_name(r['to_table'])}",
            "left_table": safe_name(r["from_table"]),
            "right_table": safe_name(r["to_table"]),
            "relationship": "many_to_one",
            "join_type": "left",
            "join_condition": {
                "left_expr": r["from_column"],
                "right_expr": r["to_column"],
            },
        })
    return joins


def dict_to_yaml(d, indent=0) -> str:
    """Minimal YAML serialiser (no external dep)."""
    lines = []
    prefix = "  " * indent
    for key, value in d.items():
        if value is None:
            continue
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            lines.append(dict_to_yaml(value, indent + 1))
        elif isinstance(value, list):
            if not value:
                continue
            lines.append(f"{prefix}{key}:")
            for item in value:
                if isinstance(item, dict):
                    item_lines = dict_to_yaml(item, indent + 2).split("\n")
                    # First field uses "- " prefix
                    first = True
                    for il in item_lines:
                        if not il.strip():
                            continue
                        if first:
                            stripped = il.lstrip()
                            item_indent = "  " * (indent + 1)
                            lines.append(f"{item_indent}- {stripped}")
                            first = False
                        else:
                            lines.append(il)
                else:
                    lines.append(f"{prefix}  - {_yaml_scalar(item)}")
        else:
            lines.append(f"{prefix}{key}: {_yaml_scalar(value)}")
    return "\n".join(lines)


def _yaml_scalar(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    # Always quote if the string contains characters special to YAML or SQL single quotes,
    # or if it is a SQL expression (contains parentheses — a reliable indicator).
    needs_quoting = any(c in s for c in [
        ":", "#", "{", "}", "[", "]", ",", "&", "*", "?", "|", "<", ">",
        "=", "!", "%", "@", "`", "\n", "'", "(", ")"
    ])
    if needs_quoting:
        # Escape backslashes first, then double-quotes, then encode newlines
        escaped = s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    return s


def main():
    parser = argparse.ArgumentParser(description="Generate Snowflake Semantic View YAML from SSAS Tabular inventory")
    parser.add_argument("--inventory", required=True, help="Path to inventory.json")
    parser.add_argument("--measures", required=True, help="Path to measures_translated.json")
    parser.add_argument("--target-schema", required=True,
                        help="Snowflake target schema, e.g. MY_DB.MY_SCHEMA")
    parser.add_argument("--output", required=True, help="Output YAML file path")
    parser.add_argument("--view-name", default=None,
                        help="Override name for the semantic view (default: model name)")
    parser.add_argument("--deploy", action="store_true",
                        help="Deploy the semantic view to Snowflake after generating YAML")
    parser.add_argument("--connection", default="COCO_JK",
                        help="Snowflake connection name for --deploy (default: COCO_JK)")
    parser.add_argument("--dag", default=None,
                        help="Path to dax_dag.json (from build_dax_dag.py + convert_dax.py --dag); "
                             "uses inline_sql from DAG nodes for fully self-contained metric exprs")
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    with open(args.measures, "r", encoding="utf-8") as f:
        measures_list = json.load(f)

    # Index measures by table (list) and calc columns by (table, name) (dict)
    measures_by_table: dict[str, list] = {}
    calc_cols_index: dict[tuple[str, str], dict] = {}
    for item in measures_list:
        tname = item.get("table", "")
        measures_by_table.setdefault(tname, []).append(item)
        if item.get("type") == "calculated_column":
            calc_cols_index[(tname, item["name"])] = item

    # If a DAG was provided, overlay sql_translation with inline_sql from the DAG.
    # inline_sql is the fully dependency-inlined SQL produced by convert_dax.py --dag,
    # so metric exprs reference no other metrics and no cross-table columns by name.
    if args.dag:
        import json as _json
        with open(args.dag, "r", encoding="utf-8") as _f:
            _dag = _json.load(_f)
        _dag_inline = {
            (n["table"], n["name"]): n["inline_sql"]
            for n in _dag["nodes"].values()
            if n.get("inline_sql") and n["type"] in ("measure", "calculated_column")
        }
        for _item in measures_list:
            _key = (_item.get("table", ""), _item.get("name", ""))
            if _key in _dag_inline:
                _item["sql_translation"] = _dag_inline[_key]
        print(f"  DAG inline_sql applied: {len(_dag_inline)} nodes overridden")

    # Build sets used by safeguard checks in build_table_entry()
    known_table_names: set = set()
    for t in inventory["tables"]:
        known_table_names.add(t["name"].lower())
        known_table_names.add(safe_name(t["name"]).lower())

    all_metric_names: set = set()
    for t in inventory["tables"]:
        for m in t.get("measures", []):
            all_metric_names.add(safe_name(m["name"]).lower())
            all_metric_names.add(m["name"].lower())

    schema = args.target_schema.strip().rstrip("/")
    view_name = safe_name(args.view_name or inventory["model_name"])

    tables_yaml = []
    for table in inventory["tables"]:
        if table.get("is_hidden"):
            continue
        entry = build_table_entry(
            table, schema, measures_by_table, calc_cols_index,
            known_table_names=known_table_names,
            all_metric_names=all_metric_names,
        )
        tables_yaml.append(entry)

    joins = build_joins(inventory["relationships"])

    # Semantic view top-level structure
    sem_view = {
        "name": view_name,
        "tables": tables_yaml,
    }
    if joins:
        sem_view["joins"] = joins

    yaml_lines = [
        f"# Snowflake Semantic View — generated from SSAS Tabular model: {inventory['model_name']}",
        f"# Source compatibility level: {inventory['compatibility_level']}",
        f"# Target schema: {schema}",
        f"# IMPORTANT: Review all metric expressions before deploying.",
        f"# Expressions marked sql_translation=null require manual completion.",
        "",
        f"name: {view_name}",
        "",
        "tables:",
    ]

    for t in tables_yaml:
        tlines = dict_to_yaml(t, indent=2).split("\n")
        first = True
        for line in tlines:
            if not line.strip():
                continue
            if first:
                stripped = line.lstrip()
                yaml_lines.append(f"  - {stripped}")
                first = False
            else:
                yaml_lines.append(line)
        yaml_lines.append("")

    if joins:
        yaml_lines.append("joins:")
        for j in joins:
            jlines = dict_to_yaml(j, indent=2).split("\n")
            first = True
            for line in jlines:
                if not line.strip():
                    continue
                if first:
                    stripped = line.lstrip()
                    yaml_lines.append(f"  - {stripped}")
                    first = False
                else:
                    yaml_lines.append(line)
        yaml_lines.append("")

    yaml_text = "\n".join(yaml_lines)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(yaml_text)

    metric_count = sum(len(t.get("metrics", [])) for t in tables_yaml)
    dim_count = sum(len(t.get("dimensions", [])) for t in tables_yaml)
    fact_count = sum(len(t.get("facts", [])) for t in tables_yaml)
    print(f"Generated semantic view: {view_name}")
    print(f"  Tables:     {len(tables_yaml)}")
    print(f"  Dimensions: {dim_count}")
    print(f"  Facts:      {fact_count}")
    print(f"  Metrics:    {metric_count}")
    print(f"  Joins:      {len(joins)}")
    print(f"  Output:     {args.output}")

    # Deploy to Snowflake if requested
    if args.deploy:
        print(f"\nDeploying semantic view to {schema} ...")
        try:
            from deploy_semantic_view import deploy_semantic_view
        except ImportError:
            # If running from a different directory, try relative import
            script_dir = Path(__file__).parent
            sys.path.insert(0, str(script_dir))
            from deploy_semantic_view import deploy_semantic_view

        # Strip comment lines for deployment
        clean_lines = [l for l in yaml_text.split("\n") if not l.strip().startswith("#")]
        yaml_clean = "\n".join(clean_lines)

        try:
            fq_name = deploy_semantic_view(yaml_clean, schema, args.connection)
            print(f"\nQuery it with:")
            print(f"  cortex analyst query \"your question\" --view {fq_name}")
        except Exception as e:
            print(f"\nWARNING: Deployment failed — {e}", file=sys.stderr)
            print("You can deploy manually with:")
            print(f"  uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/deploy_semantic_view.py \\")
            print(f"    --yaml-file {args.output} --target-schema {schema} --connection {args.connection}")
    else:
        print("")
        print("Next steps:")
        print(f"  Validate:  cortex reflect {args.output} --target-schema {schema}")
        print(f"  Deploy:    re-run with --deploy --connection {args.connection}")


if __name__ == "__main__":
    main()
