#!/usr/bin/env python3
"""
build_dax_dag.py - Build a DAX dependency DAG from an SSAS Tabular inventory.

Analyses all measures, calculated columns, and physical columns to build a
directed acyclic graph (DAG) of dependencies.  The topological ordering
produced here is consumed by convert_dax.py to ensure each expression is
translated AFTER all of its dependencies, so compound measures can be
translated with their dependencies' SQL already available as LLM context.

Node types:
  physical_column   - regular imported column (no DAX expression)
  renamed_column    - imported column with a Power Query rename in sf_column_map
  calculated_column - column defined by a DAX expression (row-level → facts[])
  measure           - DAX measure (aggregate/KPI/time-intelligence → metrics[])

Output: dax_dag.json
  {
    "nodes": { "<TableName>.<Name>": { ...node fields } },
    "edges": [ {"from": "<id>", "to": "<id>", "reason": "..."} ],
    "topological_order": [ "<id>", ... ],   # leaves first, compound measures last
    "warnings": [ "..." ]
  }

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/build_dax_dag.py \\
        --inventory <OUTPUT_DIR>/ssas_inventory.json \\
        --output    <OUTPUT_DIR>/dax_dag.json
"""

import argparse
import json
import re
from collections import defaultdict, deque
from pathlib import Path


# ---------------------------------------------------------------------------
# DAX reference extraction
# ---------------------------------------------------------------------------

# DAX function names that may appear immediately before [ but are NOT table names
_DAX_FUNCTIONS = frozenset({
    "IF", "AND", "OR", "NOT", "IN",
    "CALCULATE", "CALCULATETABLE", "FILTER", "ALL", "ALLEXCEPT", "ALLSELECTED",
    "SUMX", "AVERAGEX", "MAXX", "MINX", "COUNTX", "CONCATENATEX", "RANKX",
    "TOPN", "ADDCOLUMNS", "SELECTCOLUMNS", "SUMMARIZE", "GROUPBY",
    "VALUES", "DISTINCT", "RELATED", "RELATEDTABLE", "USERELATIONSHIP",
    "CROSSFILTER", "KEEPFILTERS", "NATURALINNERJOIN", "NATURALLEFTOUTERJOIN",
    "DATEADD", "DATESINPERIOD", "DATESMTD", "DATESQTD", "DATESYTD",
    "TOTALYTD", "TOTALQTD", "TOTALMTD", "SAMEPERIODLASTYEAR",
    "PREVIOUSMONTH", "PREVIOUSQUARTER", "PREVIOUSYEAR", "PARALLELPERIOD",
    "SELECTEDVALUE", "HASONEVALUE", "SELECTEDMEASURE", "SELECTEDMEASURENAME",
    "ISSELECTEDMEASURE", "ISBLANK", "ISNUMBER", "ISTEXT", "ISLOGICAL",
    "SWITCH", "COALESCE", "IFERROR", "DIVIDE", "FORMAT",
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "DATE", "TIME", "NOW", "TODAY", "EOMONTH", "EDATE",
    "LEFT", "RIGHT", "MID", "LEN", "TRIM", "UPPER", "LOWER",
    "FIND", "SEARCH", "REPLACE", "SUBSTITUTE", "CONCATENATE",
    "INT", "ROUND", "ROUNDUP", "ROUNDDOWN", "ABS", "SQRT",
    "SUM", "AVERAGE", "COUNT", "COUNTA", "COUNTROWS", "COUNTBLANK",
    "MIN", "MAX", "MEDIAN", "PERCENTILE", "STDEV", "VAR",
    "DISTINCTCOUNT", "DISTINCTCOUNTNOBLANK",
})

# Pattern: optional table prefix + [name]
_REF_PATTERN = re.compile(
    r"'([^']+)'\s*\[([^\]]+)\]"           # group 1,2: 'Quoted Table'[Name]
    r"|(?<!['\w])([\w][\w\s]*)\[([^\]]+)\]"  # group 3,4: UnquotedTable[Name]
    r"|\[([^\]]+)\]",                       # group 5:   bare [Name]
    re.DOTALL,
)


def extract_dax_refs(expr: str) -> list[tuple[str | None, str]]:
    """
    Parse a DAX expression and return all column/measure references.

    Returns list of (table_name_or_None, identifier_name):
      - 'Table'[Col]    → ("Table", "Col")
      - Table[Col]      → ("Table", "Col")  (if Table is not a DAX function)
      - [Name]          → (None, "Name")    (same-table column or measure)
    """
    if not expr:
        return []

    refs: list[tuple[str | None, str]] = []
    seen: set[tuple] = set()

    for m in _REF_PATTERN.finditer(expr):
        if m.group(1) is not None:
            # 'Quoted Table'[Name]
            key = (m.group(1).strip(), m.group(2).strip())
        elif m.group(3) is not None:
            # UnquotedTable[Name] — check it's not a DAX function
            tbl = m.group(3).strip()
            col = m.group(4).strip()
            key = (None, col) if tbl.upper() in _DAX_FUNCTIONS else (tbl, col)
        elif m.group(5) is not None:
            # Bare [Name]
            key = (None, m.group(5).strip())
        else:
            continue

        if key not in seen:
            seen.add(key)
            refs.append(key)

    return refs


# ---------------------------------------------------------------------------
# DAG builder
# ---------------------------------------------------------------------------

def build_dag(inventory: dict) -> dict:
    """
    Build a dependency DAG for all DAX objects in the inventory.

    Returns dict with keys: nodes, edges, topological_order, warnings.
    """
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    warnings: list[str] = []

    # Lookup: table_lower → { col_name_lower → node_id }
    col_lower_to_id: dict[str, dict[str, str]] = defaultdict(dict)
    # Lookup: table_lower → { measure_name_lower → node_id }
    meas_lower_to_id: dict[str, dict[str, str]] = defaultdict(dict)
    # Normalise table names: lowercase → canonical
    table_canon: dict[str, str] = {}

    for table in inventory["tables"]:
        tname = table["name"]
        table_canon[tname.lower()] = tname
        sf_col_map = table.get("sf_column_map") or {}

        # Physical / renamed columns
        for col in table.get("columns", []):
            cname = col["name"]
            sf_phys = (sf_col_map.get(cname) or cname).upper()
            is_renamed = (
                sf_col_map.get(cname) is not None
                and sf_col_map[cname].upper() != cname.upper()
            )
            node_id = f"{tname}.{cname}"
            nodes[node_id] = {
                "id": node_id,
                "table": tname,
                "name": cname,
                "type": "renamed_column" if is_renamed else "physical_column",
                "sf_physical": sf_phys,
                "dax_expr": None,
                "data_type": col.get("data_type"),
                # Physical columns are already resolved — sql_expr = Snowflake name
                "sql_expr": sf_phys,
                "inline_sql": sf_phys,
            }
            col_lower_to_id[tname.lower()][cname.lower()] = node_id

        # Calculated columns
        for col in table.get("calculated_columns", []):
            cname = col["name"]
            node_id = f"{tname}.{cname}"
            nodes[node_id] = {
                "id": node_id,
                "table": tname,
                "name": cname,
                "type": "calculated_column",
                "sf_physical": None,
                "dax_expr": col.get("expression"),
                "data_type": col.get("data_type"),
                "sql_expr": None,    # filled by convert_dax.py
                "inline_sql": None,  # filled by convert_dax.py
            }
            col_lower_to_id[tname.lower()][cname.lower()] = node_id

        # Measures
        for measure in table.get("measures", []):
            mname = measure["name"]
            node_id = f"{tname}.{mname}"
            nodes[node_id] = {
                "id": node_id,
                "table": tname,
                "name": mname,
                "type": "measure",
                "sf_physical": None,
                "dax_expr": measure.get("expression"),
                "format_string": measure.get("format_string"),
                "data_type": "FLOAT",
                "sql_expr": None,    # filled by convert_dax.py
                "inline_sql": None,  # filled by convert_dax.py
            }
            meas_lower_to_id[tname.lower()][mname.lower()] = node_id

    # Build edges by parsing DAX expressions
    edge_set: set[tuple[str, str]] = set()

    for node_id, node in nodes.items():
        expr = node.get("dax_expr")
        if not expr:
            continue

        tname = node["table"]
        for ref_table, ref_name in extract_dax_refs(expr):
            if ref_table:
                # Explicit cross-table reference
                canon = table_canon.get(ref_table.lower(), ref_table)
                target_id = (
                    col_lower_to_id.get(canon.lower(), {}).get(ref_name.lower())
                    or meas_lower_to_id.get(canon.lower(), {}).get(ref_name.lower())
                )
            else:
                # Same-table: check columns first, then measures
                target_id = (
                    col_lower_to_id.get(tname.lower(), {}).get(ref_name.lower())
                    or meas_lower_to_id.get(tname.lower(), {}).get(ref_name.lower())
                )

            if target_id and target_id != node_id:
                key = (node_id, target_id)
                if key not in edge_set:
                    edge_set.add(key)
                    edges.append({
                        "from": node_id,
                        "to": target_id,
                        "reason": "cross_table_ref" if ref_table else "same_table_ref",
                    })

    # Topological sort (Kahn's algorithm — dependency-first, i.e. leaves → roots)
    # Edge direction: "from" depends on "to"
    in_degree: dict[str, int] = defaultdict(int)
    dependents: dict[str, list[str]] = defaultdict(list)

    for edge in edges:
        in_degree[edge["from"]] += 1
        dependents[edge["to"]].append(edge["from"])

    # Start with nodes that have no outgoing dependencies
    queue: deque[str] = deque(nid for nid in nodes if in_degree[nid] == 0)
    topo_order: list[str] = []
    visited: set[str] = set()

    while queue:
        nid = queue.popleft()
        if nid in visited:
            continue
        visited.add(nid)
        topo_order.append(nid)
        for dep_of in dependents.get(nid, []):
            in_degree[dep_of] -= 1
            if in_degree[dep_of] == 0:
                queue.append(dep_of)

    # Any node not visited is part of a cycle
    remaining = set(nodes.keys()) - visited
    if remaining:
        for r in sorted(remaining):
            warnings.append(
                f"Cycle or unresolvable dependency — placed at end of order: {r}"
            )
        topo_order.extend(sorted(remaining))

    return {
        "nodes": nodes,
        "edges": edges,
        "topological_order": topo_order,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def _print_summary(dag: dict, output_path: str, inventory_path: str) -> None:
    nodes = dag["nodes"]
    n_phys  = sum(1 for n in nodes.values() if n["type"] in ("physical_column", "renamed_column"))
    n_calc  = sum(1 for n in nodes.values() if n["type"] == "calculated_column")
    n_meas  = sum(1 for n in nodes.values() if n["type"] == "measure")
    n_edges = len(dag["edges"])

    print(f"DAX dependency graph built: {len(nodes)} nodes, {n_edges} edges")
    print(f"  Physical / renamed columns : {n_phys}")
    print(f"  Calculated columns         : {n_calc}")
    print(f"  Measures                   : {n_meas}")
    print(f"  Topological order          : {len(dag['topological_order'])} nodes")

    if dag["warnings"]:
        print(f"\nWarnings ({len(dag['warnings'])}):")
        for w in dag["warnings"]:
            print(f"  ⚠ {w}")

    # Show a few compound measures (those with 2+ deps) for quick review
    dep_counts: dict[str, int] = defaultdict(int)
    for edge in dag["edges"]:
        dep_counts[edge["from"]] += 1
    compound = [
        (nid, cnt) for nid, cnt in dep_counts.items()
        if cnt >= 2 and nodes[nid]["type"] == "measure"
    ]
    if compound:
        print(f"\nCompound measures (≥2 deps) — will be translated with inline context:")
        for nid, cnt in sorted(compound, key=lambda x: -x[1])[:10]:
            print(f"  {nid}  ({cnt} deps)")

    print(f"\nOutput: {output_path}")
    print(f"\nNext — translate measures in DAG order:")
    print(
        f"  uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/convert_dax.py \\\n"
        f"    --inventory {inventory_path} --dag {output_path} \\\n"
        f"    --output <OUTPUT_DIR>/ssas_measures_translated.json --connection <CONN>"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build a DAX dependency DAG from an SSAS Tabular inventory"
    )
    parser.add_argument(
        "--inventory", required=True,
        help="Path to ssas_inventory.json (from parse_bim.py)",
    )
    parser.add_argument(
        "--output", required=True,
        help="Output path for dax_dag.json",
    )
    args = parser.parse_args()

    with open(args.inventory, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    dag = build_dag(inventory)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dag, f, indent=2, default=str)

    _print_summary(dag, args.output, args.inventory)


if __name__ == "__main__":
    main()
