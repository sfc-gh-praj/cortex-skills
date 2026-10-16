#!/usr/bin/env python3
"""
visualize_dax_dag.py - Generate an interactive HTML showing table relationships
and key migration info from the SSAS inventory.

Produces a self-contained HTML file (no external dependencies) with:
  - ER-style diagram of table joins (active + inactive relationships)
  - Table cards with column counts, row counts, measures, hierarchies
  - Summary stats panel

Usage:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/visualize_dax_dag.py \
        --dag <OUTPUT_DIR>/dax_dag.json \
        --inventory <OUTPUT_DIR>/ssas_inventory.json \
        --output <OUTPUT_DIR>/dax_dependency_graph.html
"""

import argparse
import json
import html as html_mod
from pathlib import Path
from datetime import date


def _esc(s):
    return html_mod.escape(str(s))


def _fmt_rows(n):
    if not n or n == '?':
        return '?'
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


def build_html(inventory: dict, dag: dict | None, model_name: str) -> str:
    tables = inventory["tables"]
    relationships = inventory["relationships"]
    today = date.today().isoformat()

    # Build table info cards data
    table_cards = []
    for t in tables:
        cols = len(t.get("columns", []))
        calc_cols = len(t.get("calculated_columns", []))
        measures = len(t.get("measures", []))
        rows = t.get("sf_row_count", None)
        is_fact = t["name"].lower().startswith("fact")
        hierarchies = []
        for h in t.get("hierarchies", []):
            levels = sorted(h.get("levels", []), key=lambda l: l.get("ordinal", 0))
            level_names = [l.get("column", "?") for l in levels]
            hierarchies.append({"name": h.get("name", ""), "levels": level_names})

        table_cards.append({
            "name": t["name"],
            "cols": cols,
            "calc_cols": calc_cols,
            "measures": measures,
            "rows": rows,
            "rows_fmt": _fmt_rows(rows),
            "is_fact": is_fact,
            "hierarchies": hierarchies,
        })

    # Build relationship data
    rels = []
    for r in relationships:
        rels.append({
            "from_table": r["from_table"],
            "from_col": r["from_column"],
            "to_table": r["to_table"],
            "to_col": r["to_column"],
            "active": r.get("is_active", True),
            "cardinality": r.get("cardinality", "many_to_one"),
        })

    # DAG stats
    dag_stats = None
    if dag:
        from collections import Counter
        type_counts = Counter(n["type"] for n in dag["nodes"].values())
        dag_stats = {
            "physical": type_counts.get("physical_column", 0),
            "renamed": type_counts.get("renamed_column", 0),
            "calc_cols": type_counts.get("calculated_column", 0),
            "measures": type_counts.get("measure", 0),
            "edges": len(dag["edges"]),
            "cross_table": sum(1 for e in dag["edges"] if e.get("reason") == "cross_table_ref"),
        }

    total_rows = sum(t.get("sf_row_count", 0) or 0 for t in tables)
    total_measures = sum(len(t.get("measures", [])) for t in tables)
    total_calc = sum(len(t.get("calculated_columns", [])) for t in tables)

    # Table cards JSON for JS
    cards_json = json.dumps(table_cards)
    rels_json = json.dumps(rels)

    # Build the relationship lines as Mermaid-like text for the diagram
    # We'll render it as pure HTML/SVG instead

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="snowflake-source" content="cortex-agent-authored" />
  <title>Table Relationships &mdash; {_esc(model_name)}</title>
  <script type="application/json" id="snowflake-report-metadata">
  {{
    "generated": "{today}",
    "intent": "Table relationship diagram and migration summary for SSAS tabular model",
    "dataSources": [{{"type": "file", "path": "ssas_inventory.json"}}, {{"type": "file", "path": "dax_dag.json"}}]
  }}
  </script>
  <style>
    :root {{ color-scheme: light dark; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, system-ui, 'Segoe UI', sans-serif;
      color: light-dark(#1e293b, #e2e8f0);
      background: light-dark(#f8fafc, #0f172a);
      padding: clamp(16px, 4vw, 32px);
      max-width: 1200px; margin: 0 auto;
    }}
    h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 4px; }}
    .subtitle {{ font-size: 0.85rem; color: light-dark(#64748b, #94a3b8); margin-bottom: 20px; }}
    img, svg {{ max-width: 100%; height: auto; }}

    /* Summary cards */
    .summary {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px; margin-bottom: 28px;
    }}
    .stat-card {{
      background: light-dark(#fff, #1e293b);
      border: 1px solid light-dark(#e2e8f0, #334155);
      border-radius: 10px; padding: 14px 16px; text-align: center;
    }}
    .stat-card .val {{ font-size: 1.6rem; font-weight: 700; color: light-dark(#0f172a, #f1f5f9); }}
    .stat-card .lbl {{ font-size: 0.75rem; color: light-dark(#64748b, #94a3b8); margin-top: 2px; }}

    /* ER Diagram area */
    .er-section {{ margin-bottom: 32px; }}
    .er-section h2 {{ font-size: 1.15rem; font-weight: 600; margin-bottom: 14px; }}
    .er-container {{
      background: light-dark(#fff, #1e293b);
      border: 1px solid light-dark(#e2e8f0, #334155);
      border-radius: 12px; padding: 24px;
      overflow-x: auto;
    }}
    .er-svg {{ min-width: 700px; }}

    /* Table detail cards */
    .tables-section h2 {{ font-size: 1.15rem; font-weight: 600; margin-bottom: 14px; }}
    .table-grid {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 16px;
    }}
    .tcard {{
      background: light-dark(#fff, #1e293b);
      border: 1px solid light-dark(#e2e8f0, #334155);
      border-radius: 10px; overflow: hidden;
    }}
    .tcard-header {{
      padding: 12px 16px;
      font-weight: 700; font-size: 0.95rem;
      display: flex; align-items: center; justify-content: space-between;
    }}
    .tcard-header.fact {{ background: light-dark(#dbeafe, #1e3a5f); }}
    .tcard-header.dim {{ background: light-dark(#f0fdf4, #14332b); }}
    .tcard-badge {{
      font-size: 0.65rem; font-weight: 600; text-transform: uppercase;
      padding: 2px 8px; border-radius: 4px;
      background: light-dark(#e2e8f0, #334155);
    }}
    .tcard-body {{ padding: 12px 16px; }}
    .tcard-row {{
      display: flex; justify-content: space-between;
      padding: 3px 0; font-size: 0.82rem;
      border-bottom: 1px solid light-dark(#f1f5f9, #0f172a);
    }}
    .tcard-row:last-child {{ border-bottom: none; }}
    .tcard-row .k {{ color: light-dark(#64748b, #94a3b8); }}
    .tcard-row .v {{ font-weight: 600; }}
    .hier {{ margin-top: 8px; }}
    .hier-title {{ font-size: 0.75rem; font-weight: 600; color: light-dark(#64748b, #94a3b8); margin-bottom: 4px; }}
    .hier-item {{
      font-size: 0.78rem; padding: 3px 0;
      color: light-dark(#475569, #cbd5e1);
    }}
    .hier-item span {{ color: light-dark(#94a3b8, #64748b); }}

    /* Relationship table */
    .rel-section {{ margin-top: 28px; }}
    .rel-section h2 {{ font-size: 1.15rem; font-weight: 600; margin-bottom: 14px; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.82rem; }}
    th, td {{ border: 1px solid light-dark(#e2e8f0, #334155); padding: 8px 12px; text-align: left; }}
    th {{ background: light-dark(#f8fafc, #0f172a); font-weight: 600; font-size: 0.78rem;
         text-transform: uppercase; color: light-dark(#64748b, #94a3b8); }}
    .inactive {{ opacity: 0.5; font-style: italic; }}
    .badge-active {{ color: #16a34a; font-weight: 600; }}
    .badge-inactive {{ color: light-dark(#94a3b8, #64748b); }}
  </style>
</head>
<body>

  <h1>Table Relationships &mdash; {_esc(model_name)}</h1>
  <div class="subtitle">{len(tables)} tables &middot; {len(relationships)} relationships &middot; {total_measures} measures &middot; {total_calc} calculated columns &middot; {_fmt_rows(total_rows)} total rows</div>

  <div class="summary">
    <div class="stat-card"><div class="val">{len(tables)}</div><div class="lbl">Tables</div></div>
    <div class="stat-card"><div class="val">{len(relationships)}</div><div class="lbl">Relationships</div></div>
    <div class="stat-card"><div class="val">{total_measures}</div><div class="lbl">Measures</div></div>
    <div class="stat-card"><div class="val">{total_calc}</div><div class="lbl">Calc Columns</div></div>
    <div class="stat-card"><div class="val">{_fmt_rows(total_rows)}</div><div class="lbl">Total Rows</div></div>
    <div class="stat-card"><div class="val">{sum(1 for r in rels if not r["active"])}</div><div class="lbl">Inactive Joins</div></div>
  </div>

  <div class="er-section">
    <h2>Entity Relationship Diagram</h2>
    <div class="er-container" id="erDiagram"></div>
  </div>

  <div class="tables-section">
    <h2>Table Details</h2>
    <div class="table-grid" id="tableGrid"></div>
  </div>

  <div class="rel-section">
    <h2>Join Details</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>From Table</th><th>Column</th><th>To Table</th><th>Column</th><th>Cardinality</th><th>Status</th></tr>
        </thead>
        <tbody id="relBody"></tbody>
      </table>
    </div>
  </div>

  <script>
  (function() {{
    var CARDS = {cards_json};
    var RELS = {rels_json};

    // --- Relationship table ---
    var rb = document.getElementById('relBody');
    RELS.forEach(function(r) {{
      var cls = r.active ? '' : ' class="inactive"';
      var badge = r.active
        ? '<span class="badge-active">Active</span>'
        : '<span class="badge-inactive">Inactive</span>';
      rb.innerHTML += '<tr' + cls + '><td>' + r.from_table + '</td><td>' + r.from_col +
        '</td><td>' + r.to_table + '</td><td>' + r.to_col +
        '</td><td>' + r.cardinality.replace('_', ' ') +
        '</td><td>' + badge + '</td></tr>';
    }});

    // --- Table detail cards ---
    var tg = document.getElementById('tableGrid');
    CARDS.forEach(function(c) {{
      var hCls = c.is_fact ? 'fact' : 'dim';
      var badge = c.is_fact ? 'Fact' : 'Dimension';
      var html = '<div class="tcard">';
      html += '<div class="tcard-header ' + hCls + '">' + c.name +
              '<span class="tcard-badge">' + badge + '</span></div>';
      html += '<div class="tcard-body">';
      html += '<div class="tcard-row"><span class="k">Columns</span><span class="v">' + c.cols + '</span></div>';
      html += '<div class="tcard-row"><span class="k">Calculated Cols</span><span class="v">' + c.calc_cols + '</span></div>';
      html += '<div class="tcard-row"><span class="k">Measures</span><span class="v">' + c.measures + '</span></div>';
      html += '<div class="tcard-row"><span class="k">Rows</span><span class="v">' + c.rows_fmt + '</span></div>';
      if (c.hierarchies && c.hierarchies.length) {{
        html += '<div class="hier"><div class="hier-title">Hierarchies</div>';
        c.hierarchies.forEach(function(h) {{
          html += '<div class="hier-item"><b>' + h.name + ':</b> ' +
                  h.levels.map(function(l,i) {{
                    return (i < h.levels.length-1) ? l + ' <span>&rarr;</span> ' : l;
                  }}).join('') + '</div>';
        }});
        html += '</div>';
      }}
      html += '</div></div>';
      tg.innerHTML += html;
    }});

    // --- ER Diagram (SVG) ---
    var dk = window.matchMedia('(prefers-color-scheme: dark)').matches;
    var container = document.getElementById('erDiagram');
    var tableMap = {{}};
    CARDS.forEach(function(c) {{ tableMap[c.name] = c; }});

    // Layout: position tables in a star pattern with fact in center
    var factTables = CARDS.filter(function(c) {{ return c.is_fact; }});
    var dimTables = CARDS.filter(function(c) {{ return !c.is_fact; }});

    var svgW = Math.max(800, container.clientWidth - 48);
    var svgH = 500;
    var cx = svgW / 2, cy = svgH / 2;
    var rx = Math.min(300, svgW / 2 - 120), ry = 180;

    var positions = {{}};
    // Place fact tables in center
    factTables.forEach(function(t, i) {{
      positions[t.name] = {{ x: cx, y: cy }};
    }});
    // Place dims around
    dimTables.forEach(function(t, i) {{
      var angle = (2 * Math.PI * i / dimTables.length) - Math.PI / 2;
      positions[t.name] = {{
        x: cx + rx * Math.cos(angle),
        y: cy + ry * Math.sin(angle)
      }};
    }});

    var boxW = 180, boxH = 60;

    var svg = '<svg class="er-svg" width="' + svgW + '" height="' + svgH + '" xmlns="http://www.w3.org/2000/svg">';

    // Draw relationship lines
    RELS.forEach(function(r) {{
      var from = positions[r.from_table];
      var to = positions[r.to_table];
      if (!from || !to) return;
      var col = r.active ? (dk ? '#60a5fa' : '#3b82f6') : (dk ? '#475569' : '#cbd5e1');
      var dash = r.active ? '' : ' stroke-dasharray="6,4"';
      svg += '<line x1="' + from.x + '" y1="' + from.y + '" x2="' + to.x + '" y2="' + to.y +
             '" stroke="' + col + '" stroke-width="' + (r.active ? 2 : 1.5) + '"' + dash + '/>';

      // Label at midpoint
      var mx = (from.x + to.x) / 2, my = (from.y + to.y) / 2;
      var label = r.from_col + ' = ' + r.to_col;
      svg += '<rect x="' + (mx - 70) + '" y="' + (my - 9) + '" width="140" height="18" rx="4" ' +
             'fill="' + (dk ? '#0f172a' : '#fff') + '" fill-opacity="0.85"/>';
      svg += '<text x="' + mx + '" y="' + (my + 4) + '" text-anchor="middle" font-size="9" ' +
             'fill="' + (dk ? '#94a3b8' : '#64748b') + '">' + label + '</text>';
      if (!r.active) {{
        svg += '<text x="' + mx + '" y="' + (my + 15) + '" text-anchor="middle" font-size="8" ' +
               'fill="' + (dk ? '#64748b' : '#94a3b8') + '" font-style="italic">(inactive)</text>';
      }}
    }});

    // Draw table boxes
    Object.keys(positions).forEach(function(name) {{
      var p = positions[name];
      var c = tableMap[name];
      var isFact = c && c.is_fact;
      var fillBg = isFact
        ? (dk ? '#1e3a5f' : '#dbeafe')
        : (dk ? '#14332b' : '#f0fdf4');
      var borderCol = isFact
        ? (dk ? '#3b82f6' : '#60a5fa')
        : (dk ? '#22c55e' : '#4ade80');

      svg += '<rect x="' + (p.x - boxW/2) + '" y="' + (p.y - boxH/2) + '" width="' + boxW +
             '" height="' + boxH + '" rx="8" fill="' + fillBg + '" stroke="' + borderCol + '" stroke-width="2"/>';

      // Table name
      svg += '<text x="' + p.x + '" y="' + (p.y - 8) + '" text-anchor="middle" font-weight="700" font-size="13" ' +
             'fill="' + (dk ? '#f1f5f9' : '#0f172a') + '">' + name + '</text>';

      // Stats line
      var info = '';
      if (c) {{
        info = c.cols + ' cols';
        if (c.measures > 0) info += ' &middot; ' + c.measures + ' measures';
        info += ' &middot; ' + c.rows_fmt + ' rows';
      }}
      svg += '<text x="' + p.x + '" y="' + (p.y + 10) + '" text-anchor="middle" font-size="10" ' +
             'fill="' + (dk ? '#94a3b8' : '#64748b') + '">' + info + '</text>';

      // Badge
      var badge = isFact ? 'FACT' : 'DIM';
      var badgeCol = isFact ? (dk ? '#3b82f6' : '#2563eb') : (dk ? '#22c55e' : '#16a34a');
      svg += '<text x="' + (p.x + boxW/2 - 8) + '" y="' + (p.y - boxH/2 + 14) + '" text-anchor="end" font-size="8" ' +
             'font-weight="700" fill="' + badgeCol + '">' + badge + '</text>';
    }});

    svg += '</svg>';
    container.innerHTML = svg;
  }})();
  </script>

</body>
</html>'''


def main():
    parser = argparse.ArgumentParser(description="Generate table relationship HTML diagram")
    parser.add_argument("--dag", default=None, help="Path to dax_dag.json (optional, for stats)")
    parser.add_argument("--inventory", default=None, help="Path to ssas_inventory.json")
    parser.add_argument("--output", required=True, help="Output HTML file path")
    parser.add_argument("--model-name", default=None, help="Model name for the title")
    args = parser.parse_args()

    # Load inventory — try to find it from dag path if not provided
    inv_path = args.inventory
    if not inv_path and args.dag:
        candidate = Path(args.dag).parent / "ssas_inventory.json"
        if candidate.exists():
            inv_path = str(candidate)
    if not inv_path:
        print("ERROR: --inventory is required (or ssas_inventory.json must be next to --dag)")
        return

    with open(inv_path, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    dag = None
    if args.dag:
        with open(args.dag, "r", encoding="utf-8") as f:
            dag = json.load(f)

    model_name = args.model_name or inventory.get("model_name", "SSAS Model")
    html_content = build_html(inventory, dag, model_name)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Table relationship diagram generated:")
    print(f"  Tables:        {len(inventory['tables'])}")
    print(f"  Relationships: {len(inventory['relationships'])}")
    print(f"  Output:        {args.output}")


if __name__ == "__main__":
    main()
