#!/usr/bin/env python3
"""
generate_ssis_report.py — SSIS Migration Assessment Report Generator

Reads ETL.Elements.csv + ETL.Issues.csv from a SnowConvert run and generates:
  • ssis_assessment_report.html    — interactive HTML report
  • packages/package_<Name>.html   — per-package detail pages
  • etl_assessment_summary.md      — markdown summary for migration tracking
  • etl_assessment_analysis.json   — structured data for downstream tools

Requires Python 3.9+. No external packages needed.
HTML report loads Tailwind CSS and Chart.js from CDN.

Usage:
    python3 scripts/generate_ssis_report.py \\
        --elements /path/to/ETL.Elements.<timestamp>.csv \\
        --issues   /path/to/ETL.Issues.<timestamp>.csv \\
        --output   ./ssis_migration_review/
"""

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from html import escape
from pathlib import Path

# ===========================================================================
# Constants — Snowflake brand palette
# ===========================================================================

SF_DEEP_BLUE = "#003545"
SF_DARK_BLUE = "#11567F"
SF_BLUE      = "#29B5E8"
SF_ORANGE    = "#FF9F36"
SF_GREEN     = "#36C782"
SF_RED       = "#FF5B5B"
SF_GRAY      = "#9CA3AF"

# Effort in hours per component status
EFFORT_MAP: dict[str, float] = {
    "Success":      0.5,
    "Partial":      2.0,
    "NotSupported": 4.0,
    "N/A":          0.0,
}

# Subtypes that signal higher manual effort
COMPLEX_SUBTYPES: set[str] = {
    "Microsoft.ScriptTask",
    "Microsoft.ScriptComponent",
    "STOCK:FOREACHLOOP",
    "STOCK:FOREACHFILEENUMERATOR",
}

CLASSIFICATION_COLORS: dict[str, str] = {
    "Orchestration":  SF_BLUE,
    "Transformation": SF_DARK_BLUE,
    "Control Flow":   SF_DEEP_BLUE,
    "Hybrid":         SF_ORANGE,
}

COMPLEXITY_COLORS: dict[str, str] = {
    "Low":    SF_GREEN,
    "Medium": SF_ORANGE,
    "High":   SF_RED,
}

STATUS_CHART_COLORS = [SF_GREEN, SF_ORANGE, SF_RED, "#E5E7EB"]


# ===========================================================================
# Data loading
# ===========================================================================

def load_csv(path: str) -> list[dict]:
    """Load a CSV file, handling UTF-8 BOM."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


# ===========================================================================
# Analysis
# ===========================================================================

def classify_package(elems: list[dict]) -> str:
    subtypes   = {e["Subtype"] for e in elems}
    categories = {e["Category"] for e in elems}
    if "Microsoft.ExecutePackageTask" in subtypes:
        return "Orchestration"
    if "Data Flow" in categories and "Control Flow" in categories:
        return "Hybrid"
    if "Data Flow" in categories:
        return "Transformation"
    return "Control Flow"


def complexity_level(elems: list[dict]) -> str:
    ns      = sum(1 for e in elems if e["Status"] == "NotSupported")
    drivers = sum(1 for e in elems if e["Subtype"] in COMPLEX_SUBTYPES)
    ewis    = sum(int(e.get("EWI Count") or 0) for e in elems)
    if ns >= 3 or drivers >= 2 or ewis >= 8:
        return "High"
    if ns >= 1 or drivers >= 1 or ewis >= 3:
        return "Medium"
    return "Low"


def build_analysis(elements: list[dict], issues: list[dict]) -> dict:
    by_pkg    = defaultdict(list)
    issues_by = defaultdict(list)
    for e in elements:
        by_pkg[e["FileName"]].append(e)
    for i in issues:
        issues_by[i["ParentFileName"]].append(i)

    packages = []
    for pkg_file, elems in sorted(by_pkg.items()):
        sc    = Counter(e["Status"] for e in elems)
        denom = len(elems) - sc.get("N/A", 0)
        supp_pct = (
            round((sc.get("Success", 0) + sc.get("Partial", 0) * 0.5) / denom * 100, 1)
            if denom else 0.0
        )
        pkg_iss = issues_by.get(pkg_file, [])
        packages.append({
            "file":           pkg_file,
            "name":           Path(pkg_file).stem,
            "classification": classify_package(elems),
            "complexity":     complexity_level(elems),
            "total":          len(elems),
            "cf_count":       sum(1 for e in elems if e["Category"] == "Control Flow"),
            "df_count":       sum(1 for e in elems if e["Category"] == "Data Flow"),
            "success":        sc.get("Success", 0),
            "partial":        sc.get("Partial", 0),
            "not_supported":  sc.get("NotSupported", 0),
            "na":             sc.get("N/A", 0),
            "supported_pct":  supp_pct,
            "effort_hrs":     round(sum(EFFORT_MAP.get(e["Status"], 0) for e in elems), 1),
            "ewi_count":      sum(1 for i in pkg_iss if i["Code"].startswith("SSC-EWI")),
            "fdm_count":      sum(1 for i in pkg_iss if i["Code"].startswith("SSC-FDM")),
            "elements":       elems,
            "issues":         pkg_iss,
        })

    sc_total = Counter(e["Status"] for e in elements)
    denom    = len(elements) - sc_total.get("N/A", 0)
    return {
        "generated_at": datetime.now().isoformat(),
        "packages":     packages,
        "totals": {
            "packages":      len(packages),
            "components":    len(elements),
            "issues":        len(issues),
            "ewi":           sum(1 for i in issues if i["Code"].startswith("SSC-EWI")),
            "fdm":           sum(1 for i in issues if i["Code"].startswith("SSC-FDM")),
            "supported_pct": round(
                (sc_total.get("Success", 0) + sc_total.get("Partial", 0) * 0.5)
                / denom * 100, 1
            ) if denom else 0.0,
            "effort_hrs":    round(sum(EFFORT_MAP.get(e["Status"], 0) for e in elements), 1),
            "success":       sc_total.get("Success", 0),
            "partial":       sc_total.get("Partial", 0),
            "not_supported": sc_total.get("NotSupported", 0),
            "na":            sc_total.get("N/A", 0),
        },
        "all_issues": issues,
    }


# ===========================================================================
# HTML micro-helpers
# ===========================================================================

def badge(text: str, color: str) -> str:
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:9999px;font-size:.73rem;font-weight:600;white-space:nowrap;">'
        f"{escape(text)}</span>"
    )


def metric_card(label: str, value: str, sub: str = "", accent: str = SF_BLUE) -> str:
    sub_html = (
        f'<div style="font-size:.78rem;color:{SF_GRAY};margin-top:4px;">{sub}</div>'
        if sub else ""
    )
    return (
        f'<div style="background:#fff;border-radius:12px;padding:24px;'
        f'border-left:4px solid {accent};box-shadow:0 1px 3px rgba(0,0,0,.1);">'
        f'<div style="font-size:.73rem;color:{SF_GRAY};text-transform:uppercase;'
        f'letter-spacing:.06em;margin-bottom:4px;">{escape(label)}</div>'
        f'<div style="font-size:2.1rem;font-weight:700;color:{SF_DEEP_BLUE};">{value}</div>'
        f"{sub_html}</div>"
    )


def th(text: str) -> str:
    return (
        f'<th style="padding:10px 16px;text-align:left;font-size:.73rem;'
        f'text-transform:uppercase;letter-spacing:.06em;color:#fff;'
        f'background:{SF_DARK_BLUE};white-space:nowrap;">{escape(text)}</th>'
    )


def td(content: str, center: bool = False) -> str:
    align = "text-align:center;" if center else ""
    return (
        f'<td style="padding:10px 16px;border-bottom:1px solid #F3F4F6;'
        f'{align}font-size:.875rem;vertical-align:top;">{content}</td>'
    )


def section_title(title: str, sub: str = "") -> str:
    sub_html = (
        f'<p style="color:{SF_GRAY};margin-top:4px;font-size:.875rem;">{escape(sub)}</p>'
        if sub else ""
    )
    return (
        f'<div style="margin-bottom:24px;">'
        f'<h2 style="font-size:1.4rem;font-weight:700;color:{SF_DEEP_BLUE};margin:0;">'
        f'{escape(title)}</h2>{sub_html}</div>'
    )


# ===========================================================================
# Main HTML report
# ===========================================================================

def generate_main_html(data: dict) -> str:
    t    = data["totals"]
    pkgs = data["packages"]
    ts   = data["generated_at"][:19].replace("T", " ")

    # Metric cards
    supp_c = SF_GREEN if t["supported_pct"] >= 80 else (SF_ORANGE if t["supported_pct"] >= 60 else SF_RED)
    cards  = (
        metric_card("SSIS Packages",    str(t["packages"]),
                    f'{t["packages"]} packages analyzed',          SF_BLUE)
        + metric_card("Total Components", str(t["components"]),
                      "control flow + data flow",                   SF_DARK_BLUE)
        + metric_card("Supported",        f'{t["supported_pct"]}%',
                      f'{t["success"]} success · {t["partial"]} partial', supp_c)
        + metric_card("Est. Effort",      f'{t["effort_hrs"]}h',
                      f'{t["ewi"] + t["fdm"]} issues to resolve',  SF_ORANGE)
    )

    # Sidebar package links
    pkg_links = "".join(
        f'<li><a href="packages/package_{p["name"]}.html"'
        f' style="display:block;padding:7px 16px 7px 28px;font-size:.8rem;'
        f'color:#93C5FD;text-decoration:none;"'
        f' onmouseover="this.style.color=\'#29B5E8\'"'
        f' onmouseout="this.style.color=\'#93C5FD\'">'
        f'{escape(p["name"])}</a></li>'
        for p in pkgs
    )

    # Package summary rows
    pkg_rows = ""
    for p in pkgs:
        pct_c  = SF_GREEN if p["supported_pct"] >= 80 else (SF_ORANGE if p["supported_pct"] >= 60 else SF_RED)
        cls_c  = CLASSIFICATION_COLORS.get(p["classification"], SF_GRAY)
        cplx_c = COMPLEXITY_COLORS[p["complexity"]]
        pkg_rows += (
            "<tr>"
            + td(
                f'<span style="font-weight:600;color:{SF_DEEP_BLUE};">{escape(p["name"])}</span><br>'
                f'<span style="font-size:.73rem;color:{SF_GRAY};">{escape(p["file"])}</span>'
            )
            + td(badge(p["classification"], cls_c))
            + td(str(p["cf_count"]), center=True)
            + td(str(p["df_count"]), center=True)
            + td(f'<span style="font-weight:600;color:{pct_c};">{p["supported_pct"]}%</span>',
                 center=True)
            + td(f'<span style="font-weight:600;">{p["effort_hrs"]}h</span>', center=True)
            + td(
                f'<span style="color:{SF_RED if p["ewi_count"] > 0 else SF_GREEN};'
                f'font-weight:600;">{p["ewi_count"]}</span>',
                center=True,
            )
            + td(
                f'<span style="color:{SF_ORANGE if p["fdm_count"] > 0 else SF_GREEN};'
                f'font-weight:600;">{p["fdm_count"]}</span>',
                center=True,
            )
            + td(badge(p["complexity"], cplx_c))
            + td(
                f'<a href="packages/package_{p["name"]}.html"'
                f' style="color:{SF_BLUE};text-decoration:none;font-weight:500;">'
                f'Details &rarr;</a>'
            )
            + "</tr>\n"
        )

    # Issues rows
    issue_rows = ""
    for i in data["all_issues"]:
        is_ewi = i["Code"].startswith("SSC-EWI")
        desc   = i["Description"]
        issue_rows += (
            f'<tr class="issue-row" data-type="{"EWI" if is_ewi else "FDM"}">'
            + td(badge("EWI", SF_RED) if is_ewi else badge("FDM", SF_ORANGE))
            + td(
                f'<code style="font-size:.78rem;background:#F3F4F6;'
                f'padding:2px 6px;border-radius:4px;">{escape(i["Code"])}</code>'
            )
            + td(f'<strong style="font-size:.85rem;">{escape(i["Name"])}</strong>')
            + td(
                f'<span style="font-size:.78rem;color:{SF_GRAY};">'
                f'{escape(Path(i["ParentFileName"]).stem)}</span>'
            )
            + td(
                f'<span style="font-size:.8rem;">'
                f'{escape(desc[:160])}{"..." if len(desc) > 160 else ""}</span>'
            )
            + "</tr>\n"
        )
    if not issue_rows:
        issue_rows = (
            f'<tr><td colspan="5" style="padding:32px;text-align:center;'
            f'color:{SF_GRAY};">No issues found.</td></tr>'
        )

    # Not-supported rows
    ns_rows = ""
    all_ns = [e for p in pkgs for e in p["elements"] if e["Status"] == "NotSupported"]
    for e in all_ns:
        cname = e["FullName"].split("\\")[-1] if "\\" in e["FullName"] else e["FullName"]
        ns_rows += (
            "<tr>"
            + td(f'<span style="font-size:.8rem;color:{SF_GRAY};">'
                 f'{escape(Path(e["FileName"]).stem)}</span>')
            + td(f'<code style="font-size:.8rem;">{escape(e["Subtype"])}</code>')
            + td(escape(cname))
            + td(badge(e["Category"],
                       SF_DARK_BLUE if e["Category"] == "Control Flow" else SF_BLUE))
            + td(f'<span style="font-size:.73rem;color:{SF_GRAY};">'
                 f'{escape(e.get("EWIs", "") or "—")}</span>')
            + "</tr>\n"
        )
    if not ns_rows:
        ns_rows = (
            f'<tr><td colspan="5" style="padding:32px;text-align:center;'
            f'color:{SF_GRAY};">No unsupported components found.</td></tr>'
        )

    # Classification chips
    class_chips = " ".join(
        badge(f'{cnt}\u00d7 {cls}', CLASSIFICATION_COLORS.get(cls, SF_GRAY))
        for cls, cnt in Counter(p["classification"] for p in pkgs).items()
    )

    # Readiness bar flex values (guard against 0)
    bar_success = t["success"] or 0.001
    bar_partial = t["partial"] or 0.001
    bar_ns      = t["not_supported"] or 0.001
    bar_na      = t["na"] or 0.001

    # Chart data (JSON embedded in <script>)
    chart_json = json.dumps({
        "status": {
            "labels": ["Success", "Partial", "Not Supported", "N/A"],
            "data":   [t["success"], t["partial"], t["not_supported"], t["na"]],
            "colors": STATUS_CHART_COLORS,
        },
        "pkg": {
            "labels":  [p["name"] for p in pkgs],
            "success": [p["success"] for p in pkgs],
            "partial": [p["partial"] for p in pkgs],
            "ns":      [p["not_supported"] for p in pkgs],
            "na":      [p["na"] for p in pkgs],
        },
    })

    legend_items = "".join(
        f'<span style="font-size:.8rem;display:flex;align-items:center;gap:6px;">'
        f'<span style="width:10px;height:10px;background:{c};'
        f'border-radius:3px;display:inline-block;"></span>{lbl} ({cnt})</span>'
        for lbl, cnt, c in [
            ("Success",       t["success"],       SF_GREEN),
            ("Partial",       t["partial"],       SF_ORANGE),
            ("Not Supported", t["not_supported"], SF_RED),
            ("N/A",           t["na"],            "#E5E7EB"),
        ]
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SSIS Migration Assessment Report</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; }}
    .nav-link {{ display:block; padding:10px 16px; color:#CBD5E1; text-decoration:none;
                 font-size:.875rem; border-left:3px solid transparent; transition:all .15s; }}
    .nav-link:hover {{ color:{SF_BLUE}; border-left-color:{SF_BLUE}; background:rgba(41,181,232,.08); }}
    table {{ border-collapse:collapse; width:100%; }}
    tbody tr:hover {{ background:#F8FAFC; }}
  </style>
</head>
<body style="background:#F1F5F9;">

  <!-- Header -->
  <header style="background:{SF_DEEP_BLUE};color:#fff;padding:0 32px;height:64px;
                 display:flex;align-items:center;justify-content:space-between;
                 position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.3);">
    <div style="display:flex;align-items:center;gap:14px;">
      <svg width="32" height="32" viewBox="0 0 48 48" fill="none">
        <circle cx="24" cy="24" r="22" fill="{SF_BLUE}" opacity=".15"/>
        <path d="M24 8l-4 12H8l10 7-4 12 10-7 10 7-4-12 10-7H28z" fill="{SF_BLUE}"/>
      </svg>
      <div>
        <div style="font-weight:700;font-size:1.05rem;letter-spacing:-.02em;">
          SSIS Migration Assessment
        </div>
        <div style="font-size:.72rem;color:{SF_BLUE};opacity:.9;margin-top:1px;">
          Powered by Snowflake SnowConvert
        </div>
      </div>
    </div>
    <div style="font-size:.75rem;color:#94A3B8;">Generated {ts}</div>
  </header>

  <div style="display:flex;">
    <!-- Sidebar -->
    <nav style="width:232px;min-height:calc(100vh - 64px);background:{SF_DARK_BLUE};
                position:sticky;top:64px;height:calc(100vh - 64px);overflow-y:auto;flex-shrink:0;">
      <div style="padding:18px 16px 6px;">
        <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.1em;
                    color:#475569;font-weight:700;">Report Sections</div>
      </div>
      <ul style="list-style:none;padding:0;margin:0;">
        <li><a href="#executive-summary"   class="nav-link">Executive Summary</a></li>
        <li><a href="#component-breakdown" class="nav-link">Component Breakdown</a></li>
        <li><a href="#package-summary"     class="nav-link">Package Summary</a></li>
        <li><a href="#issues"              class="nav-link">EWI / FDM Issues</a></li>
        <li><a href="#not-supported"       class="nav-link">Not Supported</a></li>
      </ul>
      <div style="padding:18px 16px 6px;margin-top:6px;border-top:1px solid rgba(255,255,255,.08);">
        <div style="font-size:.68rem;text-transform:uppercase;letter-spacing:.1em;
                    color:#475569;font-weight:700;">Package Detail</div>
      </div>
      <ul style="list-style:none;padding:0;margin:0;">{pkg_links}</ul>
    </nav>

    <!-- Main content -->
    <main style="flex:1;padding:32px;min-width:0;">

      <!-- ============================================================
           Section 1: Executive Summary
      ============================================================ -->
      <section id="executive-summary" style="margin-bottom:48px;scroll-margin-top:80px;">
        {section_title("Executive Summary", "SSIS package migration readiness overview")}

        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-bottom:28px;">
          {cards}
        </div>

        <!-- Readiness bar -->
        <div style="background:#fff;border-radius:12px;padding:24px;
                    box-shadow:0 1px 3px rgba(0,0,0,.1);margin-bottom:20px;">
          <div style="font-size:.875rem;font-weight:600;color:{SF_DEEP_BLUE};margin-bottom:14px;">
            Overall Migration Readiness
          </div>
          <div style="display:flex;gap:2px;border-radius:8px;overflow:hidden;height:20px;margin-bottom:12px;">
            <div style="background:{SF_GREEN};flex:{bar_success};" title="Success: {t['success']}"></div>
            <div style="background:{SF_ORANGE};flex:{bar_partial};" title="Partial: {t['partial']}"></div>
            <div style="background:{SF_RED};flex:{bar_ns};" title="Not Supported: {t['not_supported']}"></div>
            <div style="background:#E5E7EB;flex:{bar_na};" title="N/A: {t['na']}"></div>
          </div>
          <div style="display:flex;gap:18px;flex-wrap:wrap;">{legend_items}</div>
        </div>

        <div style="display:flex;gap:8px;flex-wrap:wrap;">{class_chips}</div>
      </section>

      <!-- ============================================================
           Section 2: Component Breakdown
      ============================================================ -->
      <section id="component-breakdown" style="margin-bottom:48px;scroll-margin-top:80px;">
        {section_title("Component Breakdown", "Status distribution across packages")}
        <div style="display:grid;grid-template-columns:1fr 2fr;gap:20px;">
          <div style="background:#fff;border-radius:12px;padding:24px;
                      box-shadow:0 1px 3px rgba(0,0,0,.1);">
            <div style="font-size:.875rem;font-weight:600;color:{SF_DEEP_BLUE};margin-bottom:14px;">
              Status Distribution
            </div>
            <canvas id="statusDonut"></canvas>
          </div>
          <div style="background:#fff;border-radius:12px;padding:24px;
                      box-shadow:0 1px 3px rgba(0,0,0,.1);">
            <div style="font-size:.875rem;font-weight:600;color:{SF_DEEP_BLUE};margin-bottom:14px;">
              Components per Package
            </div>
            <canvas id="pkgBar"></canvas>
          </div>
        </div>
      </section>

      <!-- ============================================================
           Section 3: Package Summary
      ============================================================ -->
      <section id="package-summary" style="margin-bottom:48px;scroll-margin-top:80px;">
        {section_title("Package Summary", f"{t['packages']} SSIS packages analyzed")}
        <div style="background:#fff;border-radius:12px;overflow:hidden;
                    box-shadow:0 1px 3px rgba(0,0,0,.1);">
          <table>
            <thead><tr>
              {th("Package")}{th("Classification")}{th("CF")}{th("DF")}
              {th("Supported %")}{th("Effort")}{th("EWI")}{th("FDM")}
              {th("Complexity")}{th("")}
            </tr></thead>
            <tbody>{pkg_rows}</tbody>
          </table>
        </div>
      </section>

      <!-- ============================================================
           Section 4: EWI / FDM Issues
      ============================================================ -->
      <section id="issues" style="margin-bottom:48px;scroll-margin-top:80px;">
        {section_title("EWI / FDM Issues",
                       f"{t['ewi']} EWIs and {t['fdm']} FDMs require manual review")}
        <div style="display:flex;gap:8px;margin-bottom:14px;">
          <button id="btn-all" onclick="filterIssues('all')"
            style="padding:5px 14px;border-radius:6px;border:none;cursor:pointer;
                   font-size:.8rem;font-weight:600;background:{SF_DARK_BLUE};color:#fff;">
            All ({t['ewi'] + t['fdm']})</button>
          <button id="btn-EWI" onclick="filterIssues('EWI')"
            style="padding:5px 14px;border-radius:6px;border:1px solid #E5E7EB;cursor:pointer;
                   font-size:.8rem;font-weight:600;background:#fff;color:{SF_DEEP_BLUE};">
            EWI ({t['ewi']})</button>
          <button id="btn-FDM" onclick="filterIssues('FDM')"
            style="padding:5px 14px;border-radius:6px;border:1px solid #E5E7EB;cursor:pointer;
                   font-size:.8rem;font-weight:600;background:#fff;color:{SF_DEEP_BLUE};">
            FDM ({t['fdm']})</button>
        </div>
        <div style="background:#fff;border-radius:12px;overflow:hidden;
                    box-shadow:0 1px 3px rgba(0,0,0,.1);">
          <table>
            <thead><tr>
              {th("Type")}{th("Code")}{th("Name")}{th("Package")}{th("Description")}
            </tr></thead>
            <tbody>{issue_rows}</tbody>
          </table>
        </div>
      </section>

      <!-- ============================================================
           Section 5: Not Supported
      ============================================================ -->
      <section id="not-supported" style="margin-bottom:48px;scroll-margin-top:80px;">
        {section_title("Not Supported Components",
                       f"{t['not_supported']} components require manual reimplementation in Snowflake")}
        <div style="background:#fff;border-radius:12px;overflow:hidden;
                    box-shadow:0 1px 3px rgba(0,0,0,.1);">
          <table>
            <thead><tr>
              {th("Package")}{th("Subtype")}{th("Component")}{th("Category")}{th("EWI Codes")}
            </tr></thead>
            <tbody>{ns_rows}</tbody>
          </table>
        </div>
      </section>

    </main>
  </div>

  <script>
  const D = {chart_json};

  // Donut — status distribution
  new Chart(document.getElementById('statusDonut'), {{
    type: 'doughnut',
    data: {{
      labels: D.status.labels,
      datasets: [{{
        data: D.status.data,
        backgroundColor: D.status.colors,
        borderWidth: 2,
        borderColor: '#fff',
        hoverOffset: 6,
      }}],
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ font: {{ size: 11 }}, padding: 12 }} }},
        tooltip: {{ callbacks: {{
          label: ctx => ` ${{ctx.label}}: ${{ctx.raw}} (${{Math.round(ctx.raw / D.status.data.reduce((a, b) => a + b, 0) * 100)}}%)`
        }} }},
      }},
    }},
  }});

  // Stacked bar — components per package
  new Chart(document.getElementById('pkgBar'), {{
    type: 'bar',
    data: {{
      labels: D.pkg.labels,
      datasets: [
        {{ label: 'Success',       data: D.pkg.success, backgroundColor: '{SF_GREEN}'  }},
        {{ label: 'Partial',       data: D.pkg.partial, backgroundColor: '{SF_ORANGE}' }},
        {{ label: 'Not Supported', data: D.pkg.ns,      backgroundColor: '{SF_RED}'    }},
        {{ label: 'N/A',           data: D.pkg.na,      backgroundColor: '#E5E7EB'     }},
      ],
    }},
    options: {{
      responsive: true,
      scales: {{
        x: {{ stacked: true, ticks: {{ font: {{ size: 11 }} }} }},
        y: {{ stacked: true, ticks: {{ stepSize: 1, font: {{ size: 11 }} }} }},
      }},
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ font: {{ size: 11 }}, padding: 10 }} }},
      }},
    }},
  }});

  // Issue type filter
  function filterIssues(type) {{
    document.querySelectorAll('.issue-row').forEach(r => {{
      r.style.display = (type === 'all' || r.dataset.type === type) ? '' : 'none';
    }});
    ['all', 'EWI', 'FDM'].forEach(t => {{
      const b = document.getElementById('btn-' + t);
      if (b) {{
        b.style.background = (t === type) ? '{SF_DARK_BLUE}' : '#fff';
        b.style.color      = (t === type) ? '#fff'           : '{SF_DEEP_BLUE}';
        b.style.border     = (t === type) ? 'none'           : '1px solid #E5E7EB';
      }}
    }});
  }}

  // Smooth scroll for sidebar anchors
  document.querySelectorAll('a[href^="#"]').forEach(a => {{
    a.addEventListener('click', e => {{
      const el = document.getElementById(a.getAttribute('href').slice(1));
      if (el) {{ e.preventDefault(); el.scrollIntoView({{ behavior: 'smooth', block: 'start' }}); }}
    }});
  }});
  </script>

</body>
</html>"""


# ===========================================================================
# Per-package detail page
# ===========================================================================

def generate_package_html(pkg: dict) -> str:
    ts     = datetime.now().strftime("%Y-%m-%d %H:%M")
    pct_c  = SF_GREEN if pkg["supported_pct"] >= 80 else (SF_ORANGE if pkg["supported_pct"] >= 60 else SF_RED)
    cls_c  = CLASSIFICATION_COLORS.get(pkg["classification"], SF_GRAY)
    cplx_c = COMPLEXITY_COLORS[pkg["complexity"]]

    # Component rows — sort NotSupported first, then Partial, then rest
    sort_order = {"NotSupported": 0, "Partial": 1, "Success": 2, "N/A": 3}
    sorted_elems = sorted(pkg["elements"], key=lambda e: sort_order.get(e["Status"], 9))

    comp_rows = ""
    for e in sorted_elems:
        s_color = {
            "Success": SF_GREEN, "Partial": SF_ORANGE,
            "NotSupported": SF_RED, "N/A": SF_GRAY,
        }.get(e["Status"], SF_GRAY)
        cname = e["FullName"].split("\\")[-1] if "\\" in e["FullName"] else e["FullName"]
        comp_rows += (
            "<tr>"
            + td(badge(e["Category"],
                       SF_DARK_BLUE if e["Category"] == "Control Flow" else SF_BLUE))
            + td(f'<code style="font-size:.78rem;">{escape(e["Subtype"])}</code>')
            + td(f'<span style="font-size:.85rem;">{escape(cname)}</span>')
            + td(badge(e["Status"], s_color))
            + td(f'<span style="font-size:.75rem;color:{SF_GRAY};">'
                 f'{escape(e.get("EWIs", "") or "—")}</span>')
            + td(f'<span style="font-size:.75rem;color:{SF_GRAY};">'
                 f'{escape(e.get("FDMs", "") or "—")}</span>')
            + "</tr>\n"
        )

    # Issue rows
    issue_rows = ""
    for i in pkg["issues"]:
        is_ewi = i["Code"].startswith("SSC-EWI")
        desc   = i["Description"]
        issue_rows += (
            "<tr>"
            + td(badge("EWI", SF_RED) if is_ewi else badge("FDM", SF_ORANGE))
            + td(f'<code style="font-size:.78rem;">{escape(i["Code"])}</code>')
            + td(f'<strong style="font-size:.85rem;">{escape(i["Name"])}</strong>')
            + td(f'<span style="font-size:.8rem;">'
                 f'{escape(desc[:200])}{"..." if len(desc) > 200 else ""}</span>')
            + "</tr>\n"
        )
    if not issue_rows:
        issue_rows = (
            f'<tr><td colspan="4" style="padding:24px;text-align:center;'
            f'color:{SF_GRAY};">No issues for this package.</td></tr>'
        )

    mini_cards = (
        metric_card("Components",  str(pkg["total"]),
                    f'CF: {pkg["cf_count"]} | DF: {pkg["df_count"]}', SF_BLUE)
        + metric_card("Supported",   f'{pkg["supported_pct"]}%',
                      f'{pkg["success"]} success · {pkg["partial"]} partial', pct_c)
        + metric_card("Est. Effort", f'{pkg["effort_hrs"]}h',
                      f'{pkg["ewi_count"]} EWI · {pkg["fdm_count"]} FDM', SF_ORANGE)
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape(pkg["name"])} — Package Detail</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin:0; }}
    table {{ border-collapse:collapse; width:100%; }}
    tbody tr:hover {{ background:#F8FAFC; }}
  </style>
</head>
<body style="background:#F1F5F9;">

  <header style="background:{SF_DEEP_BLUE};color:#fff;padding:0 32px;height:64px;
                 display:flex;align-items:center;justify-content:space-between;
                 position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.3);">
    <div style="display:flex;align-items:center;gap:14px;">
      <a href="../ssis_assessment_report.html"
         style="color:{SF_BLUE};text-decoration:none;font-size:.9rem;font-weight:500;">
        &larr; Back to Report
      </a>
      <span style="color:#334155;">|</span>
      <span style="font-weight:700;font-size:1rem;">{escape(pkg["name"])}</span>
    </div>
    <div style="font-size:.75rem;color:#94A3B8;">{ts}</div>
  </header>

  <main style="max-width:1100px;margin:0 auto;padding:32px;">

    <!-- Package header card -->
    <div style="background:#fff;border-radius:12px;padding:28px;margin-bottom:28px;
                box-shadow:0 1px 3px rgba(0,0,0,.1);border-top:4px solid {SF_BLUE};">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;
                  flex-wrap:wrap;gap:24px;">
        <div>
          <h1 style="font-size:1.6rem;font-weight:700;color:{SF_DEEP_BLUE};margin:0 0 6px;">
            {escape(pkg["name"])}
          </h1>
          <div style="font-size:.8rem;color:{SF_GRAY};margin-bottom:12px;">{escape(pkg["file"])}</div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;">
            {badge(pkg["classification"], cls_c)}
            {badge(pkg["complexity"] + " Complexity", cplx_c)}
          </div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(3,220px);gap:16px;">
          {mini_cards}
        </div>
      </div>
    </div>

    <!-- Component table -->
    <div style="margin-bottom:36px;">
      {section_title("Components", f"{pkg['total']} elements in this package")}
      <div style="background:#fff;border-radius:12px;overflow:hidden;
                  box-shadow:0 1px 3px rgba(0,0,0,.1);">
        <table>
          <thead><tr>
            {th("Category")}{th("Subtype")}{th("Component")}{th("Status")}
            {th("EWI Codes")}{th("FDM Codes")}
          </tr></thead>
          <tbody>{comp_rows}</tbody>
        </table>
      </div>
    </div>

    <!-- Issues table -->
    <div style="margin-bottom:36px;">
      {section_title("Issues", f"{pkg['ewi_count']} EWIs and {pkg['fdm_count']} FDMs")}
      <div style="background:#fff;border-radius:12px;overflow:hidden;
                  box-shadow:0 1px 3px rgba(0,0,0,.1);">
        <table>
          <thead><tr>
            {th("Type")}{th("Code")}{th("Name")}{th("Description")}
          </tr></thead>
          <tbody>{issue_rows}</tbody>
        </table>
      </div>
    </div>

  </main>
</body>
</html>"""


# ===========================================================================
# Markdown summary
# ===========================================================================

def generate_markdown_summary(data: dict) -> str:
    t    = data["totals"]
    pkgs = data["packages"]
    ts   = data["generated_at"][:19].replace("T", " ")

    lines = [
        "# ETL Assessment Summary",
        "",
        f"> Generated: {ts}  ",
        f"> Source: SnowConvert ETL.Elements.csv + ETL.Issues.csv",
        "",
        "## Key Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| SSIS Packages | {t['packages']} |",
        f"| Total Components | {t['components']} |",
        f"| Supported (Success + Partial) | {t['supported_pct']}% |",
        f"| Not Supported | {t['not_supported']} components |",
        f"| Estimated Effort | {t['effort_hrs']} hours |",
        f"| EWI Count | {t['ewi']} |",
        f"| FDM Count | {t['fdm']} |",
        "",
        "## Package Analysis",
        "",
        "| Package | Classification | CF | DF | Supported % | Effort (hrs) | EWI | FDM | Complexity |",
        "|---------|---------------|----|----|-------------|--------------|-----|-----|------------|",
    ]
    for p in pkgs:
        lines.append(
            f"| {p['name']} | {p['classification']} | {p['cf_count']} | {p['df_count']} "
            f"| {p['supported_pct']}% | {p['effort_hrs']} | {p['ewi_count']} "
            f"| {p['fdm_count']} | {p['complexity']} |"
        )

    lines += ["", "## Not Supported Components", ""]
    ns = [e for p in pkgs for e in p["elements"] if e["Status"] == "NotSupported"]
    if ns:
        for e in ns:
            cname = e["FullName"].split("\\")[-1] if "\\" in e["FullName"] else e["FullName"]
            lines.append(
                f"- **{Path(e['FileName']).stem}** — "
                f"`{e['Subtype']}` → {cname}"
            )
    else:
        lines.append("_No unsupported components found._")

    lines += ["", "## Issues (EWI / FDM)", ""]
    if data["all_issues"]:
        lines += [
            "| Code | Name | Package |",
            "|------|------|---------|",
        ]
        for i in data["all_issues"]:
            name_trunc = i["Name"][:80] + ("..." if len(i["Name"]) > 80 else "")
            lines.append(
                f"| `{i['Code']}` | {name_trunc} | {Path(i['ParentFileName']).stem} |"
            )
    else:
        lines.append("_No EWI or FDM issues found._")

    lines += [
        "",
        "---",
        "_Generated by `generate_ssis_report.py` from SnowConvert ETL CSV reports._",
    ]
    return "\n".join(lines)


# ===========================================================================
# Entry point
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate SSIS migration assessment report from SnowConvert CSVs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--elements", required=True,
                        help="Path to ETL.Elements.<timestamp>.csv")
    parser.add_argument("--issues",   required=True,
                        help="Path to ETL.Issues.<timestamp>.csv")
    parser.add_argument("--output",   required=True,
                        help="Output directory for generated files")
    args = parser.parse_args()

    elem_path   = Path(args.elements)
    issues_path = Path(args.issues)
    output_dir  = Path(args.output)

    if not elem_path.exists():
        print(f"ERROR: --elements file not found: {elem_path}", file=sys.stderr)
        sys.exit(1)

    elements = load_csv(str(elem_path))

    issues: list[dict] = []
    if issues_path.exists():
        issues = load_csv(str(issues_path))
    else:
        print(f"WARNING: --issues file not found at {issues_path}; proceeding without issues data.",
              file=sys.stderr)

    data = build_analysis(elements, issues)
    t    = data["totals"]

    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "packages").mkdir(exist_ok=True)

    # Write JSON (strip raw element rows to keep size manageable)
    json_safe = {
        "generated_at": data["generated_at"],
        "totals":       data["totals"],
        "packages": [
            {k: v for k, v in p.items() if k not in ("elements", "issues")}
            for p in data["packages"]
        ],
        "all_issues": data["all_issues"],
    }
    json_path = output_dir / "etl_assessment_analysis.json"
    json_path.write_text(json.dumps(json_safe, indent=2, ensure_ascii=False), encoding="utf-8")

    # Write main HTML
    html_path = output_dir / "ssis_assessment_report.html"
    html_path.write_text(generate_main_html(data), encoding="utf-8")

    # Write per-package pages
    for pkg in data["packages"]:
        pkg_path = output_dir / "packages" / f'package_{pkg["name"]}.html'
        pkg_path.write_text(generate_package_html(pkg), encoding="utf-8")

    # Write markdown summary
    md_path = output_dir / "etl_assessment_summary.md"
    md_path.write_text(generate_markdown_summary(data), encoding="utf-8")

    print(f"\nAssessment report generated")
    print(f"{'=' * 44}")
    print(f"  Packages:    {t['packages']}")
    print(f"  Components:  {t['components']}")
    print(f"  Supported:   {t['supported_pct']}%")
    print(f"  Est. Effort: {t['effort_hrs']}h")
    print(f"  Issues:      {t['ewi']} EWI + {t['fdm']} FDM")
    print(f"\nOutput:")
    print(f"  {html_path}")
    print(f"  {md_path}")
    print(f"  {json_path}")
    print(f"  {output_dir}/packages/ ({len(data['packages'])} package pages)")


if __name__ == "__main__":
    main()
