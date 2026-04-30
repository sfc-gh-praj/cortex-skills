# Power BI Measures Strategy: Post-SSAS Migration

**Context:** This guide is for organisations migrating from SSAS Tabular to Snowflake where
Power BI Desktop is the primary BI development tool. It covers what to do with existing
measures, how to add new ones, and the long-term path to Snowflake as the single semantic
source of truth.

---

## The Core Challenge

In SSAS Tabular, measures are defined in the model — Power BI connects via Live Connection
and all measures are served by SSAS at query time. PBIX files contain no local measures;
they are thin shells of visuals pointing to SSAS.

When SSAS is retired, those measures need a new home. There are two phases to get there.

---

## Short-Term Plan: XMLA Patched Model on Power BI Premium

### Why this approach

Power BI Premium has an XMLA read/write endpoint that speaks the same Tabular Model
protocol as SSAS. This means the existing SSAS `.bim` or `.xmla` model can be deployed
directly to Power BI Service — with all measures, relationships, hierarchies, and KPIs
intact. No measure is rebuilt. No DAX is rewritten.

### What changes and what stays the same

| Object | Change required | Notes |
|---|---|---|
| Measures (DAX) | **None** | Travel as-is from .bim to PBI Dataset |
| Calculated columns | **None** | Travel as-is |
| Relationships | **None** | Travel as-is |
| Hierarchies | **None** | Travel as-is |
| KPIs | **None** | Travel as-is |
| Data source connection | **Yes** | SQL Server → Snowflake `powerbi` schema views |
| Table M-queries | **Yes** | Updated per-table to point to Snowflake views |
| PBIX connection string | **Yes** | `asazure://...` → `powerbi://...` (XMLA endpoint) |

### Step-by-step: migrating existing measures to Power BI Premium

**Step 1 — Patch the data source in the .bim**

Open the `.bim` or `.xmla` file in Tabular Editor. Update the data source:

```
Before: Provider=SQLNCLI11;Data Source=<SQL_SERVER>;...
After:  Snowflake connector pointing to ADVENTUREWORKSDW2022_SF.powerbi
```

For each table, update the M-query partition expression:

```m
// Before (SQL Server)
let
    Source = Sql.Database("<server>", "AdventureWorksDW"),
    dbo_DimDate = Source{[Schema="dbo",Item="DimDate"]}[Data]
in
    dbo_DimDate

// After (Snowflake)
let
    Source = Snowflake.Databases("<account>.snowflakecomputing.com"),
    DB = Source{[Name="ADVENTUREWORKSDW2022_SF"]}[Data],
    Schema = DB{[Name="powerbi"]}[Data],
    DimDate = Schema{[Name="DIMDATE"]}[Data]
in
    DimDate
```

The `powerbi` schema views created in Phase 4 of this migration use the exact same column
names as the SSAS model, so M-queries resolve without column mapping.

**Step 2 — Deploy to Power BI Premium via XMLA endpoint**

Using Tabular Editor CLI:

```bash
TabularEditor.exe "<patched_model.bim>" \
  -D "powerbi://api.powerbi.com/v1.0/myorg/<WorkspaceName>" \
  "<DatasetName>" \
  -O -C -P -R -M -E -W
```

Or using pbi-tools:

```bash
pbi-tools deploy <patched_model.bim> \
  --workspace "<WorkspaceName>" \
  --dataset "<DatasetName>"
```

This creates (or updates) a Power BI Service Dataset backed by Snowflake. All measures
are immediately available in the dataset.

**Step 3 — Update existing PBIX files**

Each PBIX currently connects to SSAS via Live Connection. Change the connection endpoint:

```
Before: asazure://<region>.asazure.windows.net/<server>/<database>
After:  powerbi://api.powerbi.com/v1.0/myorg/<WorkspaceName>  →  <DatasetName>
```

This can be done in Power BI Desktop (Transform Data → Data Source Settings) or scripted
using `pbi-tools extract` + connection JSON patch + `pbi-tools compile`.

After the change, all existing visuals, filters, and bookmarks continue to work. Measures
are available via drag-and-drop from the Fields pane — identical to the SSAS experience.

### Adding new measures (short-term workflow)

New measures are added to the Power BI Service Dataset:

| Tool | Workflow |
|---|---|
| **Tabular Editor** (recommended) | Connect Tabular Editor to the XMLA endpoint → add measure → save. Change is live immediately for all reports. |
| **Power BI Desktop** | Open Desktop → Connect to the Service Dataset → switch to "Editing" mode → add measure → publish. |
| **Power BI Service** | Use the in-browser model editor (Settings → Edit data model). |

In all cases, the measure lives in the Power BI Dataset, is backed by Snowflake data,
and is immediately available to all PBIX reports connected to that dataset.

### What this means for your development workflow

- **No change to how you build reports.** Open PBIX in Desktop, fields and measures are
  in the Fields pane, drag-and-drop works exactly as with SSAS.
- **Model development** shifts from SSAS/Visual Studio to Tabular Editor or Power BI Desktop
  connected to the XMLA endpoint.
- **Version control** of the model uses the `.bim` file committed to Git.
  Tabular Editor can save changes back to a local `.bim` for source control.

### Measures in this migration (Snowflake Semantic View)

As part of this migration, all measures are also translated to SQL and stored in the
Snowflake Semantic View (Phase 5). This serves a different purpose:

| Consumer | Measure location | Purpose |
|---|---|---|
| Power BI reports | Power BI Dataset (DAX) | Interactive reporting, drag-and-drop |
| Cortex Analyst | Snowflake Semantic View (SQL metrics) | Natural language queries, AI-powered analytics |
| Programmatic queries | Snowflake Semantic View | REST API, Snowflake Notebooks, data apps |

In the short term, measures exist in both places and must be kept in sync when new measures
are added. A measure catalogue (`measure_catalogue.csv` generated in Phase 7) provides a
side-by-side reference of each measure's DAX expression and its SQL equivalent.

---

## Long-Term Plan: Snowflake Semantic View as Single Source of Truth

### When this becomes the target state

The Snowflake Semantic View ↔ Power BI connector is currently in preview. When it reaches
**General Availability**, the architecture changes significantly:

```
Today (short-term)                    GA (long-term)
──────────────────                    ───────────────
Power BI Dataset (DAX measures)       Snowflake Semantic View (SQL metrics)
        │                                      │
        ▼                                      ▼
Power BI reports                      Power BI reports
Cortex Analyst (Semantic View)        Cortex Analyst
                                      Other BI tools (Sigma, Tableau, etc.)
```

When the connector reaches GA, Snowflake becomes the single semantic layer. Measures are
defined once — in Snowflake — and consumed by Power BI, Cortex Analyst, Notebooks, and
any other tool via a standard interface.

### What the migration from short-term to long-term looks like

**Step 1 — Validate Snowflake Semantic View measures match PBI Dataset measures**

Use the `measure_catalogue.csv` from Phase 7 to verify parity. Every measure in the PBI
Dataset should have a corresponding metric in the Snowflake Semantic View with equivalent
logic.

**Step 2 — Connect Power BI to Snowflake Semantic View**

Once the connector is GA:

```
Power BI Desktop → Get Data → Snowflake Semantic View
→ Select view: ADVENTUREWORKSDW2022_SF.powerbi.<semantic_view_name>
→ Metrics appear as draggable measures in the Fields pane
```

Power BI fetches metric definitions from Snowflake rather than executing its own DAX.
The Snowflake query engine evaluates the metric SQL.

**Step 3 — Retire the Power BI Dataset DAX measures**

Once reports are validated against the Snowflake Semantic View connector:

- Remove DAX measures from the Power BI Dataset (or archive the Dataset)
- All reports now use Snowflake Semantic View metrics
- New measures are added to the Snowflake Semantic View YAML only
- Cortex Analyst and Power BI share the same metric definitions

**Step 4 — Developer workflow for new measures (long-term)**

```yaml
# Add to ssas_semantic_view.yaml
metrics:
  - name: New Measure Name
    synonyms: ["alternative name"]
    description: "What this measure computes"
    expr: SUM(FACTINTERNETSALES.SALESAMOUNT) / COUNT(DISTINCT FACTINTERNETSALES.CUSTOMERKEY)
    data_type: NUMBER
    format_template: "$#,##0.00"
```

Deploy via:

```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/deploy_semantic_view.py \
  --yaml-file ./ssas_semantic_view.yaml \
  --target-schema ADVENTUREWORKSDW2022_SF.powerbi \
  --connection COCO_JK
```

Power BI picks up the new metric on next model refresh. No Desktop work required.

### Governance during the transition period

While maintaining measures in both places (short-term period), follow these rules:

1. **New measure requests** go through a single intake process that adds to BOTH:
   - The PBI Dataset (DAX in Tabular Editor)
   - The Snowflake Semantic View YAML (SQL equivalent)

2. **The `measure_catalogue.csv`** is the authoritative register — updated whenever a
   measure is added, modified, or retired.

3. **Breaking changes** (renaming a measure, changing its calculation) must be applied
   to both systems simultaneously to avoid report breakage.

4. **Deprecate measures** by first removing from the PBI Dataset, validating no reports
   break, then removing from the Semantic View.

---

## Summary

| Phase | Measure location | Add new measures via | Power BI experience |
|---|---|---|---|
| **Now (post-migration)** | Power BI Premium Dataset (DAX) + Snowflake Semantic View (SQL) | Tabular Editor connected to XMLA endpoint | Identical to SSAS — drag-and-drop in Desktop |
| **Transition (connector preview)** | Both, with Snowflake as lead | Snowflake YAML first, PBI synced | PBI uses Snowflake metrics for new reports |
| **Target state (connector GA)** | Snowflake Semantic View only | Snowflake YAML only | PBI consumes metrics directly from Snowflake |

The goal is **Snowflake as the single semantic source of truth** — one place to define,
govern, and version-control metrics, consumed by Power BI, Cortex Analyst, and any other
tool without duplication.

---

_Part of the ssas-tabular-migration skill. See SKILL.md for the full 9-phase migration workflow._
