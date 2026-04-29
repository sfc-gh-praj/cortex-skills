---
name: ssas-tabular-migration
description: "Migrate SQL Server Analysis Services (SSAS) Tabular models to Snowflake. Use when: migrating SSAS, migrating Power BI datasets, converting model.bim, converting model.xmla, translating DAX measures to SQL, converting DAX to Snowflake, SSAS to Snowflake, tabular model migration, Power BI to Snowflake, convert DAX measures, migrate tabular model, SSAS migration, BIM file migration, XMLA file migration, DAX to SQL translation, DirectQuery migration, calculation groups, SELECTEDMEASURE, object-level security OLS, Interactive Table recommendation."
---

# SSAS Tabular → Snowflake Migration

Migrates an SSAS Tabular model (compat 1200+) to Snowflake in nine phases:
**Assess → Workload Score → Migration Plan → Schema DDL → DAX Translation → Deploy Semantic View → Power BI Views → Security → Validate**

Accepts either file format as input:
- **`model.bim`** — plain JSON, produced by Visual Studio / SSDT
- **`model.xmla`** — SOAP/XMLA envelope produced by SSMS "Script Database As → CREATE TO → File"

## Prerequisites

1. `model.bim` **or** `model.xmla` file accessible locally (compatibility level 1200+)
2. uv installed: `uv --version` — if missing: `brew install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`
3. Snowflake connection configured (default: `COCO_JK`)
4. Target Snowflake schema prepared: `DB_NAME.SCHEMA_NAME`

## Running Scripts

Always use absolute paths for `--project` and script path:
```bash
uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/<script>.py [args]
```

---

## Phase 1 — Assess

**Goal:** Parse the model file (`.bim` or `.xmla`) and produce a complete inventory with complexity score.

**Actions:**

1. Ask user for:
   ```
   1. Path to model file (.bim or .xmla)
   2. Target Snowflake schema (e.g. MY_DB.MY_SCHEMA)
   3. Snowflake connection name (default: COCO_JK)
   4. (Optional) Source Snowflake DB where tables are already migrated (e.g. ADVENTUREWORKSDW2022)
   5. (Optional) Source schema within that DB (default: DBO)
   ```
   If the user provides a source DB (question 4), pass `--source-db` and `--source-schema` to
   `parse_bim.py`. This enriches the inventory with actual row counts and sizes from Snowflake,
   changes Phase 4 DDL from CREATE TABLE to CREATE VIEW over the existing tables, and enables
   data-driven clustering/SOS decisions in Phase 2.

2. Run `parse_bim.py` — `--bim-path` accepts both `.bim` and `.xmla` files:
   ```bash
   # Without source DB (data not yet in Snowflake):
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/parse_bim.py \
     --bim-path <MODEL_PATH> --output ./ssas_inventory.json

   # With source DB (data already migrated to Snowflake):
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/parse_bim.py \
     --bim-path <MODEL_PATH> --output ./ssas_inventory.json \
     --source-db <SOURCE_DB> --source-schema <SOURCE_SCHEMA> --connection <CONNECTION>
   ```
   `<MODEL_PATH>` is the path to either the `.bim` or `.xmla` file — the script auto-detects the format.

   When `--source-db` is provided the inventory will include `sf_row_count`, `sf_bytes` per table
   and `data_already_in_snowflake: true`. It also performs **column reconciliation**:
   - `sf_column_map`: maps each SSAS column name → Snowflake physical column name (case-insensitive)
   - `sf_missing_columns`: SSAS columns with no Snowflake match (renamed in BIM via M/Power Query)
   - `sf_extra_columns`: Snowflake columns not exposed in the SSAS model (e.g. Spanish/French translations)
   - `calculated_column_resolution`: per calculated column, whether it resolves via `inline_sql` or `left_join` (for RELATED() cross-table lookups)

   Downstream phases use this enrichment data:
   - Phase 2: scoring uses actual row counts instead of heuristics
   - Phase 4: DDL generator emits `CREATE VIEW … AS SELECT FROM <source_db>.<source_schema>.<table>`
     instead of `CREATE TABLE` — no data movement required
   - Phase 5: semantic view uses Snowflake physical column names in `expr` fields (prevents `invalid identifier` errors)
   - Phase 5b: Power BI views use column mapping for correct aliases

3. Display full inventory summary including:
   - Model name + compatibility level
   - Table count, measure count, calculated columns, relationships
   - **Calculation groups** (compat 1500+ — if > 0: note N×M expansion in Phase 5)
   - **Calculated tables** (emitted as VIEWs, not storage tables)
   - **OLS present** (table/column-level security beyond RLS)
   - **Storage modes found** (import / directQuery / dual)
   - Roles, hierarchies, KPIs, perspectives, partitions
   - Complexity score: SIMPLE / MODERATE / COMPLEX

4. If compat level < 1200: script will exit with export instructions automatically.

5. Update status:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 1" --status completed \
     --notes "<table_count> tables, <measure_count> measures, complexity: <COMPLEXITY>"
   ```
   (Note: migration_status.md is created in Phase 3 — skip this call if Phase 3 hasn't run yet)

**⚠️ STOPPING POINT**: Show inventory. Confirm before proceeding.

---

## Phase 2 — Workload Assessment

**Goal:** Score each table for Interactive Table vs Regular Table suitability.

**Actions:**

1. Run `assess_deployment.py` (asks 4 workload questions interactively):
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/assess_deployment.py \
     --inventory ./ssas_inventory.json \
     --output ./deployment_assessment.json
   ```

2. Questions asked:
   - Concurrent users at peak (< 10 / 10–100 / 100–1000 / > 1000)
   - Query pattern (selective with date/region filters vs full-table scans)
   - Required response time (< 1s / 1–3s / 3+ s)
   - Refresh cadence (real-time / hourly / daily / weekly)

3. Display recommendation table:
   ```
   Table                 Score  Recommendation                CLUSTER BY
   ─────────────────────────────────────────────────────────────────────
   FactSales              85%   INTERACTIVE_TABLE             order_date, region  ⚠
   DimDate                75%   INTERACTIVE_TABLE             date_key            ⚠
   DimProduct             62%   REGULAR_TABLE_WITH_CLUSTERING product_key
   CalcTable               —    CALCULATED_VIEW               —
   ```

4. If any tables are flagged ⚠: display the cost warning block. Remind the user that
   Interactive Warehouse cannot auto-suspend before 24 hours.

5. Load `references/snowflake-equivalents.md` for Interactive Table syntax and constraints.

**⚠️ STOPPING POINT**: Show scores and cost warnings. Ask:
```
Proceed with these recommendations? Options:
1. Yes — generate migration plan with these recommendations
2. Adjust table types manually (specify which tables and target type)
```

---

## Phase 3 — Migration Plan

**Goal:** Write `MIGRATION_PLAN.md` and `migration_status.md` to the working directory.
User must approve the plan before any DDL is executed.

**Actions:**

1. Run `generate_migration_plan.py`:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_migration_plan.py \
     --inventory ./ssas_inventory.json \
     --assessment ./deployment_assessment.json \
     --target-schema <DB.SCHEMA> \
     --output ./MIGRATION_PLAN.md \
     --status ./migration_status.md
   ```

2. Two files are written to the current working directory:
   - `MIGRATION_PLAN.md` — full plan including table type assignments, CLUSTER BY rationale
     mapped to source DAX patterns, cost warnings, DAX translation scope, security scope,
     ballpark token estimates, risks, and an approval section
   - `migration_status.md` — phase-by-phase status tracker (Phases 1–3 pre-marked Completed;
     Phases 4–7 as Pending with ballpark token estimates)

3. Show the user:
   ```
   Migration plan written to: ./MIGRATION_PLAN.md
   Status tracker written to: ./migration_status.md
   ```

**⚠️ HARD STOPPING POINT — APPROVAL REQUIRED**

Do NOT proceed to Phase 4 until the user explicitly approves.

```
MIGRATION_PLAN.md has been written to your working directory.

Review it carefully — it includes:
  • Table-by-table type assignments (Interactive / Regular / View)
  • CLUSTER BY columns mapped to your source DAX query patterns
  • Cost warnings for Interactive Tables with low concurrency
  • Ballpark token estimates (planning only — not for cost invoicing)
  • Risks and items requiring manual attention

Reply 'approved' to proceed to Phase 4 (Schema DDL generation).
Reply 'stop' to abort.
```

If user replies 'stop': halt the migration and ask what they want to change.
If user replies 'approved': continue to Phase 4.

---

## Phase 4 — Schema DDL

**Goal:** Generate `CREATE TABLE` / `CREATE INTERACTIVE TABLE` / `CREATE VIEW` SQL.
**The DDL file must be reviewed before being deployed to Snowflake.**

**Actions:**

1. Mark Phase 4 in progress:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 4" --status in_progress
   ```

2. Run `generate_ddl.py` with assessment:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_ddl.py \
     --inventory ./ssas_inventory.json \
     --assessment ./deployment_assessment.json \
     --target-schema <DB.SCHEMA> \
     --output ./ssas_ddl.sql
   ```

3. Show counts: regular tables, interactive tables, calculated views, skipped hidden.

4. DDL footer includes:
   - `CREATE OR REPLACE INTERACTIVE WAREHOUSE bi_serving_wh` with all interactive tables attached
   - `CREATE OR REPLACE WAREHOUSE maintenance_wh` for refresh jobs
   - OLS REVOKE / masking policy comments

**⚠️ DDL VALIDATION STOPPING POINT — USER MUST REVIEW BEFORE DEPLOY**

```
ssas_ddl.sql has been written to your working directory.

IMPORTANT: Review the DDL file before any objects are created in Snowflake.
Check:
  • Table names and column names match your expectations
  • CLUSTER BY columns are correct for each table
  • Interactive Table TARGET_LAG values are appropriate
  • OLS REVOKE statements in the footer are correct

Reply one of:
  'deploy ddl'    → execute:  snow sql -f ./ssas_ddl.sql --connection <CONN>
  'review first'  → open the file; come back when ready to deploy
  'skip deploy'   → save file only, deploy manually later
```

Do NOT execute `snow sql` without this explicit confirmation.

5. If 'deploy ddl': execute the DDL file, display output, capture object names created.

6. Update status:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 4" --status completed \
     --objects "CREATE TABLE X, CREATE INTERACTIVE TABLE Y, ..." \
     --tokens 0 \
     --notes "<N> tables: <n_interactive> interactive, <n_regular> regular, <n_view> view(s)"
   ```

---

## Phase 5 — DAX Translation

**Goal:** Translate all measures, calculated columns, and calculation group items to SQL. Generate semantic view YAML.

**Actions:**

1. Mark Phase 5 in progress:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 5" --status in_progress
   ```

2. **Load** `references/dax-to-sql-patterns.md` for context.

3. Run `convert_dax.py`:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/convert_dax.py \
     --inventory ./ssas_inventory.json \
     --output ./ssas_measures_translated.json \
     --connection <CONNECTION>
   ```

   - ~25 regex patterns handle simple DAX automatically
   - `claude-sonnet-4-5` via Cortex handles: CALCULATE, FILTER, RELATED, ALL, time intelligence, RANKX, TOPN
   - **Calculation groups**: expands N items × M base measures → individual named metrics
   - Unresolvable expressions flagged `manual_review` with original DAX preserved

4. Show translation report:
   - Auto (pattern): N | LLM (Cortex): N | Manual review: N
   - Calculation group expansions: N (if applicable)
   - List all `manual_review` items with DAX

5. For each `manual_review` item: present the DAX and ask user for the SQL equivalent. Update `ssas_measures_translated.json`.

6. Run `generate_semantic_view.py`:
   ```bash
   # Generate YAML only:
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_semantic_view.py \
     --inventory ./ssas_inventory.json \
     --measures ./ssas_measures_translated.json \
     --target-schema <DB.SCHEMA> \
     --output ./ssas_semantic_view.yaml

   # Generate YAML and deploy to Snowflake in one step:
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_semantic_view.py \
     --inventory ./ssas_inventory.json \
     --measures ./ssas_measures_translated.json \
     --target-schema <DB.SCHEMA> \
     --output ./ssas_semantic_view.yaml \
     --deploy --connection <CONNECTION>
   ```
   The script uses `sf_column_map` from the enriched inventory to resolve Snowflake
   physical column names in `expr` fields, and skips RELATED() cross-table calculated
   columns (those are handled in Phase 5b Power BI views instead).

7. Re-run `generate_ddl.py` with `--measures-json` to embed translated calculated columns:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_ddl.py \
     --inventory ./ssas_inventory.json \
     --assessment ./deployment_assessment.json \
     --measures-json ./ssas_measures_translated.json \
     --target-schema <DB.SCHEMA> \
     --output ./ssas_ddl_with_views.sql
   ```

8. Update status (use actual token count from convert_dax.py output if available):
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 5" --status completed \
     --tokens <ACTUAL_OR_ESTIMATE> \
     --notes "auto: <N>, llm: <N>, manual_review: <N>, expansions: <N>"
   ```

**⚠️ STOPPING POINT**: Show translation summary and YAML preview. Ask:
```
1. Validate YAML: cortex reflect ./ssas_semantic_view.yaml
2. Deploy semantic view to Snowflake now
3. Generate Power BI zero-break views
4. Save files only
```

---

## Phase 5b — Deploy Semantic View

**Goal:** Create the semantic view object directly in Snowflake so it's available for Cortex Analyst queries.

**Actions:**

1. If not already deployed via `--deploy` flag in Phase 5, run `deploy_semantic_view.py`:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/deploy_semantic_view.py \
     --yaml-file ./ssas_semantic_view.yaml \
     --target-schema <DB.SCHEMA> \
     --connection <CONNECTION>
   ```
   This calls `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML()` to create the semantic view object.
   The function's third parameter is `dry_run`: `FALSE` creates the view (default), `TRUE` validates only.
   Use `--dry-run` flag to validate without creating.

2. Verify deployment:
   ```sql
   SHOW SEMANTIC VIEWS IN SCHEMA <DB.SCHEMA>;
   ```

3. Test with Cortex Analyst:
   ```bash
   cortex analyst query "show me total sales" --view <DB.SCHEMA.VIEW_NAME>
   ```

---

## Phase 5c — Power BI Migration Views

**Goal:** Generate Snowflake views that preserve exact SSAS table names and column aliases,
enabling existing Power BI reports to reconnect without modification.

**Actions:**

1. Run `generate_powerbi_views.py`:
   ```bash
   # Generate SQL file only:
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_powerbi_views.py \
     --inventory ./ssas_inventory.json \
     --measures ./ssas_measures_translated.json \
     --source-schema <DB.SOURCE_SCHEMA> \
     --target-schema <DB.PBI_SCHEMA> \
     --output ./powerbi_views.sql

   # Generate and deploy:
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_powerbi_views.py \
     --inventory ./ssas_inventory.json \
     --measures ./ssas_measures_translated.json \
     --source-schema <DB.SOURCE_SCHEMA> \
     --target-schema <DB.PBI_SCHEMA> \
     --output ./powerbi_views.sql \
     --deploy --connection <CONNECTION>
   ```

2. The views ensure:
   - **Exact SSAS table names** as view names (e.g. `DimDate`, not `VW_DIMDATE`)
   - Only columns exposed in the SSAS model (hidden / language-variant columns excluded)
   - Snowflake ALL-CAPS columns aliased back to SSAS mixed-case names
   - Same-table calculated columns as inline SQL expressions
   - Cross-table `RELATED()` calculated columns resolved via LEFT JOIN
   - Uses `sf_column_map` from enriched inventory for correct physical column references

3. Show the user the Power BI reconnection instructions:
   ```
   Power BI reports can now reconnect to Snowflake:
   1. Open .pbix file → Change data source → Snowflake DirectQuery
   2. Point to: <DB.PBI_SCHEMA>
   3. Rebuild hierarchies in Power BI Model view
   4. All columns and calculated columns will map automatically
   ```

**⚠️ STOPPING POINT**: Review generated views before deploying.

---

## Phase 6 — Security

**Goal:** Migrate both RLS (row-level) and OLS (object-level) security.

**Actions:**

1. Mark Phase 6 in progress:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 6" --status in_progress
   ```

### 6a — Row-Level Security (RLS)

For each role with `filter_expression`:
- Translate DAX filter → Snowflake Row Access Policy
- `USERNAME()` → `CURRENT_USER()`
- Mapping-table patterns → subquery-based policy

```sql
CREATE OR REPLACE ROW ACCESS POLICY rap_<table>_<role>
  AS (<col> VARCHAR) RETURNS BOOLEAN ->
    EXISTS (SELECT 1 FROM <mapping_table>
            WHERE username = CURRENT_USER() AND <col> = :1)
    OR IS_ROLE_IN_SESSION('SYSADMIN');

ALTER TABLE <schema>.<table>
  ADD ROW ACCESS POLICY rap_<table>_<role> ON (<col>);
```

### 6b — Object-Level Security (OLS) — if `has_ols: true`

**Load** `references/ssas-features-complete.md` section "Object-Level Security".

For table-level OLS (`metadata_permission: none`):
```sql
REVOKE SELECT ON <schema>.<table> FROM ROLE <role_name>_ROLE;
```

For column-level OLS:
- Option A (mask data): `CREATE MASKING POLICY ... ALTER TABLE ... SET MASKING POLICY`
- Option B (omit column): create a role-specific view without the restricted column

The DDL file (`ssas_ddl.sql`) already contains the generated REVOKE / masking policy comments.

2. Update status:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 6" --status completed \
     --objects "ROW ACCESS POLICY rap_X, REVOKE ON Y, ..." \
     --tokens <ESTIMATE> \
     --notes "RLS: <N> policies, OLS: <N> revokes"
   ```

**⚠️ STOPPING POINT**: Review `ssas_rls_policies.sql` and OLS section of DDL before executing.

---

## Phase 7 — Validate & Export

**Goal:** Confirm migration is complete, generate migration mapping report, and summarise all output files.

**Actions:**

1. Mark Phase 7 in progress:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 7" --status in_progress
   ```

2. Generate the migration mapping report:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/generate_migration_mapping.py \
     --inventory ./ssas_inventory.json \
     --measures ./ssas_measures_translated.json \
     --target-schema <DB.SCHEMA> \
     --pbi-schema <DB.PBI_SCHEMA> \
     --output ./MIGRATION_MAPPING.md
   ```
   This produces a detailed report covering: table mapping, column mapping per table,
   calculated columns, measures, relationships, hierarchies, manual review items,
   and a post-migration checklist.

3. Spot-check queries:
   ```sql
   SELECT COUNT(*) FROM <each migrated table>;
   SELECT * FROM <PBI_SCHEMA>.<each view> LIMIT 5;
   ```

4. For semantic view: `cortex analyst query "show me [a metric name]" --view=<DB.SCHEMA.VIEW>`

5. Update status:
   ```bash
   uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
     --phase "Phase 7" --status completed \
     --notes "All tables validated, semantic view deployed, PBI views created"
   ```

6. Present final migration report, list all output files, and show the final contents of `migration_status.md`.

---

## Workflow Decision Tree

```
Phase 1: ASSESS
  parse_bim.py → ssas_inventory.json (+ column reconciliation if --source-db)
  ⚠️ STOP: confirm inventory
  ↓
Phase 2: WORKLOAD ASSESSMENT
  assess_deployment.py → deployment_assessment.json
  (4 questions + BIM signal scoring → per-table recommendation)
  ⚠️ STOP: confirm recommendations + cost warnings
  ↓
Phase 3: MIGRATION PLAN
  generate_migration_plan.py → MIGRATION_PLAN.md + migration_status.md
  ⚠️ HARD STOP: user must reply 'approved' before Phase 4
  ↓ (only after 'approved')
Phase 4: SCHEMA DDL
  generate_ddl.py --assessment → ssas_ddl.sql
  ⚠️ STOP: user must reply 'deploy ddl' / 'review first' / 'skip deploy'
  update_migration_status.py → Phase 4 completed
  ↓
Phase 5: DAX TRANSLATION + SEMANTIC VIEW
  convert_dax.py → ssas_measures_translated.json
  generate_semantic_view.py [--deploy] → ssas_semantic_view.yaml [+ Snowflake object]
  generate_ddl.py --measures-json → ssas_ddl_with_views.sql
  update_migration_status.py → Phase 5 completed
  ⚠️ STOP: review translations + choose deploy / PBI views / save
  ↓
Phase 5b: DEPLOY SEMANTIC VIEW (if not done in Phase 5)
  deploy_semantic_view.py → Snowflake semantic view object
  SHOW SEMANTIC VIEWS to verify
  ↓
Phase 5c: POWER BI MIGRATION VIEWS
  generate_powerbi_views.py [--deploy] → powerbi_views.sql [+ Snowflake views]
  ⚠️ STOP: review views before deploying
  ↓
Phase 6: SECURITY
  6a: RLS → Row Access Policies
  6b: OLS → REVOKE + masking policies  (if has_ols)
  update_migration_status.py → Phase 6 completed
  ⚠️ STOP: review before executing
  ↓
Phase 7: VALIDATE & EXPORT
  generate_migration_mapping.py → MIGRATION_MAPPING.md
  Spot-check queries + Cortex Analyst test
  update_migration_status.py → Phase 7 completed
  Show final migration_status.md
```

## Output Files

| File | Generated by | Contents |
|---|---|---|
| `ssas_inventory.json` | `parse_bim.py` | Full parsed model inventory (+ `sf_column_map`, `calculated_column_resolution` when `--source-db` used) |
| `deployment_assessment.json` | `assess_deployment.py` | Per-table Interactive/Regular recommendation + scores |
| `MIGRATION_PLAN.md` | `generate_migration_plan.py` | Full migration plan — table types, CLUSTER BY rationale, token estimates, risks |
| `migration_status.md` | `generate_migration_plan.py` | Living phase-by-phase status tracker (updated each phase) |
| `ssas_ddl.sql` | `generate_ddl.py` | CREATE TABLE / INTERACTIVE TABLE / VIEW + OLS comments |
| `ssas_ddl_with_views.sql` | `generate_ddl.py --measures-json` | DDL + calculated column views with translated SQL |
| `ssas_measures_translated.json` | `convert_dax.py` | Per-expression DAX → SQL translation results |
| `ssas_semantic_view.yaml` | `generate_semantic_view.py` | Snowflake Semantic View definition |
| `powerbi_views.sql` | `generate_powerbi_views.py` | Power BI zero-break migration views (exact SSAS names, column aliases, JOIN resolution) |
| `MIGRATION_MAPPING.md` | `generate_migration_mapping.py` | Detailed SSAS-vs-Snowflake mapping report with post-migration checklist |

## Troubleshooting

**`parse_bim.py` fails with JSON error on a `.bim` file** → compat < 1200 (XML/ASSL format). Export via SSMS: right-click DB → Script → CREATE TO → File (`.xmla`), then pass the `.xmla` file to `--bim-path`.

**`parse_bim.py` fails on a `.xmla` file** → Two possible causes:
  1. *ASSL XML (compat < 1200)*: the `<Statement>` element contains XML, not JSON. Upgrade the model compat level to 1200+ in Visual Studio first.
  2. *Wrong export format*: ensure you used SSMS "Script Database As → CREATE TO → File" (SOAP envelope format). Tabular Editor `.xmla` exports may use a different wrapper — open the file and verify it contains a `<Statement>` element with `{` JSON inside.

**`convert_dax.py` LLM fails** → verify: `SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-5', 'test');`

**`cortex reflect` reports errors** → load `references/snowflake-equivalents.md` YAML section. Ensure `base_table` is fully qualified `DB.SCHEMA.TABLE`.

**Interactive Table CREATE fails** → must use a standard warehouse (not the interactive warehouse) for the CREATE command itself.

**Calculated column VIEW fails at query time** → column names may differ between the base table and the view definition. Check `quote_name()` output in DDL.

**`update_migration_status.py` reports "phase not found"** → phase name must match exactly the start of the cell, e.g. `--phase "Phase 4"` (not `"Phase 4 — Schema DDL"`).

**`SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` fails with syntax error** → ensure you use `CALL` (not `CREATE SEMANTIC VIEW`). The function takes 3 args: schema string, dollar-quoted YAML content, and boolean `dry_run` flag. Use `FALSE` to create (replaces if exists), `TRUE` to validate only. Example: `CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('DB.SCHEMA', $$yaml$$, FALSE)`.

**Semantic view YAML has `$$` in content** → `deploy_semantic_view.py` auto-falls back to single-quote escaping when `$$` appears in the YAML. If you see quoting errors, check for unescaped dollar signs in measure expressions.

**Column mismatch between SSAS and Snowflake** → run `parse_bim.py` with `--source-db` and `--source-schema` to trigger column reconciliation. Check `sf_missing_columns` and `sf_extra_columns` in the inventory JSON to identify gaps before generating views.

**Power BI views show wrong column names** → `generate_powerbi_views.py` uses `sf_column_map` from the enriched inventory. Re-run `parse_bim.py` with `--source-db` to refresh the mapping if tables were altered after initial parsing.

**RELATED() calculated columns missing from semantic view** → by design. Cross-table RELATED() columns are excluded from the semantic view YAML and instead materialized as LEFT JOINs in Power BI migration views. Check `powerbi_views.sql` for these columns.
