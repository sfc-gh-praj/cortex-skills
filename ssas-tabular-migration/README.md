# SSAS Tabular → Snowflake Migration Skill

A Cortex Code skill that automates the end-to-end migration of SQL Server Analysis Services (SSAS) Tabular models — including Power BI Premium semantic models — to Snowflake.

---

## Getting Started

### Step 1 — Extract your model file from SSAS

You need either a `.bim` or `.xmla` file from your SSAS Tabular database. **`.xmla` is recommended** — it is more portable and works with all compatibility levels ≥ 1200.

#### Export `.xmla` from SSMS (recommended)

1. Open **SQL Server Management Studio (SSMS)**
2. Connect to your **Analysis Services** instance
3. In Object Explorer, right-click your database → **Script Database As → CREATE To → File**
4. Save the file as `YourModel.xmla`

> The exported file will contain a SOAP/XMLA envelope with a `<Statement>` element wrapping the JSON model definition. `parse_bim.py` detects this format automatically.

#### Export `.bim` from Visual Studio / SSDT

1. Open your Tabular project in **Visual Studio** with the Analysis Services extension
2. Navigate to the project folder
3. Copy `Model.bim` from the project root — this is plain JSON and can be passed directly

> **Compatibility level requirement:** Both formats require **compatibility level 1200 or higher**. If your model is at 1100/1103 (XML-based), `parse_bim.py` will exit with upgrade instructions.

---

### Step 2 — Start the migration in Cortex Code

Paste the following prompt into Cortex Code to begin the full guided migration:

```
Migrate my SSAS Tabular model to Snowflake.

Model file: /path/to/YourModel.xmla        ← or .bim
Output folder: /path/to/output/            ← where artifacts will be saved
Target Snowflake schema: MY_DB.MY_SCHEMA   ← DB.SCHEMA to create objects in
Snowflake connection: MY_CONNECTION        ← your configured connection name

The source data is already in Snowflake at MY_SOURCE_DB.DBO   ← omit if not yet migrated
```

**Minimal version (fewest inputs):**

```
Migrate the SSAS Tabular model at /path/to/YourModel.xmla to Snowflake schema MY_DB.MY_SCHEMA using connection MY_CONNECTION.
```

Cortex Code will ask for any missing details before proceeding.

#### What Cortex Code does with this prompt

1. Loads the `ssas-tabular-migration` skill automatically
2. Checks for an existing `migration_status.md` — resumes from the last completed phase if one is found
3. Walks you through all seven phases with explicit stopping points before any DDL is deployed

---

## What This Skill Does

Walks you through seven phases to produce a complete, deployment-ready set of Snowflake SQL artifacts from a `model.bim` file:

| Phase | What happens |
|---|---|
| **1 — Assess** | Parses model.bim; inventories all objects; detects calculation groups, OLS, storage modes, perspectives |
| **2 — Workload Score** | Asks 4 questions about your workload + reads BIM structural signals to recommend Interactive Table vs Regular Table per table |
| **3 — Migration Plan** | Writes `MIGRATION_PLAN.md` with full table assignments, CLUSTER BY rationale mapped to source DAX patterns, token estimates, risks. Requires explicit user approval before proceeding |
| **4 — Schema DDL** | Generates `CREATE TABLE`, `CREATE INTERACTIVE TABLE`, or `CREATE VIEW` SQL — requires explicit user confirmation before deploying to Snowflake |
| **5 — DAX Translation** | Translates all DAX measures, calculated columns, and calculation group items to Snowflake SQL; generates a Semantic View YAML |
| **6 — Security** | Migrates row-level security (RLS) to Row Access Policies and object-level security (OLS) to REVOKE / masking policies |
| **7 — Validate** | Spot-check queries + final migration summary |

---

## Output Files

| File | Contents |
|---|---|
| `ssas_inventory.json` | Complete parsed model — tables, columns, measures, relationships, roles, calculation groups, perspectives |
| `deployment_assessment.json` | Per-table recommendation (Interactive / Regular / View), scores, CLUSTER BY columns, TARGET_LAG, cost warnings |
| `MIGRATION_PLAN.md` | Full migration plan — table type assignments, CLUSTER BY rationale, cost warnings, token estimates (ballpark), risks, approval gate |
| `migration_status.md` | Living phase-by-phase tracker — status, Snowflake objects created, ballpark token estimates per phase |
| `ssas_ddl.sql` | Full Snowflake DDL: CREATE TABLE / INTERACTIVE TABLE / VIEW + Interactive Warehouse + OLS comments |
| `ssas_ddl_with_views.sql` | Same DDL with calculated column views containing translated SQL expressions |
| `ssas_measures_translated.json` | Per-expression translation results: original DAX → SQL, method (pattern/llm/manual_review), notes |
| `ssas_semantic_view.yaml` | Snowflake Semantic View — tables, dimensions, facts, metrics (including N×M calc group expansions) |

---

## Phase-by-Phase Walkthrough

### Phase 1 — Assess (`parse_bim.py`)

**What it does:**

Reads your `model.bim` file and builds a complete inventory of every object in the model. This is the foundation for all subsequent phases — every other script reads this inventory rather than the raw BIM.

**What it extracts:**

- **Tables** — name, storage mode (import / directQuery / dual), whether it is a date table, whether it is a calculated table
- **Columns** — name, data type, whether it is calculated (has a DAX expression), whether it is the table's key column
- **Measures** — name, DAX expression, format string, KPI metadata (goal, status, trend)
- **Hierarchies** — name, levels in order (used as clustering candidates in Phase 3)
- **Partitions** — SQL query or M (Power Query) expression, partition name (used to detect date-range partitioning patterns)
- **Relationships** — from/to table and column, active/inactive flag, cross-filtering direction (one-way or both directions)
- **Roles** — name, RLS filter expressions per table (DAX), table-level OLS permissions, column-level OLS permissions
- **Calculation groups** (compat 1500+) — calc items, DAX expressions, ordinals
- **Perspectives** — model subsets that represent business domain views

**Complexity scoring:**
- **SIMPLE** — fewer than 5 tables, no calculation groups, no OLS, no DirectQuery
- **MODERATE** — 5–15 tables, or has one of: calculation groups, OLS, DirectQuery, bidirectional filters
- **COMPLEX** — 15+ tables, or has multiple of the above

Rejects compat < 1200 with export instructions.

---

### Phase 2 — Workload Assessment (`assess_deployment.py`)

**What it does:**

Determines the right Snowflake table type for each table. Combines BIM structural signals (automatic) with four user questions to score each table 0–100 and assign a recommendation.

**The 4 Workload Questions:**

**Question 1 — Concurrent users at peak**
```
a) Fewer than 10
b) 10 – 100
c) 100 – 1,000
d) More than 1,000
```
Why it matters: Interactive Tables share a warm cache across concurrent users. With fewer than 100 users the always-on Interactive Warehouse cost is rarely justified. Score: a=0, b=+10, c=+30, d=+50. **Cost gate: answers (a) or (b) attach a cost warning to every INTERACTIVE_TABLE recommendation.**

---

**Question 2 — Query pattern**
```
a) Most filter by date range, region, or a specific category  (selective)
b) Many aggregate ALL data without filters                     (full scans)
c) A mix of both
```
Why it matters: Interactive Tables pre-materialise aggregations and excel at selective queries. They provide no advantage for queries that must read every row. Score: a=+25, b=−20, c=+5

---

**Question 3 — Required response time**
```
a) Sub-second  (< 1 second)
b) 1 – 3 seconds
c) 3+ seconds acceptable
```
Why it matters: Interactive Tables are designed specifically for < 1s. If 1–3s is acceptable, a well-clustered regular table with result caching typically meets SLA at far lower cost. Score: a=+20, b=+5, c=0

---

**Question 4 — Data refresh cadence**
```
a) Real-time / near-real-time
b) Hourly
c) Daily
d) Weekly / ad-hoc
```
Why it matters: Interactive Tables use `TARGET_LAG` to control how often Snowflake re-materialises pre-computed aggregations. This answer sets the TARGET_LAG in generated DDL. Score: a=+10, b=+8, c=+3, d=0. TARGET_LAG: a→60s, b→1 hour, c→1 day, d→7 days.

---

**BIM structural signals (automatic):**

| Signal | Score |
|---|---|
| `storageMode = directQuery` | +40 |
| Date-range partitions | +20 |
| `isDateTable = true` | +15 |
| Dimension table (one-side only) | +15 |
| Has hierarchies | +10 |
| Bidirectional cross-filter | −15 |
| Large fact with no partitions | −10 |

**Table type decision:**
- Score ≥ 70 → `INTERACTIVE_TABLE`
- Score 40–69 → `REGULAR_TABLE_WITH_CLUSTERING`
- Score < 40 → `REGULAR_TABLE`
- `is_calculated_table = true` → `CALCULATED_VIEW` (overrides score)

**Cost gate — Interactive Warehouse 24h min auto-suspend:**

| Concurrent users | Cost stance |
|---|---|
| < 10 | COST CAUTION — always-on billing almost certainly not justified. Table flagged ⚠. |
| 10–100 | COST NOTE — evaluate cache hit rate. Table flagged ⚠. |
| 100–1,000 | No warning — generally justified. |
| > 1,000 | No warning — strongly justified. |

---

### Phase 3 — Migration Plan (`generate_migration_plan.py`)

**What it does:**

Reads both `ssas_inventory.json` and `deployment_assessment.json` and writes two files to the current working directory:

**`MIGRATION_PLAN.md` contains:**
1. Executive summary (counts, complexity, storage modes)
2. Table migration plan — type assigned per table, CLUSTER BY columns, TARGET_LAG, cost warning flag
3. CLUSTER BY rationale — explains why each clustering column was chosen, mapped to source DAX patterns (see below)
4. Cost warnings (if any)
5. DAX translation scope — expression counts and ballpark token estimates
6. Security migration scope — RLS roles, OLS objects
7. Output files list across all remaining phases
8. Risks and attention items
9. Approval gate — user must reply 'approved' before Phase 4 proceeds

**`migration_status.md` contains:**

A phase-by-phase status table updated throughout the migration:

```
| Phase | Status | Objects Created in Snowflake | Est. Tokens | Notes |
| Phase 1 — Assess              | ✅ Completed | — | 0      | 12 tables, 47 measures   |
| Phase 2 — Workload Assessment | ✅ Completed | — | 0      | 2 interactive, 9 regular |
| Phase 3 — Migration Plan      | ✅ Completed | — | 0      | MIGRATION_PLAN.md written |
| Phase 4 — Schema DDL          | ⏳ Pending   | — | 0      | —                        |
| Phase 5 — DAX Translation     | ⏳ Pending   | — | ~23,500| ~47 LLM calls (estimate) |
| Phase 6 — Security            | ⏳ Pending   | — | ~1,500 | ~3 RLS roles             |
| Phase 7 — Validate            | ⏳ Pending   | — | 0      | —                        |
```

Token estimates are **ballpark figures only** — not actual usage, not for cost invoicing.
Updated each phase by `update_migration_status.py`.

**This phase has a hard approval gate.** The skill will not proceed to Phase 4 until the user replies 'approved'.

---

### CLUSTER BY Rationale — Mapping from Source DAX Query Patterns

This is a key section of `MIGRATION_PLAN.md`. It explains the conceptual bridge between SSAS performance and Snowflake clustering.

**The analogy:**

In SSAS Tabular, **VertiPaq** stores each column as a compressed dictionary + run-length encoded value vector. When a DAX filter runs (e.g. `CALCULATE([Revenue], DimDate[Year] = 2024)`), VertiPaq looks up the value `2024` in the `[Year]` column dictionary — effectively **O(1) with no row scanning**.

Snowflake's columnar storage achieves equivalent selectivity through **micro-partition pruning**: Snowflake divides tables into 16 MB micro-partitions and records the min/max value of each column per partition. When a table is `CLUSTER BY (col)`, rows with similar values are physically co-located, so Snowflake **skips entire partitions** that cannot satisfy a filter predicate.

**The columns that VertiPaq filters via dictionary lookups are the same columns that should be Snowflake CLUSTER BY keys.**

**DAX pattern → CLUSTER BY mapping:**

| DAX Pattern | CLUSTER BY Candidate | Why |
|---|---|---|
| `TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD` | Date / OrderDate / FiscalDate | Time intelligence filters resolve through the date dimension — VertiPaq dictionary lookup → Snowflake date-range partition pruning |
| `CALCULATE([M], Region = "West")` | Region, Territory, Country | Explicit filter predicate — same column should be CLUSTER BY for micro-partition skip |
| `FILTER(Fact, [FK] = value)` | FK column | Row-level filter on FK — co-locating by FK reduces cross-partition scatter on joins |
| Hierarchy Year→Quarter→Month→Day | Year (topmost level) | Drill-down queries always filter the broadest level first |
| Date-range partitions in source | Partition date column | Source model explicitly partitions by this column — same drives Snowflake clustering |

The plan file shows this explanation per table, with the specific source traced for each column (partition expression / date table key / hierarchy level / relationship FK).

---

### Phase 4 — Schema DDL (`generate_ddl.py`)

**What it does:**

Routes each table to the correct `CREATE` statement and writes the DDL file. **The DDL file must be reviewed by the user before the skill executes it against Snowflake.**

The skill presents three options after DDL generation:
- `'deploy ddl'` — executes `snow sql -f ./ssas_ddl.sql`
- `'review first'` — waits for user to open and inspect the file
- `'skip deploy'` — saves only, user deploys manually

The skill **never auto-executes DDL** without explicit user confirmation.

Per table types generated:
- **`INTERACTIVE_TABLE`**: `CREATE INTERACTIVE TABLE ... CLUSTER BY (...) TARGET_LAG = '...' WAREHOUSE = ...`
- **`REGULAR_TABLE_WITH_CLUSTERING`**: `CREATE TABLE ... CLUSTER BY (...)`
- **`REGULAR_TABLE`**: `CREATE TABLE ...`
- **`CALCULATED_VIEW`**: `CREATE OR REPLACE VIEW ...`

DDL footer: Interactive Warehouse block, maintenance warehouse, OLS REVOKE / masking policy section.

---

### Phase 5 — DAX Translation (`convert_dax.py` + `generate_semantic_view.py`)

**`convert_dax.py`** — two-stage translation:

**Stage 1 — Regex pattern matching (~25 rules, no LLM):**

| DAX | SQL |
|---|---|
| `SUM('Sales'[Amount])` | `SUM(sales.amount)` |
| `DISTINCTCOUNT('Product'[ID])` | `COUNT(DISTINCT product.id)` |
| `DIVIDE([A], [B], 0)` | `IFF(b = 0, 0, a / b)` |
| `TODAY()` | `CURRENT_DATE()` |
| `SELECTEDMEASURE()` | `__SELECTEDMEASURE__` (placeholder) |

**Stage 2 — Cortex LLM fallback (`claude-sonnet-4-5`):**
For: `CALCULATE`, `FILTER`, `RELATED`, `ALL`, time intelligence, `RANKX`, `TOPN`, `SWITCH`, etc.

**Calculation group expansion (N×M):**
Each calc item × each base measure → one named metric, `SELECTEDMEASURE()` replaced by base measure SQL:
- `Total Sales` × `MTD` → `Total Sales MTD`
- `Gross Margin` × `YTD` → `Gross Margin YTD`

**`generate_semantic_view.py`**: Maps columns → dimensions/facts, measures → metrics, relationships → joins, format strings → synonyms.

---

### Phase 6 — Security

**RLS:** DAX filter expressions → Row Access Policies (`CURRENT_USER()`, subquery mapping tables).

**OLS (if `has_ols: true`):**
- Table-level (`metadataPermission: none`) → `REVOKE SELECT ON <table> FROM ROLE`
- Column-level → masking policy (returns NULL) or role-specific view (omits column)

---

### Phase 7 — Validate & Export

Row count checks, calculated column view spot-checks, semantic view validation, final `migration_status.md` display.

---

## File Roles

### `SKILL.md`
The workflow brain. Seven-phase migration process, stopping points, script commands, status update calls, troubleshooting. Loaded automatically by Cortex Code when SSAS migration is requested.

---

### `scripts/parse_bim.py`
**Input:** `model.bim`  **Output:** `ssas_inventory.json`

Parses JSON-format model.bim (compat 1200+). Pure stdlib. Rejects compat < 1200 with export instructions.

---

### `scripts/assess_deployment.py`
**Input:** `ssas_inventory.json`  **Output:** `deployment_assessment.json`

Runs the 4-question workload questionnaire. Scores each table 0–100 using BIM signals + user answers. Attaches cost warnings for Interactive Table recommendations with low concurrency (< 100 users).

---

### `scripts/generate_migration_plan.py`
**Input:** `ssas_inventory.json` + `deployment_assessment.json`  
**Output:** `MIGRATION_PLAN.md` + `migration_status.md`

Generates the full human-readable migration plan including CLUSTER BY rationale mapped to source DAX patterns, ballpark token estimates (with disclaimer), cost warnings, risks, and an approval gate. Also creates the initial `migration_status.md` with Phases 1–3 completed and Phases 4–7 pending.

---

### `scripts/update_migration_status.py`
**Input:** `migration_status.md` (existing)  
**Output:** `migration_status.md` (updated in-place)

Updates a single phase row in the status table. Called at the end of each phase with `--phase`, `--status`, `--objects`, `--tokens`, `--notes`. Idempotent — safe to call multiple times. Prints a confirmation line with the new status.

---

### `scripts/generate_ddl.py`
**Input:** `ssas_inventory.json` + optional `deployment_assessment.json` + optional `measures_translated.json`  
**Output:** `ssas_ddl.sql`

Routes each table to the correct CREATE statement. Emits Interactive Warehouse block, maintenance warehouse, OLS section in footer. Never deployed automatically — skill requires explicit user confirmation.

---

### `scripts/convert_dax.py`
**Input:** `ssas_inventory.json`  **Output:** `ssas_measures_translated.json`

Translates all DAX to SQL via regex patterns then Cortex LLM. Expands calculation groups N×M. Flags unresolvable expressions for manual review.

---

### `scripts/generate_semantic_view.py`
**Input:** `ssas_inventory.json` + `ssas_measures_translated.json`  **Output:** `ssas_semantic_view.yaml`

Generates Snowflake Semantic View YAML for Cortex Analyst. Maps columns, measures, relationships, and calc group expansions.

---

### `references/dax-to-sql-patterns.md`
Full DAX → SQL pattern library. Loaded during Phase 5.

---

### `references/snowflake-equivalents.md`
SSAS → Snowflake object mapping, data types, storage mode → table type routing, Interactive Table syntax, Row Access Policy and masking policy templates.

---

### `references/ssas-features-complete.md`
Deep reference for complex scenarios. 10 sections: compatibility levels, calculation groups, OLS, storage modes, partitioning, perspectives, bidirectional filters, calculated tables, date tables, translations.

---

### `pyproject.toml`
`uv` project config. Dependency: `snowflake-connector-python>=3.0.0` (used by `convert_dax.py`).

---

## Supported SSAS Features

| Feature | Supported |
|---|---|
| Tables, columns, measures, KPIs | Full |
| Calculated columns | Full |
| Hierarchies | Extracted (parent-child → recursive CTE doc) |
| Relationships (active + inactive, bidirectional) | Full |
| Partitions (SQL query + M/Power Query) | Full |
| Row-Level Security (RLS) | → Row Access Policies |
| Object-Level Security, table-level | → REVOKE SQL |
| Object-Level Security, column-level | → masking policy / column GRANT options |
| Calculation groups (compat 1500+) | N×M expansion |
| Calculated tables | → VIEW |
| Perspectives | Extracted → semantic view per perspective |
| Translations / multi-language | → synonyms |
| Interactive Table recommendation | Scored per table with cost gate |
| DirectQuery models | → Interactive Table |
| Composite (mixed) models | Per-table routing |
| Compat 1100/1103 (XML) | Error + export instructions |

---

## Quick Start

```bash
SKILL_DIR=~/.snowflake/cortex/skills/ssas-tabular-migration

# Phase 1: Parse the model
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/parse_bim.py \
  --bim-path /path/to/model.bim --output ./ssas_inventory.json

# Phase 2: Score tables
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/assess_deployment.py \
  --inventory ./ssas_inventory.json --output ./deployment_assessment.json

# Phase 3: Generate migration plan (writes MIGRATION_PLAN.md + migration_status.md)
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/generate_migration_plan.py \
  --inventory ./ssas_inventory.json \
  --assessment ./deployment_assessment.json \
  --target-schema MY_DB.MY_SCHEMA
# *** Review MIGRATION_PLAN.md and approve before continuing ***

# Phase 4: Generate DDL (review before deploying)
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/generate_ddl.py \
  --inventory ./ssas_inventory.json \
  --assessment ./deployment_assessment.json \
  --target-schema MY_DB.MY_SCHEMA \
  --output ./ssas_ddl.sql
# *** Review ssas_ddl.sql before executing ***

# Phase 5: Translate DAX
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/convert_dax.py \
  --inventory ./ssas_inventory.json \
  --output ./ssas_measures_translated.json \
  --connection COCO_JK

uv run --project $SKILL_DIR python $SKILL_DIR/scripts/generate_semantic_view.py \
  --inventory ./ssas_inventory.json \
  --measures ./ssas_measures_translated.json \
  --target-schema MY_DB.MY_SCHEMA \
  --output ./ssas_semantic_view.yaml

# Validate semantic view
cortex reflect ./ssas_semantic_view.yaml

# Update status at end of each phase
uv run --project $SKILL_DIR python $SKILL_DIR/scripts/update_migration_status.py \
  --phase "Phase 5" --status completed --tokens 23500 --notes "47 measures translated"
```
