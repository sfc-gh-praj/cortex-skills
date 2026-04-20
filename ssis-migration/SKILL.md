---
name: ssis-migration
description: "**[REQUIRED]** Use for ALL SSIS to Snowflake migration tasks. Covers: assessment, migration planning, implementation, deployment, and E2E testing of SSIS packages (.dtsx) migrated via SnowConvert. Triggers: SSIS migration, SSIS to Snowflake, migrate SSIS, SSIS assessment, SSIS packages, dtsx migration, SSIS ETL migration, SnowConvert SSIS, SSIS conversion."
---

# SSIS to Snowflake Migration

End-to-end migration of SSIS packages to Snowflake using SnowConvert output. Follows a mandatory 5-phase process with user approval gates between phases.

> Reference: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README) | [EWI Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/conversion-issues/ssisEWI) | [FDM Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/ssisFDM)

## Prerequisites

- SnowConvert has already been run on the SSIS packages (user pre-requisite)
- Python 3.9+ available locally (used by the bundled assessment script)
- Snowflake account with appropriate permissions
- `.dtsx` package files available locally

## Setup

1. **Load** `references/component_mapping_reference.md` — SSIS-to-Snowflake component mapping patterns
2. **Load** `references/snowflake_patterns.md` — Snowflake implementation patterns for common SSIS constructs

## Workflow Overview

```
Phase 1: Assessment (bundled generate_ssis_report.py script)
  ↓
Phase 2: Migration Planning (user selects approach)
  ↓  ⚠️ STOP
Phase 3: Detailed Mapping → MIGRATION_PLAN.md (13 sections)
  ↓  ⚠️ STOP — user must approve plan
Phase 4: Implementation (generate all SQL/dbt files)
  ↓  ⚠️ STOP — user must approve generated scripts before any deployment
Phase 5: Validation & Testing (deploy + E2E test)
  ↓  LOG to migration_phase_tracking.md
DONE
```

## Progress Reporting (MANDATORY)

At the **start of every phase** and at every **⚠️ MANDATORY STOPPING POINT**,
output a progress banner to the user in this exact format:

```
---
📍 Phase N of 5 — <Phase Name>
Progress: [████████░░░░░░░░░░░░] X%
Status: <In Progress | Awaiting User Input | Complete>
---
```

**Phase progress thresholds:**

| Milestone | % | Banner |
|---|---|---|
| Phase 1 started | 5% | `[█░░░░░░░░░░░░░░░░░░░]` |
| Phase 1 complete | 20% | `[████░░░░░░░░░░░░░░░░]` |
| Phase 2 started | 25% | `[█████░░░░░░░░░░░░░░░]` |
| Phase 2 complete | 35% | `[███████░░░░░░░░░░░░░]` |
| Phase 3 started | 40% | `[████████░░░░░░░░░░░░]` |
| Phase 3 complete | 55% | `[███████████░░░░░░░░░]` |
| Phase 4 started | 60% | `[████████████░░░░░░░░]` |
| Phase 4 complete | 80% | `[████████████████░░░░]` |
| Phase 5 started | 85% | `[█████████████████░░░]` |
| Phase 5 complete | 100% | `[████████████████████]` |

At every stopping point where user input is awaited, include:
```
⚠️  WAITING FOR YOUR INPUT — migration is paused until you respond.
```

**CRITICAL**: Create `<OUTPUT_DIR>/migration_phase_tracking.md` at the start of Phase 1. It MUST begin with a project header:

```
# SSIS to Snowflake Migration - Phase Tracking Log

**Project:** <project name derived from SSIS package set>
**Start DateTime:** <YYYY-MM-DD HH:MM:SS>
**SSIS Source:** `<ssis_source_path>`
**Packages:** <comma-separated list of .dtsx package names>

---
```

Log every phase start and completion to this file. Each phase entry MUST include:
- **Start datetime** (when the phase began)
- **End datetime** (when the phase completed)
- **Duration** (elapsed time)
- **Tokens used** (estimated input + output tokens consumed during the phase)
- Phase status and key outcomes
- **Phase-specific stats** (see each phase section for required stats tables and lists)

**Token tracking**: At each phase boundary, note the approximate token usage for that phase. If exact counts are unavailable, estimate based on the number of tool calls, file reads/writes, and SQL executions performed. Log as `~NNNk` (e.g., `~45k`). Also maintain a running cumulative total across all phases.

**REQUIRED**: The tracking file MUST include a **Token Estimation Methodology** section near the top (after the project header, before Phase 1) so readers understand how estimates were derived. Use this template:

```
## Token Estimation Methodology

Token counts are **approximate estimates** — exact API token counters are not exposed during the session. Estimates are derived using activity-based costing:

| Activity | Approx. Tokens |
|----------|---------------|
| File read (~200 lines) | ~2-4k |
| File read (~500+ lines) | ~5-10k |
| Bash command + output | ~1-3k |
| SQL execution + results | ~1-3k |
| File write/edit | ~2-5k |
| Skill loading (SKILL.md expansion) | ~5-15k |
| Agent reasoning + response text | ~1-3k |
| Tool call round-trip overhead | ~0.5-1k |

**Per-phase formula**: Count tool calls in phase × average cost per activity type = estimated tokens.
Example: Phase with 12 file reads + 5 bash commands + 3 file writes ≈ (12×3k) + (5×2k) + (3×3.5k) ≈ ~56k

**Limitations**: Conversation history accumulation is not precisely factored. Cumulative totals may drift for longer sessions. These are ballpark figures for effort comparison, not billing metrics.
```

Example format for each phase:
```
## Phase 1: Assessment — COMPLETED

**Status:** COMPLETED
**DateTime Completed:** 2026-03-27 10:42:00

- Start: 2026-03-27 10:15:00
- End:   2026-03-27 10:42:00
- Duration: 27 min
- Tokens: ~35k (cumulative: ~35k)
- Token breakdown: 8 file reads (~24k) + 4 bash commands (~8k) + 1 skill load (~5k) ≈ ~37k → rounded to ~35k

### Package Analysis Results
| Package | Classification | Components (CF/DF) | Effort (hrs) | Key Findings |
|---------|...

### Connection Managers Identified
| Name | Type | Purpose |
|------|...

(... additional phase-specific stats ...)
```

**REQUIRED**: After all phases are complete, append these summary sections to `migration_phase_tracking.md`:

### Effort Tracking Table
A table summarizing estimated vs actual effort per phase:
```
## Effort Tracking

| Phase | Estimated Effort | Actual Effort | Notes |
|-------|-----------------|---------------|-------|
| Phase 1 — Assessment | 1.0 hr | 0 hrs 45 min | ... |
| Phase 2 — Planning | 0.5 hr | 0 hrs 15 min | ... |
| Phase 3 — Detailed Mapping | 3.0 hr | 1 hrs 30 min | ... |
| Phase 4 — Implementation | 6.0 hr | 2 hrs 30 min | ... |
| Phase 5 — Validation | 1.0 hr | 1 hrs 00 min | ... |
| **Total** | **11.5 hrs** | **6 hrs 00 min** | **% faster than estimated** |
```

### Code Rewrite Tracking Table
A line-by-line breakdown comparing SnowConvert (SC) generated code vs manual rewrites:
```
## Code Rewrite Tracking

| Category | SC Generated (LOC) | Manual Rewrite (LOC) | Rewrite % | Notes |
|----------|-------------------|---------------------|-----------|-------|
| SQL DDLs | ... | ... | ...% | ... |
| UDFs | ... | ... | ...% | ... |
| Stored Procedures | ... | ... | ...% | ... |
| Tasks | ... | ... | ...% | ... |
| dbt Models (if used) | ... | ... | ...% | ... |
| **TOTALS** | **...** | **...** | **...%** | ... |
```

### Rewrite Summary
A summary table of key rewrite metrics:
```
## Rewrite Summary

| Metric | Value |
|--------|-------|
| Total SC-generated lines of code | ... |
| Lines requiring manual rewrite/new code | ... |
| Overall rewrite percentage | ...% |
| SC artifacts used as-is | ... |
| SC artifacts modified | ... |
| New artifacts (not from SC) | ... |
| Critical rewrites | ... |
```

### Key Technical Decisions (optional)
If significant technical decisions were made during migration, include a table:
```
## Key Technical Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | ... | ... |
```

---

## Phase 1: Assessment

> **ANNOUNCE PROGRESS** — output banner: Phase 1 of 5 — Assessment | `[█░░░░░░░░░░░░░░░░░░░]` 5% | Status: In Progress

### Step 1.0: Gather Required Paths (MANDATORY FIRST)

**⚠️ MANDATORY STOPPING POINT**: Collect ALL paths before any analysis.

> **ANNOUNCE PROGRESS** — output banner: Phase 1 of 5 — Assessment | `[█░░░░░░░░░░░░░░░░░░░]` 5% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — migration is paused until you respond.

Ask user for:
1. **SSIS source path** — Where are the `.dtsx` files?
2. **SnowConvert CSV path** — Where are `ETL.Elements.csv` and `ETL.Issues.csv`? (typically `Reports/SnowConvert/`)
3. **SnowConvert output path** — Converted SQL/dbt output (typically `Output/SnowConvert/`). Ask if user wants to use SnowConvert output or start from scratch.
4. **Assessment output path** — Where to place results (e.g., `ssis_migration_review/`)
5. **Target Snowflake database name** — The database to create/use for this migration (e.g., `TELECOM_ETL`). Used in all DDLs, `profiles.yml`, and Task definitions.
6. **Target Snowflake schema name** — The schema to use within the target database. Defaults to `PUBLIC` if not specified.
7. **Snowflake warehouse name** — The existing warehouse to use for queries, dbt runs, and Snowflake Tasks (e.g., `COMPUTE_WH`). Must already exist in the account.

### Step 1.1: Verify SnowConvert Output

Verify at the CSV path:
- `ETL.Elements.*.csv` — Component inventory
- `ETL.Issues.*.csv` — Migration issues/warnings (EWIs)

Also check SnowConvert output path for: converted SQL, dbt project, file formats.

### Step 1.2: Run ETL Assessment (MANDATORY)

**Locate the SnowConvert ETL report CSVs** in the user's SnowConvert output directory. They are placed under `Reports/SnowConvert/` inside the conversion output folder:

```bash
find <SNOWCONVERT_OUTPUT_DIR> -name "ETL.Elements*.csv" | head -1
find <SNOWCONVERT_OUTPUT_DIR> -name "ETL.Issues*.csv"   | head -1
```

If the CSVs are not found, ask the user for the SnowConvert output directory. If SnowConvert has not been run yet, document components manually from the `.dtsx` files and skip to Step 1.3.

**Run the bundled assessment script** (requires Python 3.9+, no external packages):

```bash
python3 ~/.snowflake/cortex/skills/ssis-migration/scripts/generate_ssis_report.py \
    --elements /path/to/ETL.Elements.<timestamp>.csv \
    --issues   /path/to/ETL.Issues.<timestamp>.csv \
    --output   <OUTPUT_DIR>/
```

Where `<OUTPUT_DIR>` is the migration review folder (e.g. `ssis_migration_review/`).

**This generates:**
- `ssis_assessment_report.html` — Interactive HTML report with component breakdown, EWI/FDM issues, and per-package drill-down
- `packages/package_<Name>.html` — Per-package detail pages
- `etl_assessment_summary.md` — Markdown summary; copy its content directly into `migration_phase_tracking.md`
- `etl_assessment_analysis.json` — Structured data for downstream use

After running, display the output paths to the user.

### Step 1.3: Review Assessment Output

Present findings:
- DAG visualizations (control flow + data flow per package)
- Component inventory with conversion status
- Components marked `NotSupported` or with EWI markers
- Display paths to all generated artifacts

#### OpenFlow Connector Availability Annotation (Mandatory)

After reviewing detected source/destination types in `ETL.Elements.csv`, annotate `etl_assessment_summary.md` with Snowflake OpenFlow connector availability. This informs Phase 2 Source Ingestion and CDC decisions before any architecture choices are made.

| Detected Source/Destination | OpenFlow Connector | Note |
|-----------------------------|--------------------|------|
| SQL Server (OLE DB / CDC Source) | Yes — `sqlserver` | CDC-capable; managed near-real-time replication |
| Oracle (OLE DB / ADO.NET) | Yes — `oracle-embedded-license` or `oracle-independent-license` | Licensing decision + network connectivity must be assessed |
| MySQL | Yes — `mysql` | CDC-capable |
| PostgreSQL | Yes — `postgresql` | CDC-capable |
| SAP BW Source | No | Snowpark Python or certified partner tools |
| OData Source | No | Airflow HTTP Operator or Snowflake External Function |
| Azure Blob / ADLS Source | No — native COPY INTO | Use external stage directly |
| Flat File / ODBC Source | No — native COPY INTO | Snowpipe or COPY INTO from stage |

For each source with an available connector, add to `etl_assessment_summary.md`:
> `"[Source Type] detected — Snowflake OpenFlow [connector] connector available. Evaluate for Phase 2 Source Ingestion strategy before writing custom ingest code."`

**Log** Phase 1 to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats.

> **Speed tip**: Read `<OUTPUT_DIR>/etl_assessment_analysis.json` — it contains all the data below. Use it directly instead of re-deriving from the CSVs.

#### Phase 1 Required Stats in Tracking File

1. **Package Analysis Results** — Read from `etl_assessment_analysis.json` → `packages[]`. Build a table: Package, Classification, Components (CF/DF), Supported %, Effort (hrs), Complexity, Key Findings. Do not recompute — copy values from JSON.
2. **Connection Managers Identified** — Table with columns: Name, Type, Purpose (read from `.dtsx` files or SnowConvert output)
3. **Unsupported Elements** — Read from `etl_assessment_analysis.json` → filter `packages[].elements[]` where `Status == "NotSupported"`. List each as: Package → Subtype → Component name.
4. **Complexity Drivers** — Read from `etl_assessment_analysis.json` → `packages[]` where `complexity` is `"High"` or `"Medium"`. List the specific subtypes driving complexity (ScriptTask, ForEachLoop, etc.).
5. **Assessment Output Files** — Table listing all generated artifacts with File, Path, and Size columns (use `ls -lh <OUTPUT_DIR>/` output)

**Only proceed to Phase 2 after assessment is complete.**

---

## Phase 2: Migration Planning

> **ANNOUNCE PROGRESS** — output banner: Phase 2 of 5 — Migration Planning | `[████░░░░░░░░░░░░░░░░]` 20% | Status: In Progress

### Step 2.1: Present Implementation Options

**⚠️ MANDATORY STOPPING POINT**: Collect user selections for each category.

> **ANNOUNCE PROGRESS** — output banner: Phase 2 of 5 — Migration Planning | `[█████░░░░░░░░░░░░░░░]` 25% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — migration is paused until you respond.

#### Control Flow Orchestration
| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| Stored Procedures | Complex orchestration, file loops, conditional logic | Full control, native, no dependencies | More code |
| Tasks + Streams | Event-driven, simple orchestration | Serverless, auto-scaling | Limited complex logic |
| External Orchestrator | Enterprise scheduling, cross-platform | Rich monitoring | External dependency |

#### Data Flow Transformations
| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| dbt Models | Complex transforms, testing, lineage | Version control, testing framework | Requires dbt setup |
| Dynamic Tables | Continuous transforms, simple logic | Auto-refresh, declarative | Less flexible |
| SP SQL | Simple transforms, tight coupling | Single deployment unit | Harder to test |
| Hybrid | Mixed complexity | Best of both | More components |

#### File Storage
| Option | Best For |
|--------|----------|
| Internal Stage | Full Snowflake-native, folder structure, directory tables |
| External Stage (S3/Azure/GCS) | Existing cloud storage |
| Hybrid | Landing in cloud, processing in Snowflake |

#### File Movement
| Option | Best For |
|--------|----------|
| Stage Folder Ops (COPY FILES + REMOVE) | Snowflake-native file management |
| External Process (Python/Shell) | Complex file operations |
| Skip | Files managed externally |

#### Source Ingestion Pattern

| Option | Best For | Notes |
|--------|----------|-------|
| COPY INTO (Batch) | File-based ingestion, scheduled loads | Native Snowflake; internal/external stages |
| Snowpipe (Auto-ingest) | Near-real-time file ingestion | S3/ADLS event triggers |
| OpenFlow Connector | DB sources (SQL Server, Oracle, MySQL, PostgreSQL) | Managed CDC/bulk replication; see Phase 1 annotation |
| External Tables | Data stays in cloud storage, query-in-place | No load cost; query performance trade-off |
| Snowpark Python | Custom/complex sources | Full flexibility; requires Python |

#### CDC / Change Data Capture (Conditional — complete only when CDC Control Task, CDC Source, or CDC Splitter detected in assessment DAG)

> **SSIS CDC components (CDC Control Task, CDC Source, CDC Splitter) are SQL Server CDC feature only.** They do not apply to Oracle without the deprecated Attunity Oracle CDC Service add-on (supported only through SQL Server 2017). Confirm source database before applying.

**Tier 1 — Direct SQL Server CDC replacement (reads SQL Server change tables):**

| Option | Notes |
|--------|-------|
| **OpenFlow SQL Server Connector** | Recommended. Managed service — reads SQL Server CDC change tables directly, handles initial snapshot + streaming changes, no custom code. |

**Tier 2 — Snowflake-side change tracking (data already replicated into Snowflake staging tables):**

| Option | Notes |
|--------|-------|
| **Streams + Dynamic Tables** | Declarative, low-ops. Best for continuous refresh patterns with simple transformations. |
| **Streams + Tasks** | Imperative. Full control over INSERT/UPDATE/DELETE routing via `METADATA$ACTION`. More SP code to maintain. |

> **Note on Snowflake Streams:** Streams track changes on *Snowflake tables* — they do not read from SQL Server. Use Tier 2 only when a bulk replication layer already lands data into Snowflake staging, and you need downstream change propagation within Snowflake.

#### Oracle Source Strategy (Conditional — complete only when Oracle Source or Oracle Destination detected in assessment)

| Scenario | Recommended Approach |
|----------|----------------------|
| Oracle reachable from Snowflake cloud (public IP / VPN / Direct Connect) | OpenFlow Oracle Connector (SPCS deployment) |
| Oracle on-prem, OpenFlow BYOC runtime can be deployed in customer network | OpenFlow Oracle Connector (BYOC deployment) — requires XStream setup + licensing decision |
| Oracle on-prem, no VPC or network join possible | Blob Storage Intermediary — see options below |

**Blob Storage Intermediary options (on-prem Oracle, no network):**
- **Debezium** (on-prem, LogMiner) → Kafka → S3/ADLS → Snowpipe — true CDC, open source, no Oracle license needed
- **Oracle GoldenGate** → Trail files → S3/ADLS → Snowpipe — true CDC, requires GoldenGate license
- **SQL*Plus/JDBC incremental extract** → CSV/Parquet → S3/ADLS → COPY INTO — near-CDC via high watermark, misses DELETEs
- **Oracle Data Pump export** → S3/ADLS → COPY INTO — batch only, simplest to implement

Record all selections. **Log** Phase 2 to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 2 Required Stats in Tracking File

1. **Implementation Approach Selection** — Table with columns: Decision, Options, Selected (cover all 7 dimensions: Orchestration, Transformations, File Storage, File Movement, Source Ingestion, CDC if applicable, Oracle strategy if applicable)
2. **Rationale** — Bullet list explaining WHY each approach was selected (not just what was selected)

---

## Phase 3: Detailed Mapping & Migration Plan

> **ANNOUNCE PROGRESS** — output banner: Phase 3 of 5 — Detailed Mapping | `[███████░░░░░░░░░░░░░]` 35% | Status: In Progress

### Step 3.1: DAG Review

Read all DAGs under `<OUTPUT_DIR>/dags/`. Analyze:
- Control Flow: component ordering, precedence constraints, loop boundaries
- Data Flow: sources → transforms → lookups → script components → destinations

### Step 3.2: Analyze SnowConvert Output

If user chose to use SnowConvert output, read all converted files:
- SQL stored procedures, DDLs, file formats
- dbt models, macros, sources, profiles
- Identify EWI/FDM markers in converted code
- Read the C# script component code (if any NotSupported components)
- Read both `ETL.Issues.*.csv` and `Issues.*.csv` for complete issue list

### Step 3.3: Write MIGRATION_PLAN.md (MANDATORY)

**First, check package complexity** from `etl_assessment_analysis.json`:

```
Low-complexity criteria (ALL must be true across every package):
  • no package has complexity = "High" or "Medium"
  • totals.not_supported = 0
  • totals.ewi = 0  (or only informational EWIs with no manual action required)
  • no ScriptTask or ScriptComponent in any package
```

#### If ALL packages are Low complexity → Condensed 5-Section Plan

Write `<OUTPUT_DIR>/MIGRATION_PLAN.md` with these 5 sections only:

1. **Executive Summary** — package count, component count, supported %, estimated effort, chosen approach
2. **Package Mapping** — per-package table: SSIS Component → Snowflake Object (SP/Task/UDF/COPY INTO), one row per element
3. **Implementation Approach** — chosen options from Phase 2 (orchestration, transforms, ingestion, file storage) with rationale
4. **Issues & Manual Steps** — every EWI/FDM from `etl_assessment_analysis.json` → `all_issues[]`, with resolution action for each
5. **Deployment Order** — numbered sequence of SQL scripts to run

#### If ANY package is Medium or High complexity → Full 13-Section Plan

**Load** `references/phase3_migration_plan_template.md` for the required 13-section structure.

Write `<OUTPUT_DIR>/MIGRATION_PLAN.md` containing ALL 13 mandatory sections. Every EWI/FDM issue from the CSV must be individually listed (do NOT group or deduplicate).

### Step 3.4: User Approval Gate (MANDATORY)

**⚠️ MANDATORY STOPPING POINT**: Phase 4 MUST NOT begin until user explicitly approves the plan.

> **ANNOUNCE PROGRESS** — output banner: Phase 3 of 5 — Detailed Mapping | `[███████████░░░░░░░░░]` 55% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — review MIGRATION_PLAN.md and confirm before Phase 4 begins.

1. Tell user the plan location
2. Ask user to review
3. Wait for explicit approval ("approved", "looks good", "go ahead")
4. If changes requested, update and repeat

**Log** Phase 3 (including approval status) to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 3 Required Stats in Tracking File

1. **DAG Review Summary** — List of SnowConvert files reviewed with key observations (EWI markers found, components requiring manual rewrite, etc.)
2. **Component Mapping Summary** — Total components mapped across all packages, broken down by: Master_Orchestrator components → target, Setup components → target, Data_Load CF components → target, Data_Load DF components → target
3. **MIGRATION_PLAN.md Stats** — Line count, all 13 section titles confirmed written

---

## Phase 4: Implementation

> **ANNOUNCE PROGRESS** — output banner: Phase 4 of 5 — Implementation | `[████████████░░░░░░░░]` 60% | Status: In Progress

### Step 4.1: Gather Target Details

Confirm with user:
- Target Database/Schema
- Snowflake Connection name
- Warehouse name

### Step 4.1a: OpenFlow Connector Deployment (Conditional — only when OpenFlow was selected in Phase 2 for Source Ingestion or CDC)

**⚠️ MANDATORY STOPPING POINT before generating SQL scripts**: If the user selected any OpenFlow connector in Phase 2 (Source Ingestion Pattern or CDC dimension), deploy and validate the connector first. The OpenFlow connector creates the destination schemas and tables — SQL implementation scripts must align with the replicated table structure.

#### Which connector to deploy

| Phase 2 Selection | Connector to Deploy |
|-------------------|---------------------|
| Source Ingestion → OpenFlow Connector (SQL Server source) | `sqlserver` |
| Source Ingestion → OpenFlow Connector (Oracle source, Embedded license) | `oracle-embedded-license` |
| Source Ingestion → OpenFlow Connector (Oracle source, BYOL) | `oracle-independent-license` |
| Source Ingestion → OpenFlow Connector (MySQL source) | `mysql` |
| Source Ingestion → OpenFlow Connector (PostgreSQL source) | `postgresql` |
| CDC → OpenFlow SQL Server Connector | `sqlserver` |

#### Parameters to collect before invoking OpenFlow skill

From the assessment output (`ETL.Elements.csv`, connection managers) and user input:

**For SQL Server (`sqlserver` connector):**
- Source: SQL Server connection URL (`jdbc:sqlserver://host:1433;databaseName=db`)
- Source username and password
- Tables to replicate — derive from OLE DB Source / CDC Source components in the assessment DAG
- Snowflake destination database (from Step 4.1)
- Snowflake role and warehouse

**For Oracle (`oracle-embedded-license` or `oracle-independent-license`):**
- Oracle version (12cR1+) and platform (on-prem / OCI / RDS Custom)
- Oracle Connection URL and XStream Out Server URL
- XStream Outbound Server name
- Connect username and password
- Tables to replicate — derive from Oracle Source components in the assessment DAG
- Core count + core factor (Embedded license only)
- Snowflake destination database, role, warehouse

#### Invoke the OpenFlow skill

Tell the user:
> "Your Phase 2 selection includes an OpenFlow connector. I'll now invoke the `openflow` skill to deploy and configure it. This must complete and validate successfully before we generate the SQL implementation scripts."

**Invoke** the `openflow` skill and follow its full deployment workflow:
1. Network access (EAI) — if SPCS deployment
2. Network validation — test connectivity to source database endpoint
3. Deploy connector flow
4. Configure source, destination, and ingestion parameters (use tables identified from assessment DAG)
5. Upload JDBC driver (SQL Server / MySQL / PostgreSQL — not needed for Oracle, OCI driver is bundled)
6. Verify controllers (before enabling)
7. Enable controllers
8. Verify processors (after enabling)
9. For Oracle: verify XStream connectivity via CaptureChangeOracle processor
10. Start the flow
11. Validate data is flowing — confirm destination schemas and tables created in Snowflake

**⚠️ Do NOT proceed to Step 4.2 until the OpenFlow connector is running and data has been validated flowing into Snowflake.**

#### Post-OpenFlow actions before Step 4.2

Once OpenFlow is validated:
1. Note the Snowflake destination schema and table names created by the connector (these will be lowercase if `CASE_SENSITIVE` identifier resolution was used — SQL scripts must quote them)
2. Confirm which tables are being replicated — these become the source tables for downstream SP/dbt transformations
3. Add an entry to `solution_artifacts_generated.md`: `OpenFlow [connector] connector — deployed and validated, replicating [N] tables from [source] to [destination_schema]`

#### If OpenFlow deployment fails

- Resolve connectivity or configuration issues using the `openflow` skill troubleshooting workflows
- Do NOT fall back to custom COPY INTO/Snowpipe without explicitly re-presenting the Phase 2 Source Ingestion options to the user and getting a new selection
- Log the failure and resolution in `migration_phase_tracking.md`

### Step 4.2: Generate Implementation Files

**First, determine the generation strategy** based on what SnowConvert produced:

#### Strategy A — Adopt SnowConvert Output (default when SnowConvert SQL exists)

> **Speed tip**: If SnowConvert converted SQL files exist (verified in Step 1.1), adopt them directly. Do NOT regenerate files that are already converted. Only write new files for gaps and apply targeted fixes for EWI/FDM markers.

1. **Copy** SnowConvert-converted SQL files into `<OUTPUT_DIR>/implementation/sql/` with numbered prefixes
2. **Scan every file** for blocking EWI markers using:
   ```bash
   grep -rn "!!!RESOLVE EWI!!!" <SNOWCONVERT_OUTPUT_DIR>/
   ```
   Blocking markers look like: `!!!RESOLVE EWI!!! /*** SSC-EWI-SSIS0014 - MESSAGE ***/!!!`
   Informational markers look like: `--** SSC-FDM-SSIS0005 - MESSAGE **` — these require no code change.
3. **Fix each blocking marker** — for every `!!!RESOLVE EWI!!!` found, look up the EWI code in `references/snowflake_patterns.md` → **EWI / FDM Fix Reference** section. Apply the documented fix action. For codes not in the table, consult the public docs linked at the top of that section.
4. **Write new files only** for objects SnowConvert did not produce: orchestrator SP, Task DAG, file format (if missing), test data
5. Log which files were adopted as-is, which were patched, and which were written from scratch in `solution_artifacts_generated.md`

#### Strategy B — Generate from Scratch (only when no SnowConvert SQL exists, or user explicitly chose to start fresh)

Based on the MIGRATION_PLAN.md, generate all artifacts from scratch. The output structure depends on the project — a typical layout (adapt as needed):

```
<OUTPUT_DIR>/
├── implementation/
│   ├── sql/
│   │   ├── 01_create_database.sql
│   │   ├── 02_create_stages.sql
│   │   ├── 03_create_file_format.sql
│   │   ├── 04_create_tables.sql
│   │   ├── ...                          (additional scripts as needed)
│   │   └── NN_run_test.sql
│   ├── dbt_project/          (if dbt selected)
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   ├── macros/
│   │   ├── seeds/
│   │   └── tests/
│   ├── test_data/
│   └── solution_artifacts_generated.md
```

**Key implementation patterns** — refer to `references/snowflake_patterns.md`:
- ForEachLoop → LIST + CURSOR or DIRECTORY(@stage) cursor
- Script Component (C#) → JavaScript/SQL UDFs + lookup seed tables
- Script Task (C#) → Snowflake Scripting (IF/THEN/RAISE)
- SQL OUTPUT clause → sequence + NEXTVAL or MAX(id)
- SSIS expressions (REVERSE, FINDSTRING) → SPLIT_PART
- File System Tasks → COPY FILES + REMOVE via EXECUTE IMMEDIATE
- Send Mail Task → SYSTEM$SEND_EMAIL + Notification Integration
- Bulk Insert Task → COPY INTO + inline FILE_FORMAT
- CreateDirectory → .dummy placeholder file
- Data Flow Task → dbt project (`EXECUTE DBT PROJECT`) or SP inline SQL

Write `<OUTPUT_DIR>/implementation/solution_artifacts_generated.md` listing all files with source attribution (SnowConvert-adopted / patched / new) and EWI resolution mapping.

### Step 4.3: User Approval Gate (MANDATORY)

**⚠️ MANDATORY STOPPING POINT**: Phase 5 MUST NOT begin until user explicitly approves the generated implementation code.

> **ANNOUNCE PROGRESS** — output banner: Phase 4 of 5 — Implementation | `[████████████████░░░░]` 80% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — review all generated scripts before any deployment begins.

1. Present a summary of all generated files (SQL scripts, dbt models, UDFs, SPs, test data) with:
   - File count by category (SQL, dbt, test data, docs)
   - Which files are SC-as-is, modified, fully rewritten, or new
   - Key design decisions made (e.g., column width changes, nullable adjustments)
   - Any deviations from the MIGRATION_PLAN.md and why
2. Tell user the output directory path and list the file tree
3. Ask user to review the generated scripts
4. Wait for explicit approval ("approved", "looks good", "go ahead", etc.)
5. If changes requested → update files, re-present summary, and repeat
6. Do NOT deploy anything to Snowflake until approval is received

**Log** Phase 4 (including approval status) to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 4 Required Stats in Tracking File

1. **Configuration Inputs** — Database, Warehouse, Schemas, Connection name, and approach selections
2. **Generated Deliverables** — Numbered table with columns: #, File, Type, Description (list every generated file)
3. **Total File Count** by category (SQL scripts, dbt models/configs, test data, docs)

---

## Phase 5: Validation & Testing

> **ANNOUNCE PROGRESS** — output banner: Phase 5 of 5 — Validation & Testing | `[█████████████████░░░]` 85% | Status: In Progress

### Step 5.1: Deploy to Snowflake

Execute SQL scripts in numbered order using `snowflake_sql_execute`. Fix any deployment errors iteratively.

**Common deployment issues** (from `references/snowflake_patterns.md`):
- SQL UDFs with subqueries can't be used inside SP temp table context → use inline JOINs
- COPY FILES doesn't support bind variables in FILES=() → use EXECUTE IMMEDIATE
- INTO clause with CASE containing subqueries → simplify query
- VARCHAR length mismatches → verify source data lengths against actual data

### Step 5.2: Upload Test Data & Run SP Test

1. Upload test CSV files to source stage
2. Run the master orchestrator SP directly via `CALL sp_orchestrator()` (or equivalent)
3. Validate SP results: row counts, audit trail, file movement, transformations

### Step 5.3: Task Testing (if Tasks were created)

If the migration includes Snowflake Tasks (SSIS scheduled execution equivalent), test the full task DAG:

1. **Verify task state**: `SHOW TASKS IN DATABASE <DB>` — confirm all tasks exist and are `suspended`
2. **Re-upload test data** to source stage (previous SP test may have moved files):
   ```sql
   PUT file:///<path>/test_data/*.csv @source_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ALTER STAGE source_stage REFRESH;
   ```
3. **Resume tasks** (child tasks first, then root):
   ```sql
   ALTER TASK <child_task> RESUME;
   ALTER TASK <root_task> RESUME;
   ```
4. **Trigger immediate execution**: `EXECUTE TASK <root_task>;`
5. **Wait and verify** task history for both root and child tasks:
   ```sql
   SELECT NAME, STATE, SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME, ERROR_CODE, ERROR_MESSAGE
   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
       TASK_NAME => '<task_name>',
       SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
   ))
   ORDER BY SCHEDULED_TIME DESC LIMIT 5;
   ```
6. **Validate results**: Confirm new batch in `dim_audit`, rows inserted in target tables, files moved to processed stage
7. **Suspend tasks** after testing to avoid unintended scheduled runs:
   ```sql
   ALTER TASK <root_task> SUSPEND;
   ALTER TASK <child_task> SUSPEND;
   ```

**Task testing checklist:**
- [ ] All tasks created and visible via `SHOW TASKS`
- [ ] Root task `EXECUTE TASK` triggers successfully (STATE = `SUCCEEDED`)
- [ ] Child task(s) triggered automatically after root (STATE = `SUCCEEDED`)
- [ ] Orchestrator SP executed correctly via task chain
- [ ] New batch_id generated in audit table
- [ ] All rows processed and inserted
- [ ] Files moved from source to processed stage
- [ ] Tasks suspended after testing

### Step 5.4: Validation Checklist

- [ ] Infrastructure created successfully
- [ ] Sample data loaded to stage
- [ ] SP orchestration runs without errors
- [ ] Task DAG executes end-to-end (if applicable)
- [ ] Transformations produce expected results
- [ ] Row counts match
- [ ] Error handling works
- [ ] File movement operations complete
- [ ] Audit trail captures required metrics

### Step 5.5: Write Test Report

Write `<OUTPUT_DIR>/Solution_End_End_Testing.md` with:
- Deployment summary (per-script pass/fail)
- Issues found and fixed during deployment
- E2E test results (orchestrator output)
- Validation checklist results
- Component traceability (SSIS → Snowflake → test result)
- Data quality summary
- Objects created inventory

**Log** Phase 5 to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 5 Required Stats in Tracking File

1. **Deployment Results** — Table with columns: #, Script, Result (PASS/FAIL), Notes (one row per SQL script deployed)
2. **Code Fixes Required During Deployment** — For each fix: Fix #, File, Error message, Root Cause, Fix description with code snippet. This is critical for capturing lessons learned.
3. **UDF Smoke Tests** — Table with columns: UDF, Test Input, Expected, Actual, Status (PASS/FAIL)
4. **E2E Test Results** — Table with columns: Metric, Expected, Actual, Status (e.g., rows extracted, rows inserted, rows rejected, fraud-flagged rows, valid IMEIs, source stage post-run, archive stage post-run, audit records)
5. **Validation Query Results** — Table with columns: Validation Section, Result (one row per validation section from the validation script)
6. **Task DAG Testing** (if tasks were created) — Task name, schedule, action, state, manual execution test result, task history verification
7. **Deployed Snowflake Objects Summary** — Table with columns: Object Type, Name, Status (every object created: databases, schemas, stages, file formats, tables, UDFs, SPs, tasks)
8. **Migration Timeline Summary** — Table with columns: Phase, Status, Key Output (one row per phase for a final rollup view)

> **ANNOUNCE PROGRESS** — output banner: Phase 5 of 5 — Validation & Testing | `[████████████████████]` 100% | Status: Complete
> Migration is complete. All phases done — hand off `Solution_End_End_Testing.md` and `migration_phase_tracking.md` to stakeholders.

---

## Stopping Points

- ✋ Phase 1, Step 1.0: Paths gathered from user
- ✋ Phase 2: User selects implementation approach
- ✋ Phase 3, Step 3.4: User approves MIGRATION_PLAN.md
- ✋ Phase 4, Step 4.3: User approves generated implementation scripts (NO deployment until approved)
- ✋ Phase 5, Step 5.3: Task testing (if tasks were created — resume, execute, validate, suspend)
- ✋ Phase 5: Final results presented

## Output

Complete SSIS-to-Snowflake migration including:
- Assessment artifacts (JSON, HTML reports, DAGs)
- `MIGRATION_PLAN.md` with 13 mandatory sections
- All SQL scripts, dbt project (if selected), UDFs, test data
- `solution_artifacts_generated.md` — artifact inventory
- `Solution_End_End_Testing.md` — E2E test report
- `migration_phase_tracking.md` — phase-by-phase log with start/end timestamps, durations, effort tracking, and code rewrite analysis

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| SnowConvert CSVs not found | Verify path to `Reports/SnowConvert/` folder |
| EWI count mismatch | Always enumerate ETL.Issues.csv individually — never group |
| SQL UDF subquery error in SP | Replace UDF calls with inline JOINs to lookup tables |
| COPY FILES bind variable error | Use EXECUTE IMMEDIATE with string concatenation |
| INTO clause context error | Simplify SELECT — avoid CASE with subqueries in INTO |
| VARCHAR truncation | Check source data lengths against actual data; adjust column widths |
| `<STAGE_PLACEHOLDER>` in converted code | Replace with actual stage name post-migration |
| DUMMY_WAREHOUSE in converted TASKs | Replace with actual warehouse name |
| Send Mail — attachments not supported | Upload to stage, use GET_PRESIGNED_URL for links |
| Bulk Insert — native format not supported | Export source data to CSV before migration |
| ForEach non-file enumerators not supported | Implement manually using Snowflake queries/scripting |
| Task child not triggered after EXECUTE | Resume child tasks before root: `ALTER TASK <child> RESUME` first |
| DIRECTORY(@stage) shows stale data | Use `LIST @stage` for accurate results; `ALTER STAGE REFRESH` for directory metadata |
| COMMENT on TASK syntax error | Remove COMMENT clause — some task configurations don't support it |
| Task cron won't fire immediately | Use `EXECUTE TASK <root>` for on-demand triggering instead of waiting for schedule |
