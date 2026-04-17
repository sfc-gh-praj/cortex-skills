---
name: ssis-migration
description: "**[REQUIRED]** Use for ALL SSIS to Snowflake migration tasks. Covers: assessment, migration planning, implementation, deployment, and E2E testing of SSIS packages (.dtsx) migrated via SnowConvert. Triggers: SSIS migration, SSIS to Snowflake, migrate SSIS, SSIS assessment, SSIS packages, dtsx migration, SSIS ETL migration, SnowConvert SSIS, SSIS conversion."
---

# SSIS to Snowflake Migration

End-to-end migration of SSIS packages to Snowflake using SnowConvert output. Follows a mandatory 5-phase process with user approval gates between phases.

> Reference: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README) | [EWI Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/conversion-issues/ssisEWI) | [FDM Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/ssisFDM)

## Prerequisites

- SnowConvert has already been run on the SSIS packages (user pre-requisite)
- Access to Cortex Code with `snowconvert-assessment` skill
- Snowflake account with appropriate permissions
- `.dtsx` package files available locally

## Setup

1. **Load** `references/component_mapping_reference.md` — SSIS-to-Snowflake component mapping patterns
2. **Load** `references/snowflake_patterns.md` — Snowflake implementation patterns for common SSIS constructs

## Workflow Overview

```
Phase 1: Assessment (snowconvert-assessment skill)
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

### Step 1.0: Gather Required Paths (MANDATORY FIRST)

**⚠️ MANDATORY STOPPING POINT**: Collect ALL paths before any analysis.

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

**Invoke** the `snowconvert-assessment` skill to generate:
- `etl_assessment_analysis.json` — Structured assessment data
- `ssis_assessment_report.html` — Interactive HTML report
- `dags/` — Control Flow and Data Flow DAG visualizations
- `ai_ssis_summary.html` — AI-generated summary

After assessment, generate `etl_assessment_summary.md` from the JSON.

### Step 1.3: Review Assessment Output

Present findings:
- DAG visualizations (control flow + data flow per package)
- Component inventory with conversion status
- Components marked `NotSupported` or with EWI markers
- Display paths to all generated artifacts

**Log** Phase 1 to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 1 Required Stats in Tracking File

1. **Package Analysis Results** — Table with columns: Package, Classification, Components (CF/DF), Effort (hrs), Key Findings
2. **Connection Managers Identified** — Table with columns: Name, Type, Purpose
3. **Unsupported Elements** — Numbered list of all components marked `NotSupported` or with critical EWI markers
4. **Complexity Drivers** — Numbered list of the top complexity contributors (Script Components, loops, Script Tasks, etc.) with specific details
5. **Assessment Output Files** — Table listing all generated artifacts with File, Path, and Size columns

**Only proceed to Phase 2 after assessment is complete.**

---

## Phase 2: Migration Planning

### Step 2.1: Present Implementation Options

**⚠️ MANDATORY STOPPING POINT**: Collect user selections for each category.

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

Record selections. **Log** Phase 2 to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 2 Required Stats in Tracking File

1. **Implementation Approach Selection** — Table with columns: Decision, Options, Selected
2. **Rationale** — Bullet list explaining WHY each approach was selected (not just what was selected)

---

## Phase 3: Detailed Mapping & Migration Plan

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

**Load** `references/phase3_migration_plan_template.md` for the required 13-section structure.

Write `<OUTPUT_DIR>/MIGRATION_PLAN.md` containing ALL 13 mandatory sections. Every EWI/FDM issue from the CSV must be individually listed (do NOT group or deduplicate).

### Step 3.4: User Approval Gate (MANDATORY)

**⚠️ MANDATORY STOPPING POINT**: Phase 4 MUST NOT begin until user explicitly approves the plan.

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

### Step 4.1: Gather Target Details

Confirm with user:
- Target Database/Schema
- Snowflake Connection name
- Warehouse name

### Step 4.2: Generate Implementation Files

Based on the MIGRATION_PLAN.md, generate all artifacts. The output structure depends on the project — a typical layout (adapt as needed):

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

Write `<OUTPUT_DIR>/implementation/solution_artifacts_generated.md` listing all files with source attribution and EWI resolution mapping.

### Step 4.3: User Approval Gate (MANDATORY)

**⚠️ MANDATORY STOPPING POINT**: Phase 5 MUST NOT begin until user explicitly approves the generated implementation code.

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
