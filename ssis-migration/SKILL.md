---
name: ssis-migration
description: "**[REQUIRED]** Use for ALL SSIS to Snowflake migration tasks. Covers: assessment, migration planning, implementation, deployment, and E2E testing of SSIS packages (.dtsx) migrated via SnowConvert. Triggers: SSIS migration, SSIS to Snowflake, migrate SSIS, SSIS assessment, SSIS packages, dtsx migration, SSIS ETL migration, SnowConvert SSIS, SSIS conversion."
---

# SSIS to Snowflake Migration

End-to-end migration of SSIS packages to Snowflake using SnowConvert output. Follows a mandatory 5-phase process with user approval gates between phases.

> Reference: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README) | [EWI Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/conversion-issues/ssisEWI) | [FDM Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/ssisFDM)

## Prerequisites

- `.dtsx` package files available locally
- Python 3.9+ available locally (used by the bundled assessment script)
- Snowflake account with appropriate permissions
- `snowflake-migration` plugin installed — required for the automated dbt TDD Fix Loop (Phase 4). Install by adding `"https://github.com/Snowflake-Labs/cortex-code-migrations/tree/preview/plugin"` to the `plugins` array in `~/.snowflake/cortex/settings.json`, then restart Cortex Code.
- SnowConvert output is **optional** — the skill supports both "start from scratch" (manual `.dtsx` analysis) and "use SnowConvert output" paths

## Setup

1. **Load** `references/component_mapping_reference.md` — SSIS-to-Snowflake component mapping patterns
2. **Load** `references/snowflake_patterns.md` — Snowflake implementation patterns for common SSIS constructs

## Sub-Skill References

The dbt TDD Fix Loop (Phase 4, Step 4.4) delegates to sub-skills from the `migrate-etl-package` skill.
Resolve `MEP_SKILL_DIR` at runtime — do NOT hardcode the path:

```bash
MEP_SKILL_DIR=$(find ~/.snowflake/cortex/remote-cache \
  -path "*/migrate-objects/actions/migrate-etl-package" \
  -maxdepth 8 -type d 2>/dev/null | head -1)
```

| Constant | Path | Used In |
|----------|------|---------|
| `DBT_TEST_GEN_SKILL` | `$MEP_SKILL_DIR/dbt-test-gen/SKILL.md` | Phase 4 Step 4.4 |
| `DBT_FIXER_SKILL` | `$MEP_SKILL_DIR/dbt-fixer/SKILL.md` | Phase 4 Step 4.4 |
| `TRACK_STATUS_PY` | `$MEP_SKILL_DIR/scripts/track_status.py` | Phase 4 Step 4.4, Phase Gate |
| `SSIS_PLATFORM_DIR` | `$MEP_SKILL_DIR/platforms/ssis` | Phase 4 Step 4.4 |
| `TRANSFORMATION_GUIDE` | `$MEP_SKILL_DIR/platforms/ssis/dataflow-guide.md` | Phase 4 Step 4.4 |

If `MEP_SKILL_DIR` resolves to empty, the prerequisite check in Step 0.1 was bypassed or the cache was invalidated mid-session — re-run the Step 0.1 check and resolve before continuing.

## Agent Wait Protocol

**NEVER use `bash sleep`, `bash_output`, or `cortex agent output` to wait for agents.**

1. Spawn all parallel agents in a **single message** (multiple parallel tool calls in one response)
2. **End your turn** — do not narrate, loop, or call additional tools while waiting
3. Automatic task notifications arrive when each background agent finishes — process each notification as it arrives
4. After all notifications arrive, verify output files exist on disk before continuing to the next step

This protocol applies to both the dbt-test-gen wave and the dbt-fixer wave in Step 4.4.

## Phase Gate Enforcement

A machine-readable gate must pass before Phase 5 begins. After completing Step 4.4, run:

```bash
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  validate-phase <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json 4
```

**Gate passes when**: every dbt node has a terminal status (`test-passed`, `fixed`, `no-fix-needed`, `skipped`, `failed`, `needs-user`). Zero nodes in `pending` or `dbt-tested`.

**Gate fails**: report the blocking nodes to the user. Do NOT proceed to Phase 5 until they are resolved or the user explicitly accepts them as `needs-user`.

For resumability, create `STATE.md` at Phase 1 start:

```bash
mkdir -p <OUTPUT_DIR>/.ssis-dbt-tracking
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  update-state <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json \
  --current-phase 1 --phase-status "In Progress" \
  --next-action "Phase 1: Assessment"
```

On re-entry, check for `<OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json` — if it exists, read it to resume from the correct step rather than restarting from Phase 1.

## Workflow Overview

```
Phase 1: Assessment (bundled generate_ssis_report.py script)
  ↓  LOG Phase 1 → migration_phase_tracking.md
Phase 2: Migration Planning (user selects approach)
  ↓  LOG Phase 2 → migration_phase_tracking.md  ← MUST happen before Phase 3
  ↓  ⚠️ STOP
Phase 3: Detailed Mapping → MIGRATION_PLAN.md (13 sections)
  ↓  LOG Phase 3 → migration_phase_tracking.md  ← MUST happen before Phase 4
  ↓  ⚠️ STOP — user must approve plan
Phase 4: Implementation (generate all SQL/dbt files)
  ↓  LOG Phase 4 → migration_phase_tracking.md  ← MUST happen before Phase 5
  ↓  ⚠️ STOP — user must approve generated scripts before any deployment
Phase 5: Validation & Testing (deploy + E2E test)
  ↓  LOG Phase 5 → migration_phase_tracking.md
DONE
```

> **CRITICAL LOGGING RULE**: Each phase MUST be logged to `migration_phase_tracking.md` **immediately when that phase completes** — before any user STOP and before moving to the next phase. Do NOT defer or batch phase logging to the end of the run.

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
| Phase 4 dbt test-gen wave started | 65% | `[█████████████░░░░░░░]` |
| Phase 4 dbt fix wave started | 70% | `[██████████████░░░░░░]` |
| Phase 4 dbt TDD complete | 75% | `[███████████████░░░░░]` |
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

### Step 0.1: Verify Plugin Prerequisites (MANDATORY FIRST)

Before collecting any user inputs, verify that the `snowflake-migration` plugin is installed and the `migrate-etl-package` sub-skill is available. This ensures the full dbt TDD Fix Loop (Phase 4) can run.

**1. Check if plugin is configured:**

```bash
grep -q "cortex-code-migrations" ~/.snowflake/cortex/settings.json && echo "CONFIGURED" || echo "NOT_CONFIGURED"
```

**2. Resolve `MEP_SKILL_DIR`:**

```bash
MEP_SKILL_DIR=$(find ~/.snowflake/cortex/remote-cache \
  -path "*/migrate-objects/actions/migrate-etl-package" \
  -maxdepth 8 -type d 2>/dev/null | head -1)
echo "MEP_SKILL_DIR=$MEP_SKILL_DIR"
```

**3. Decision logic:**

| Plugin Configured? | MEP_SKILL_DIR resolves? | Action |
|--------------------|------------------------|--------|
| Yes | Yes | Proceed to Step 1.0 |
| Yes | No (empty) | Tell user: "The `snowflake-migration` plugin is configured but the cache is missing. Please restart Cortex Code to fetch the plugin, then re-invoke this skill." Use `AskUserQuestion` to confirm they've restarted. Re-run check. |
| No | N/A | Tell user: "The `snowflake-migration` plugin is required for automated dbt testing and fixing (Phase 4). Please add it to your settings and restart Cortex Code." Display the following instructions, then use `AskUserQuestion` to confirm completion before proceeding. |

**Installation instructions to display when plugin is missing:**

> Add the following to `~/.snowflake/cortex/settings.json` inside the `"plugins"` array:
>
> ```json
> "plugins": [
>   "https://github.com/Snowflake-Labs/cortex-code-migrations/tree/preview/plugin"
> ]
> ```
>
> If the `"plugins"` key doesn't exist, create it as a top-level array.
> Then restart Cortex Code — the plugin will be fetched and cached automatically.

**4. Confirmation gate** — Use `AskUserQuestion`:

- **Header**: "Plugin check"
- **Question**: "The `snowflake-migration` plugin has been verified. Ready to proceed with SSIS migration assessment?"
- **Option A**: "Yes — continue to Phase 1" (proceed to Step 1.0)
- **Option B**: "I need help installing the plugin" (re-display instructions above)

**ENFORCEMENT RULES**:
- Do NOT proceed to Step 1.0 until `MEP_SKILL_DIR` resolves to a valid directory.
- Do NOT skip this step or treat it as optional.
- If the user explicitly declines to install the plugin, warn them that Phase 4 will fall back to manual EWI scanning (no automated dbt test-gen or fix loop) and ask for confirmation before continuing.

---

### Step 1.0: Gather Required Paths (MANDATORY FIRST)

**⚠️ MANDATORY STOPPING POINT**: You MUST collect ALL answers before doing any file reading, directory listing, or analysis. Do NOT skip or infer any answer. Do NOT proceed to Step 1.1 until every question below has been explicitly answered by the user.

> **ANNOUNCE PROGRESS** — output banner: Phase 1 of 5 — Assessment | `[█░░░░░░░░░░░░░░░░░░░]` 5% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — migration is paused until you respond.

**REQUIRED**: Use the `AskUserQuestion` tool to collect answers. Do NOT ask these as plain text — they must be asked via the tool so the user can respond structured. Ask all questions in a **single `AskUserQuestion` call** (up to 4 at a time). If more than 4 questions are needed, ask in two batches — wait for the first batch response before asking the second.

#### Batch 1 — SnowConvert availability + SSIS source + output path

Ask these 3 questions together:

1. **SnowConvert availability** (header: "SnowConvert", multiSelect: false)
   - Question: "Has SnowConvert AI already been run on these SSIS packages?"
   - Option A: "Yes, SnowConvert output exists" — SC has been run and output (converted SQL/dbt files, ETL CSVs) is available
   - Option B: "No — start from scratch" — SC has NOT been run; manual analysis from .dtsx files only

2. **SSIS source path** (header: "SSIS source", multiSelect: false) — only if NOT already provided in the user's message
   - Question: "Where are the .dtsx package files located? (provide full path)"
   - Offer reasonable defaults based on any path already mentioned in the conversation
   - If the user already provided the SSIS path in their initial message, skip this question and use the provided path.

3. **Assessment output path** (header: "Output path", multiSelect: false)
   - Question: "Where should all migration output files (assessment report, migration plan, generated SQL/dbt) be placed?"
   - Offer 2–3 reasonable path suggestions derived from the SSIS source path (e.g., a sibling folder named `ssis_migration_output`)

#### Batch 2 — SnowConvert paths (CONDITIONAL on Batch 1 answer) + Snowflake target details

After receiving Batch 1 answers, ask Batch 2. Include the SnowConvert path questions ONLY if the user answered "Yes, SnowConvert output exists" in Batch 1. Always include the Snowflake target questions.

4. **SnowConvert CSV path** (header: "CSV path", multiSelect: false) — **ONLY if SnowConvert output exists**
   - Question: "Where are ETL.Elements.csv and ETL.Issues.csv located? (typically inside Reports/SnowConvert/ within the SnowConvert output folder)"
   - If the user answers "N/A" or leaves blank, treat as no CSVs available and fall back to manual analysis.

5. **SnowConvert output path** (header: "SC output path", multiSelect: false) — **ONLY if SnowConvert output exists**
   - Question: "Where is the converted SQL/dbt output folder from SnowConvert? (typically Output/SnowConvert/ inside the SnowConvert output folder)"

6. **Target Snowflake database name** (header: "Target DB", multiSelect: false)
   - Question: "What Snowflake database name should be used as the migration target? (used in all DDLs, profiles.yml, and Task definitions)"
   - Offer 2–3 suggestions derived from the SSIS project name

7. **Target Snowflake schema name** (header: "Target schema", multiSelect: false)
   - Question: "What Snowflake schema should be used within the target database?"
   - Suggest: PUBLIC, and one domain-specific option derived from the project

8. **Snowflake warehouse name** (header: "Warehouse", multiSelect: false)
   - Question: "Which Snowflake warehouse should be used for queries, dbt runs, and Snowflake Task execution? (must already exist in the account)"
   - Suggest: COMPUTE_WH and 1–2 other common names

**ENFORCEMENT RULES**:
- Do NOT start reading `.dtsx` files until Step 1.0 is fully complete.
- Do NOT assume SnowConvert output exists or doesn't exist — always ask.
- Do NOT skip the SnowConvert CSV/output path questions if the user said "Yes, SnowConvert output exists."
- If the user provides a path that doesn't exist on disk, verify with `ls` and ask them to correct it before continuing.
- Record all collected values in a summary block before proceeding:

```
## Step 1.0 — Collected Inputs
- SSIS source path: <value>
- SnowConvert available: Yes / No
- SnowConvert CSV path: <value or N/A>
- SnowConvert output path: <value or N/A>
- Assessment output path: <value>
- Target database: <value>
- Target schema: <value>
- Snowflake warehouse: <value>
```

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

Present all option tables below to the user, then stop and wait for their selections.

#### Control Flow Orchestration
| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| Stored Procedures | Complex orchestration, file loops, conditional logic | Full control, native, no dependencies | More code |
| Tasks + Streams | Event-driven, simple orchestration | Serverless, auto-scaling | Limited complex logic |
| External Orchestrator | Enterprise scheduling, cross-platform | Rich monitoring | External dependency |

#### Data Flow Transformations
| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| dbt Models | Complex transforms, testing, lineage | Version control, testing framework; **Snowflake-native** (runs inside Snowflake, no local install needed) | Deployed via `snow dbt deploy` + executed via `EXECUTE DBT PROJECT` |
| Dynamic Tables | Continuous transforms, simple logic | Auto-refresh, declarative | Less flexible |
| SP SQL | Simple transforms, tight coupling | Single deployment unit | Harder to test |
| Hybrid | Mixed complexity | Best of both | More components |

> **dbt default execution mode**: When dbt Models is selected, dbt is **always deployed to Snowflake as a native object** using `snow dbt deploy` and executed inside Snowflake via `EXECUTE DBT PROJECT`. Do **not** run dbt locally unless the customer explicitly requests local dbt CLI during Phase 1 or Phase 2.

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

**⚠️ MANDATORY STOPPING POINT**: All options above have been presented. Collect user selections for each category.

> **ANNOUNCE PROGRESS** — output banner: Phase 2 of 5 — Migration Planning | `[█████░░░░░░░░░░░░░░░]` 25% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — migration is paused until you respond.

Record all selections. **Immediately log Phase 2 to `migration_phase_tracking.md`** (do NOT defer this — log Phase 2 before moving to Phase 3) with start datetime, end datetime, duration, and the following stats:

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

**REQUIRED**: Use the `AskUserQuestion` tool to collect approval. Do NOT accept a plain text acknowledgment as approval — it must go through the tool.

Ask the following question via `AskUserQuestion`:

- **Header**: "Plan Review"
- **Question**: "Please review `MIGRATION_PLAN.md` at `<OUTPUT_DIR>/MIGRATION_PLAN.md`. It contains all 13 sections including DAG diagrams, component mappings, EWI resolutions, script rewrites, and open decisions. Are you ready to approve and begin Phase 4 implementation?"
- **Option A**: "Approved — proceed with Phase 4 implementation"
- **Option B**: "Changes needed — I'll describe what to fix" (if selected, ask follow-up for the change description, update plan, and re-ask)
- **Option C**: "Pause — I need more time to review"  (if selected, run `cortex ctx task pause` and stop)

**ENFORCEMENT RULES**:
- Do NOT start Phase 4 if the user selects "Changes needed" — update `MIGRATION_PLAN.md` and re-present.
- Do NOT start Phase 4 if the user selects "Pause" — pause the task and wait.
- Only proceed when "Approved" is explicitly selected.

**Log** Phase 3 (including approval status) to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

#### Phase 3 Required Stats in Tracking File

1. **DAG Review Summary** — List of SnowConvert files reviewed with key observations (EWI markers found, components requiring manual rewrite, etc.)
2. **Component Mapping Summary** — Total components mapped across all packages, broken down by: Master_Orchestrator components → target, Setup components → target, Data_Load CF components → target, Data_Load DF components → target
3. **MIGRATION_PLAN.md Stats** — Line count, all 13 section titles confirmed written

---

## Phase 4: Implementation

> **ANNOUNCE PROGRESS** — output banner: Phase 4 of 5 — Implementation | `[████████████░░░░░░░░]` 60% | Status: In Progress

### Step 4.1: Gather Target Details

Confirm with user (if not already collected in Phase 1):
- Target Database/Schema
- **Snowflake SQL Connection name** — this is used for ALL `sql_execute` calls in Phase 5. Record it as `<CONNECTION_NAME>` (e.g., `COCO_JK`). Pass it explicitly to every `sql_execute` call via the `connection` parameter.
- Warehouse name

> **CONNECTION RULE**: Every `sql_execute` call throughout Phase 5 MUST include `connection=<CONNECTION_NAME>`. Never rely on the default active connection — always pass the user-confirmed name explicitly.

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
│   │   ├── profiles.yml      (⚠️ MUST NOT contain password/authenticator/token/private_key_path/env_var() — auth is handled by the Snowflake session)
│   │   ├── models/
│   │   ├── macros/
│   │   ├── seeds/
│   │   └── tests/
│   ├── test_data/
│   └── solution_artifacts_generated.md
```

**Key implementation patterns** — refer to `references/snowflake_patterns.md`:
- ForEachLoop → LIST + CURSOR or DIRECTORY(@stage) cursor
- Script Component (C#) → inline CASE expressions + Python or JavaScript UDFs (ask user preference) + seed table LEFT JOINs, all folded into the nearest downstream existing dbt model (do NOT create a new intermediate model)
- Script Task (C#) → Snowflake Scripting (IF/THEN/RAISE)
- SQL OUTPUT clause → sequence + NEXTVAL or MAX(id)
- SSIS expressions (REVERSE, FINDSTRING) → SPLIT_PART
- File System Tasks → COPY FILES + REMOVE via EXECUTE IMMEDIATE
- Send Mail Task → SYSTEM$SEND_EMAIL + Notification Integration
- Bulk Insert Task → COPY INTO + inline FILE_FORMAT
- CreateDirectory → .dummy placeholder file
- Data Flow Task → dbt project (deploy via `snow dbt deploy`, execute via `EXECUTE DBT PROJECT`) or SP inline SQL — **default is Snowflake-native dbt** unless customer explicitly chose local dbt CLI in Phase 2

Write `<OUTPUT_DIR>/implementation/solution_artifacts_generated.md` listing all files with source attribution (SnowConvert-adopted / patched / new) and EWI resolution mapping.

### Step 4.2a: Parallel SQL Generation for Multi-Package Projects (when ≥ 3 SSIS packages)

When the migration includes **3 or more SSIS packages** that each produce independent SQL scripts (e.g., separate orchestrator SPs, separate staging tables, separate file format scripts), spawn **one `general-purpose` background agent per package** to generate or patch that package's files in parallel. Each agent operates on an isolated subdirectory.

**When to use this step**: Only when packages produce independent output files with no shared state. If packages share a single orchestrator SP or a single database schema, generate sequentially in Step 4.2 instead.

**Spawn pattern**:
```
For each package <pkg_name> in packages (index 0..N-1):
  Spawn background general-purpose agent with:
  - Task: Generate/patch SQL files for SSIS package <pkg_name>
  - Input: MIGRATION_PLAN.md, SnowConvert output for <pkg_name>, references/snowflake_patterns.md
  - Output dir: <OUTPUT_DIR>/implementation/sql/<pkg_name>/
  - Instruction: apply Strategy A (or B if no SC output), write numbered scripts,
                 resolve all !!!RESOLVE EWI!!! markers, write pkg_artifacts_<pkg_name>.md
  - Max agents per wave: 5
```

**Agent Wait Protocol**: End your turn after spawning. Do not proceed until all agents return.

After all agents return:
1. Validate each agent produced `pkg_artifacts_<pkg_name>.md` (artifact manifest)
2. Merge all pkg_artifacts files into the top-level `solution_artifacts_generated.md`
3. Check for naming conflicts (duplicate table names, duplicate stage names across packages) — resolve before proceeding

**Constraint**: `snow dbt deploy` and SQL script deployment in Phase 5 remain sequential — this parallelism applies to **generation only**, not deployment.

### Step 4.2b: Prepare and Deploy dbt Project to Snowflake (MANDATORY when dbt selected)

> **Skip this step only if** the customer explicitly chose "Local dbt CLI" during Phase 1 or Phase 2.

Snowflake-native dbt authenticates via the active Snowflake session. Before deploying, the `profiles.yml` **must** be stripped of any auth fields.

**1. Migrate `profiles.yml`** — remove all of the following fields if present:

```yaml
# REMOVE these fields entirely — they are not valid for Snowflake-native dbt:
# user, password, token, private_key_path, private_key_passphrase
# authenticator: externalbrowser  ← remove this; replace with authenticator: oauth (see below)
# Also remove any env_var() calls in profiles.yml or dbt_project.yml vars
```

Correct minimal `profiles.yml` for Snowflake-native deployment:

```yaml
<profile_name>:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account_identifier>   # required — update per deployment environment
      authenticator: oauth             # required — inherits session user at EXECUTE DBT PROJECT runtime
      role: <role>
      database: <database>
      warehouse: <warehouse>
      schema: <schema>
      threads: 4
```

> **Why `authenticator: oauth`**: `snow dbt deploy` validates `profiles.yml` before packaging. At `EXECUTE DBT PROJECT` runtime, dbt runs inside Snowflake's execution environment where browser-based auth (`externalbrowser`) is not available. `oauth` tells the dbt adapter to use the current session's OAuth token. Omitting `authenticator` entirely may also work on some versions, but `oauth` is the explicit, safe choice. Do **not** use `externalbrowser`, `username_password_mfa`, or any user/password combination.

Also replace any `env_var('KEY')` in `dbt_project.yml` vars with literal values or `{{ var('key') }}` patterns (values supplied at runtime via `ARGS='build --vars ...'`).

**2. Deploy dbt project to Snowflake:**

```bash
snow dbt deploy <PROJECT_NAME> \
  --source <path_to_dbt_project> \
  --database <TARGET_DATABASE> \
  --schema <TARGET_SCHEMA> \
  --connection <CONNECTION_NAME>
```

**3. Verify deployment:**

```bash
snow dbt list --in schema <TARGET_SCHEMA> --database <TARGET_DATABASE> --connection <CONNECTION_NAME>
```

The deployed project appears as a Snowflake object at `<DATABASE>.<SCHEMA>.<PROJECT_NAME>`. It is executed by the orchestrator SP via `EXECUTE DBT PROJECT <DATABASE>.<SCHEMA>.<PROJECT_NAME> ARGS='build'`.

**4.** Update `solution_artifacts_generated.md` to record the deployed dbt project object name.

### Step 4.3: dbt Seed Type Safety (MANDATORY when dbt project exists)

Before running the dbt TDD Fix Loop, ensure every seed CSV has explicitly declared column types in `dbt_project.yml`. This prevents silent `dbt seed` type mismatch errors — dbt auto-infers numeric-looking values as `NUMBER(38,0)`, causing JOIN failures when a `VARCHAR`-derived expression is compared to that column.

**Rules — always declare explicit `+column_types` for these patterns:**

| CSV column pattern | Required `+column_types` value |
|---|---|
| Date values (`YYYY-MM-DD`) | `date` |
| Timestamps | `timestamp_ntz` |
| JSON / nested structures | `variant` |
| `true` / `false` | `boolean` |
| Large integers (>9 digits) | `bigint` |
| Mixed numeric/string (TAC, IMEI prefixes, codes) | `varchar` |

**Template — add to every `dbt_project.yml` when seeds exist:**

```yaml
seeds:
  <project_name>:
    "<seed_folder_name>":
      +schema: "{{ target.schema }}"
      +tags: ["migrate_etl_package_test"]
      +column_types:
        <date_col>: date
        <timestamp_col>: timestamp_ntz
        <json_col>: variant
        <prefix_col>: varchar       # prevents NUMBER cast in JOIN conditions
```

**Verification — run before proceeding to Step 4.4:**

```bash
grep -rn "column_types\|+schema:" <DBT_PROJECT_PATH>/dbt_project.yml
```

If `column_types` is absent and the seed CSV contains any of the above column patterns, add the block now. Do not skip — type mismatches surface as cryptic runtime errors during `dbt seed`, not as compilation errors.

---

### Step 4.4: dbt TDD Fix Loop (MANDATORY when dbt project exists)

Replaces manual EWI marker scanning for dbt models. Runs `dbt-test-gen` then `dbt-fixer` for each dbt project generated in Step 4.2, fixing compilation errors and EWI markers iteratively (up to 5 attempts per node) before any deployment.

> **Skip this step only if** no `dbt_project.yml` was generated in Step 4.2, OR if `MEP_SKILL_DIR` could not be resolved (fall back to Step 4.5 manual validation only).

#### Step 4.4.0: Detect dbt Projects and Resolve MEP_SKILL_DIR

```bash
find <OUTPUT_DIR>/implementation -name "dbt_project.yml" 2>/dev/null
```

If nothing found: skip to Step 4.5.

```bash
MEP_SKILL_DIR=$(find ~/.snowflake/cortex/remote-cache \
  -path "*/migrate-objects/actions/migrate-etl-package" \
  -maxdepth 8 -type d 2>/dev/null | head -1)
echo "MEP_SKILL_DIR=$MEP_SKILL_DIR"
```

If empty: warn the user and skip to Step 4.5 manual validation.

#### Step 4.4.1: Initialize Tracking

```bash
mkdir -p <OUTPUT_DIR>/.ssis-dbt-tracking
```

Register each dbt project — **sequential, one call per project**:

```bash
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  init-dbt <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json \
  <project_name> <dbt_project_path>
```

#### Step 4.4.2: dbt-Test-Gen Wave (parallel)

Use `team_create` tool: `team_name="ssis-dbt-tdd-<package_name>"`.

Spawn **one `general-purpose` background agent per dbt project** — **unconditional, no exceptions**. The agent handles broken projects (placeholder config, broken macros, missing sources) internally and documents all blockers in `test_report.md`. Early exit without test artifacts is a protocol violation.

For each project spawn with `run_in_background=true`:
- Instruction: Read `$DBT_TEST_GEN_SKILL` Task Mode section
- Context: `project_path=<DBT_PROJECT_PATH>`, `source_file=<DTSX_FILE_PATH>`, `transformation_guide=$TRANSFORMATION_GUIDE`, `session_status_json=<OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json`, `ROADMAP_path=<OUTPUT_DIR>/.ssis-dbt-tracking/ROADMAP.md`

**Max 5 agents per wave.** If >5 projects: spawn first 5, follow Agent Wait Protocol, then spawn remaining.

**Follow the Agent Wait Protocol: end your turn immediately after spawning. Do NOT issue further tool calls.**

After notifications arrive — **validate ALL of these for EACH project**:
- `<OUTPUT_DIR>/.migrate-etl-package/tests/dbt/<project>/seeds/` — at least 1 `.csv`
- `<OUTPUT_DIR>/.migrate-etl-package/tests/dbt/<project>/tests/` — at least 1 `.sql`
- `<OUTPUT_DIR>/.migrate-etl-package/tests/dbt/<project>/test_report.md` — must exist

If ANY artifact is missing: respawn that project's agent (max 2 retries). After 2 retries: mark project nodes `failed` with reason `test-gen-exhaustion`.

Update tracking — **sequential, one call per project**:

```bash
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  update-dbt <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json \
  <project_name> --status dbt-tested
```

#### Step 4.4.3: Wave Checkpoint

Before spawning fix agents, re-verify:
1. Re-read `session_status.json` — confirm ALL projects have status `dbt-tested`
2. Glob `seeds/*.csv` and `tests/*.sql` for each project — confirm counts > 0
3. If any project is still `pending`: do NOT proceed — respawn its test-gen agent

#### Step 4.4.3b: Pre-apply Known Fixes from fix_log (MANDATORY before spawning fix agents)

Before spawning any fix agent, check whether `fix_log.md` already contains patterns matching the current project's file type or error class. Apply known fixes **before** first deployment — not after the first failure.

```bash
# Check if fix_log has any SQL/SP entries relevant to this project
grep -i "SQL/SP Fix\|LIST.*PATTERN\|COPY FILES\|IDENTITY\|ErrorCode\|ErrorColumn" \
  <OUTPUT_DIR>/.ssis-dbt-tracking/fix_log.md
```

For each matching SQL/SP fix entry:
1. Identify which files in `implementation/sql/` match the "Pre-apply to" field
2. Apply the fix to those files **now**, before the fix agent runs
3. Log applied fixes in the agent instruction so the agent knows not to re-apply

**Why this matters**: Without pre-application, the fix agent deploys, hits the known error, diagnoses it, and re-applies the fix — adding an unnecessary round trip. Pre-applying converts a reactive loop into a proactive pass.

#### Step 4.4.4: dbt-Fix Wave (parallel)

Spawn **one `general-purpose` background agent per project** that has ANY of:
- Failing tests (test-failed nodes in `test_report.md`)
- Compilation errors (documented in `test_report.md` `bootstrap_status`)
- `!!!RESOLVE EWI!!!` markers in dbt model or macro files

**For projects where ALL nodes passed and NO compilation errors**: no agent needed. Write a minimal file:

```
<OUTPUT_DIR>/.ssis-dbt-tracking/dbt_learnings_<project_name>.md
content: "## no-fix-needed\nAll nodes passed baseline. No fixes required."
```

For each project needing fixes, spawn with `run_in_background=true`:
- Instruction: Read `$DBT_FIXER_SKILL` Task Mode section
- Context: same as test-gen + `test_report_path=<OUTPUT_DIR>/.migrate-etl-package/tests/dbt/<project>/test_report.md`, `original_backup_path=<OUTPUT_DIR>/.migrate-etl-package/original/`

**Max 5 agents per wave.** Follow Agent Wait Protocol: end your turn after spawning.

After notifications arrive — **validate**:
- `<OUTPUT_DIR>/.ssis-dbt-tracking/dbt_learnings_<project_name>.md` exists for EVERY project

Update tracking — **sequential**:

```bash
# Per node:
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  update-dbt-node <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json \
  <project_name> <node_name> --status <fixed|failed|skipped>

# Per project:
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  update-dbt <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json \
  <project_name> --status dbt-fixed
```

#### Step 4.4.5: Merge Learnings

Read all `<OUTPUT_DIR>/.ssis-dbt-tracking/dbt_learnings_*.md`. Append new patterns (no duplicates) to `<OUTPUT_DIR>/.ssis-dbt-tracking/fix_log.md`.

**Also capture SQL/SP fix patterns** — any fix applied to a stored procedure, setup SQL, or data load SQL during this phase must be recorded in `fix_log.md` so subsequent files receive the same fix pre-applied rather than failing first.

Format for SQL/SP entries:

```markdown
## SQL/SP Fix: <short title>
- **File(s) affected**: `implementation/sql/<filename>.sql`
- **Root cause**: <one sentence>
- **Symptom**: <what failed or returned wrong results>
- **Fix**: <exact change made>
- **Pre-apply to**: any SP that uses `<pattern>` — apply fix before first deployment

### Example
## SQL/SP Fix: LIST stage PATTERN depth mismatch after COPY FILES
- **File(s) affected**: `12_setup_sp.sql`, `13_data_load_sp.sql`
- **Root cause**: COPY FILES flattens subdirectory paths to stage root
- **Symptom**: LIST @stage PATTERN = '.*/.*\\.csv' returns 0 rows; pipeline processes 0 files
- **Fix**: Change PATTERN = '.*/.*\\.csv' → PATTERN = '.*\\.csv'
- **Pre-apply to**: any SP that LISTs a stage after COPY FILES restore
```

#### Step 4.4.6: Phase Gate (MANDATORY)

```bash
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  validate-phase <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json 4
```

**⚠️ Do NOT proceed to Step 4.5 if this gate fails.** Report any blocking nodes to the user with their failure reason. The user may explicitly mark a node `needs-user` to allow forward progress.

#### Step 4.4.7: Shutdown Team

Send `shutdown_request` to each agent → wait for `shutdown_response` notifications → call `team_delete`.

**Log the dbt TDD Fix Loop outcome to `migration_phase_tracking.md`:**
- Projects processed, nodes fixed vs failed
- Fix log: `<OUTPUT_DIR>/.ssis-dbt-tracking/fix_log.md`

---

### Step 4.4-SP: SP SQL TDD Fix Loop (MANDATORY when SP SQL selected for transformations)

Replaces manual EWI marker scanning for stored procedure SQL. Runs `sp-test-gen` then `sp-fixer` for each SP generated in Step 4.2, fixing compilation errors, EWI markers, and logic bugs iteratively (up to 5 attempts per SP) before any deployment.

> **Skip this step only if** dbt was selected in Phase 2 for Data Flow Transformations (use Step 4.4 instead), OR no SP SQL transformation files exist.

**Sub-skill references:**

```bash
SP_TDD_DIR=~/.snowflake/cortex/skills/ssis-migration/sp-tdd
SP_TEST_GEN_SKILL=$SP_TDD_DIR/sp-test-gen/SKILL.md
SP_FIXER_SKILL=$SP_TDD_DIR/sp-fixer/SKILL.md
TRACK_SP_STATUS_PY=~/.snowflake/cortex/skills/ssis-migration/scripts/track_sp_status.py
SP_TEST_PATTERNS=~/.snowflake/cortex/skills/ssis-migration/references/sp_test_patterns.md
```

#### Step 4.4-SP.0: Detect SP SQL Files

```bash
find <OUTPUT_DIR>/implementation/sql/ -name "*sp_*" -o -name "*proc_*" -o -name "*procedure*" | grep -i "\.sql$"
```

If nothing found: skip to Step 4.5.

#### Step 4.4-SP.1: Initialize SP Tracking

Register each SP in the session status file — **sequential, one call per SP**:

```bash
python3 $TRACK_SP_STATUS_PY init \
  <OUTPUT_DIR>/.sp-tdd/session_status.json \
  <sp_name> \
  <sp_file_path>
```

#### Step 4.4-SP.2: sp-test-gen Wave (parallel)

Use `team_create` tool: `team_name="ssis-sp-tdd-<package_name>"`.

Spawn **one `general-purpose` background agent per SP** (or per logical SP group if SPs are tightly coupled). Each agent generates test data + assertions from source `.dtsx` analysis.

For each SP, spawn with `run_in_background=true`:
- Instruction: Read `$SP_TEST_GEN_SKILL` Task Mode section
- Context: `sp_name=<SP_NAME>`, `sp_file=<SP_FILE_PATH>`, `source_file=<DTSX_FILE_PATH>`, `migration_plan=<OUTPUT_DIR>/MIGRATION_PLAN.md`, `session_status_json=<OUTPUT_DIR>/.sp-tdd/session_status.json`, `test_patterns=$SP_TEST_PATTERNS`, `target_database=<DB>`, `target_schema=<SCHEMA>`, `warehouse=<WH>`, `connection=<CONNECTION_NAME>`

**Max 5 agents per wave.** If >5 SPs: spawn first 5, follow Agent Wait Protocol, then spawn remaining.

**Follow the Agent Wait Protocol: end your turn immediately after spawning. Do NOT issue further tool calls.**

After notifications arrive — **validate ALL of these for EACH SP**:
- `<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/seeds/` — at least 1 `.sql` file
- `<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/assertions/` — at least 1 `.sql` file
- `<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/test_harness_<sp_name>.sql` — must exist
- `<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/test_report.md` — must exist

If ANY artifact is missing: respawn that SP's agent (max 2 retries). After 2 retries: mark SP `failed` with reason `test-gen-exhaustion`.

Update tracking — **sequential, one call per SP**:

```bash
python3 $TRACK_SP_STATUS_PY update \
  <OUTPUT_DIR>/.sp-tdd/session_status.json \
  <sp_name> --status sp-tested
```

#### Step 4.4-SP.3: Wave Checkpoint

Before spawning fix agents, re-verify:
1. Re-read `session_status.json` — confirm ALL SPs have status `sp-tested`
2. Glob test artifacts for each SP — confirm files exist
3. If any SP is still `pending`: do NOT proceed — respawn its test-gen agent

#### Step 4.4-SP.4: sp-fixer Wave (parallel)

Spawn **one `general-purpose` background agent per SP** that has ANY of:
- Failing assertions (recorded in `test_report.md`)
- Compilation errors (documented in `test_report.md` `bootstrap_status`)
- `!!!RESOLVE EWI!!!` markers in SP SQL file

**For SPs where ALL assertions passed and NO compilation errors**: no agent needed. Update status directly:

```bash
python3 $TRACK_SP_STATUS_PY update \
  <OUTPUT_DIR>/.sp-tdd/session_status.json \
  <sp_name> --status test-passed
```

For each failing SP, spawn with `run_in_background=true`:
- Instruction: Read `$SP_FIXER_SKILL` Task Mode section
- Context: `sp_name=<SP_NAME>`, `sp_file=<SP_FILE_PATH>`, `source_file=<DTSX_FILE_PATH>`, `test_report_path=<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/test_report.md`, `test_harness_path=<OUTPUT_DIR>/.sp-tdd/tests/<sp_name>/test_harness_<sp_name>.sql`, `session_status_json=<OUTPUT_DIR>/.sp-tdd/session_status.json`, `target_database=<DB>`, `target_schema=<SCHEMA>`, `warehouse=<WH>`, `connection=<CONNECTION_NAME>`

**Follow the Agent Wait Protocol: end your turn immediately after spawning.**

After all agents return:
1. Read each agent's learnings file (`sp_learnings_<sp_name>.md`)
2. Merge all learnings into `sp_fix_log.md`:
   ```bash
   python3 $TRACK_SP_STATUS_PY merge-learnings \
     <OUTPUT_DIR>/.sp-tdd/session_status.json \
     <OUTPUT_DIR>
   ```
3. Update status for each SP based on agent results:
   ```bash
   python3 $TRACK_SP_STATUS_PY update \
     <OUTPUT_DIR>/.sp-tdd/session_status.json \
     <sp_name> --status <fixed|failed|needs-user> [--reason "<reason>"]
   ```

#### Step 4.4-SP.5: Phase Gate (MANDATORY)

```bash
python3 $TRACK_SP_STATUS_PY validate-phase \
  <OUTPUT_DIR>/.sp-tdd/session_status.json 4
```

**Gate passes when**: every SP has a terminal status (`test-passed`, `fixed`, `failed`, `needs-user`, `skipped`). Zero SPs in `pending`, `sp-tested`, or `fixing`.

**Gate fails**: report the blocking SPs to the user. Do NOT proceed to Step 4.5 until they are resolved or the user explicitly accepts them as `needs-user`.

#### Step 4.4-SP.6: Shutdown Team

Send `shutdown_request` to each agent → wait for `shutdown_response` notifications → call `team_delete`.

**Log the SP TDD Fix Loop outcome to `migration_phase_tracking.md`:**
- SPs processed, fixed vs failed
- Fix log: `<OUTPUT_DIR>/.sp-tdd/sp_fix_log.md`

---

### Step 4.5: Post-SnowConvert dbt Validation Checklist (MANDATORY before Phase 5)

> **Run this checklist on every SSIS migration that uses dbt**, regardless of project. These patterns are universal SnowConvert output issues that only surface at runtime with real data — they are undetectable by static SQL review or `snow dbt deploy`. Running this checklist reduces E2E debugging from 10+ SP call attempts to 1–2.

#### A. Grep checks — run on every project

**1. SSIS error output columns**
```bash
grep -rn "ErrorCode\|ErrorColumn\|Flat File Source Error Output Column" models/
```
Any model that **selects** these columns from another dbt model (not defining them as `NULL::INT AS ErrorCode`) must be rewritten. These are SSIS OLE DB/Flat File Destination error output port columns — they have no Snowflake equivalent.

**Fix pattern** — replace the entire model body with a `WHERE FALSE` stub:
```sql
{{ config(materialized='view') }}
SELECT
    CAST(NULL AS INT)  AS ErrorCode,
    CAST(NULL AS INT)  AS ErrorColumn,
    -- ... other columns expected downstream ...
WHERE FALSE
```
Apply the same fix to all downstream models that read those columns.

**2. Zero-datetime numeric cast**
```bash
grep -rn "::NUMERIC = 0\|::NUMBER = 0\|CAST.*AS NUMBER.*= 0" models/
```
SnowConvert translates SSIS's uninitialized DateTime value (`0`) as a numeric comparison on a timestamp column. Snowflake cannot cast `TIMESTAMP_NTZ` to `NUMERIC`.

**Fix:** Replace `IFF((col)::NUMERIC = 0, NULL::TIMESTAMP_NTZ, col)` with `IFF(col IS NULL, NULL::TIMESTAMP_NTZ, col)`

**3. Ephemeral materialization with pre-hooks**
```bash
grep -rn "ephemeral" dbt_project.yml models/
```
If any model uses `materialized='ephemeral'` AND other models reference it in `pre_hook` SQL (e.g., `m_update_row_count_variable`), change it to `view`. Ephemeral CTEs only exist within the single SQL statement that runs the model — pre-hook SQL runs separately and cannot see them.

**Fix:** In `dbt_project.yml`, change all `+materialized: ephemeral` under `intermediate:` to `+materialized: view`.

**4. `COMMENT` clause in Python SP DDL**
```bash
grep -n "COMMENT" sql/11_create_sp*.sql sql/*process_files*.sql 2>/dev/null
```
SnowConvert generates `CREATE PROCEDURE ... COMMENT = '...'` at a position the Snowflake Python SP DDL parser rejects.

**Fix:** Remove the `COMMENT = '...'` line from any Python SP `CREATE OR REPLACE PROCEDURE` DDL.

#### B. Manual checks — verify against actual source data

**5. VARCHAR widths**

Check staging model column widths against actual sample data:

| Column | Common SSIS metadata width | Actual width | Risk |
|--------|--------------------------|--------------|------|
| IMEI | `DT_STR(14)` | **15 digits** | Always wrong |
| MSISDN / phone | `DT_STR(10)` | Up to 15 | Often wrong |
| event_type / type codes | `DT_STR(1)` | 2–3 chars | Common |
| description fields | `DT_STR(50)` | 100–255 | Check source |

**Fix:** Query the stage directly with `SELECT $N FROM @stage (FILE_FORMAT => ...)` and measure max `LEN($N)` per column before setting `::VARCHAR(N)` casts.

**6. File format SKIP_HEADER and record delimiter**

```sql
SHOW FILE FORMATS LIKE '<format_name>' IN SCHEMA <db>.<schema>;
```

Verify:
- `SKIP_HEADER` = **1** if the CSV has a header row (almost always true)
- `RECORD_DELIMITER` matches actual file line endings:
  - Windows CRLF files: `\r\n` (but test with `\n` if `MULTI_LINE=true` causes issues)
  - Unix LF files: `\n`
  - `MULTI_LINE=true` combined with `\r` delimiter can cause the entire file to parse as one row

**Quick test:** Run `SELECT $1, $2, ... FROM @stage/file.csv (FILE_FORMAT => ...) LIMIT 5` without type casts. If the first row is the header, add `SKIP_HEADER=1`. If `$7` contains `\n` followed by the next row's first field, the record delimiter is wrong.

**7. profiles.yml — no auth fields**

```bash
grep -n "user:\|password:\|authenticator:\|token:\|private_key\|env_var" dbt_project/profiles.yml
```

Snowflake-native dbt (`snow dbt deploy`) auth is handled by the Snowflake session — **no user/password/token/authenticator fields are allowed**. Remove any of these lines.

Valid `profiles.yml` contains only: `type`, `account`, `role`, `database`, `warehouse`, `schema`, `threads`.

#### C. Summary table

| Check | Command | Fix if found |
|-------|---------|-------------|
| SSIS error columns in SELECT | `grep -rn "ErrorCode\|ErrorColumn" models/` | Replace model body with `WHERE FALSE` stub |
| Zero-datetime numeric cast | `grep -rn "::NUMERIC = 0"` | Change to `IS NULL` check |
| Ephemeral + pre-hooks | `grep -rn "ephemeral" dbt_project.yml` | Change to `view` |
| COMMENT in Python SP DDL | `grep -n "COMMENT" sql/*sp*.sql` | Remove line |
| VARCHAR widths | Query stage with `$N` positional cols | Fix widths to match actual data |
| SKIP_HEADER=0 on header CSV | `SHOW FILE FORMATS` | Set `SKIP_HEADER=1`, fix `RECORD_DELIMITER` |
| Auth fields in profiles.yml | `grep "user:\|password:\|token:"` | Remove those lines |
| dbt seed JOIN type safety | `grep -rn "LEFT JOIN.*_map\|JOIN.*_mapping" models/` | Cast seed column to VARCHAR (see check 8 below) |
| IDENTITY columns on dbt tables | `grep -rn "IDENTITY\|AUTOINCREMENT" sql/*tables*.sql` | Remove IDENTITY from dbt-managed tables (see check 9 below) |
| LIST @stage PATTERN uses `.*/.*` | `grep -rn "LIST @" sql/*.sql` | Replace `.*/.*\\.csv` with `.*\\.csv` (see check 10 below) |

**8. dbt seed column type safety in JOIN conditions**

dbt auto-infers numeric-looking seed columns as `NUMBER(38,0)`. Any JOIN where a `VARCHAR`-derived expression (e.g. `SUBSTR(col, 1, 5)`) is compared against such a column will fail for non-numeric inputs — Snowflake casts the VARCHAR to NUMBER.

```bash
grep -rn "LEFT JOIN.*tac_\|LEFT JOIN.*lac_\|JOIN.*_mapping" models/
```

**Fix:** Cast seed columns to `::VARCHAR` in JOIN conditions:
```sql
-- Unsafe — fails for non-numeric IMEI values like 'INVALID_IMEI_XX'
ON SUBSTR(p.tac, 1, 5) = t.tac_prefix
-- Safe
ON SUBSTR(p.tac, 1, 5) = t.tac_prefix::VARCHAR AND LENGTH(t.tac_prefix::VARCHAR) = 5
```

Apply this to every seed JOIN where the left-hand side comes from a stage-read VARCHAR column.

**9. IDENTITY columns on dbt-managed tables**

dbt incremental models introspect the target table schema at run time. An `IDENTITY` (autoincrement) column named `id` causes dbt to attempt `SELECT id FROM <staging_view>` — which fails because the staging view does not expose `id`.

```bash
grep -rn "IDENTITY\|AUTOINCREMENT" sql/*tables*.sql
```

**Fix:** Remove `IDENTITY`/`AUTOINCREMENT` from every table that dbt writes to. Generate surrogate keys via a sequence `NEXTVAL` in the orchestrator SP instead:
```sql
-- In the SP, before calling dbt:
LET audit_id INT := TELECOM_ETL.PUBLIC.SEQ_AUDIT_ID.NEXTVAL;
```

> Completing this checklist before Step 5 (E2E testing) is the single highest-ROI action in an SSIS migration. It converts a 10+ iteration debugging loop into a 1–2 attempt first run.

**10. LIST @stage PATTERN — use depth-independent regex**

Stored procedures that loop over stage files commonly use:

```bash
grep -rn "LIST @" sql/*.sql
```

Look for any pattern containing `.*/.*` (requires subdirectory separator):
```sql
LIST @stage PATTERN = '.*/.*\\.csv';  -- FRAGILE: matches 'dir/file.csv' but NOT 'file.csv'
```

This pattern fails silently when files are at the stage root — `RESULT_SCAN` returns 0 rows, the ForEachLoop cursor processes nothing, and no error is raised.

**When this occurs**: `COPY FILES` used to restore files from a processed stage does **not** preserve subdirectory paths. Files originally at `batch_0/file.csv` land at `file.csv` (stage root) after `COPY FILES INTO @source_stg/`.

**Fix:** Replace all `.*/.*\\.csv` patterns with `.*\\.csv` — matches files at any depth:
```sql
LIST @stage PATTERN = '.*\\.csv';    -- SAFE: matches root-level and subdir files
```

Apply this fix to every SP that iterates over stage files using `LIST` + `RESULT_SCAN`.

---

### Step 4.5b: Generate Project-Specific rerun_reset.sql (MANDATORY)

After completing the validation checklist, generate `<OUTPUT_DIR>/implementation/sql/rerun_reset.sql` populated with the **actual** object names discovered during migration. This file is the runnable reset script for E2E re-testing — it replaces the generic Step 5.4 template with project-specific SQL.

The agent must substitute all placeholders using values from `MIGRATION_PLAN.md` and the generated SQL scripts:

```sql
-- ============================================================
-- rerun_reset.sql  — generated by ssis-migration skill
-- Purpose: Reset pipeline state for a clean E2E re-run
-- Run IN ORDER. Skipping any step causes 0-row runs.
-- ============================================================

-- 1. Restore source files from processed stage
--    (COPY FILES flattens paths — files land at stage root, not in subdirs)
COPY FILES INTO @<actual_source_stage>/
  FROM @<actual_processed_stage>/;

-- 2. Refresh directory metadata (MANDATORY after COPY FILES)
ALTER STAGE <actual_source_stage> REFRESH;

-- 3. Truncate target tables
TRUNCATE TABLE <actual_database>.<actual_schema>.<fact_table>;
TRUNCATE TABLE <actual_database>.<actual_schema>.<audit_table>;

-- 4. Reset batch counter
UPDATE <actual_database>.<actual_schema>.<control_table>
SET VARIABLE_VALUE = TO_VARIANT(0)
WHERE VARIABLE_NAME = '<batch_id_variable_name>'
  AND VARIABLE_SCOPE = '<batch_id_scope>';

-- 5. Trigger pipeline
EXECUTE TASK <actual_database>.<actual_schema>.<root_task_name>;
```

**How to populate the placeholders:**
| Placeholder | Where to find it |
|---|---|
| `<actual_source_stage>` | Stage created in setup SQL (grep `CREATE STAGE` in sql/) |
| `<actual_processed_stage>` | Stage for processed files (grep `processed` in setup SQL) |
| `<fact_table>` | Main insert target (grep `INSERT INTO` in data_load SP) |
| `<audit_table>` | Audit/log table (grep `DIM_AUDIT\|audit` in SP) |
| `<control_table>` | Control variable table (grep `CONTROL_VARIABLES\|control` in SP) |
| `<batch_id_variable_name>` | Variable name in control table (grep `batch_id\|User_batch` in SP) |
| `<root_task_name>` | Root task (grep `CREATE TASK.*SCHEDULE\|EXECUTE TASK` in tasks SQL) |

Write this file to `implementation/sql/rerun_reset.sql` and list it in `solution_artifacts_generated.md`.

---

### Step 4.6: User Approval Gate (MANDATORY)

**⚠️ MANDATORY STOPPING POINT**: Phase 5 MUST NOT begin until user explicitly approves the generated implementation code.

> **ANNOUNCE PROGRESS** — output banner: Phase 4 of 5 — Implementation | `[████████████████░░░░]` 80% | Status: Awaiting User Input
> ⚠️ WAITING FOR YOUR INPUT — review all generated scripts before any deployment begins.

**REQUIRED**: Use the `AskUserQuestion` tool to collect approval. Do NOT proceed to Phase 5 based on a plain text response — approval must come through the tool.

First, present a summary of all generated files:
- File count by category (SQL, dbt models, seeds, UDFs, SPs, test data)
- Which files are SC-as-is, SC-modified, fully rewritten, or new/manual
- Key design decisions made (e.g., column width changes, nullable adjustments, SP renames)
- Any deviations from `MIGRATION_PLAN.md` and why
- Output directory path and full file tree

Then ask the following via `AskUserQuestion`:

- **Header**: "Scripts Review"
- **Question**: "All implementation files have been generated at `<OUTPUT_DIR>/implementation/`. Please review the SQL scripts, dbt models, UDFs, and SPs. Are you ready to approve and proceed to Phase 5 deployment and testing?"
- **Option A**: "Approved — deploy to Snowflake and run E2E tests"
- **Option B**: "Changes needed — I'll describe what to fix" (if selected, ask follow-up, update files, re-present summary, and re-ask)
- **Option C**: "Pause — I need more time to review" (if selected, run `cortex ctx task pause` and stop)

**ENFORCEMENT RULES** — These are absolute constraints, not guidelines:
- **HARD STOP — FORBIDDEN until "Approved" is explicitly selected via `AskUserQuestion`**: Do NOT call `sql_execute`, do NOT run `snow dbt deploy`, do NOT execute any `bash` command that connects to Snowflake, and do NOT use any other tool that creates or modifies Snowflake objects. No exceptions — even if all files are generated and ready.
- A plain-text "yes", "go ahead", or similar conversational message does NOT count as approval. Approval must come as an explicit "Approved" option selection through the `AskUserQuestion` tool.
- Do NOT start Phase 5 if the user selects "Changes needed" — update the files and re-present.
- Do NOT start Phase 5 if the user selects "Pause" — run `cortex ctx task pause` and stop.
- Only proceed to Phase 5 when "Approved" is explicitly selected through the tool.

**Log** Phase 4 (including approval status) to `migration_phase_tracking.md` with start datetime, end datetime, duration, and the following stats:

**⚠️ HARD ENFORCEMENT — LOG BEFORE PROCEEDING**: You MUST write the Phase 4 entry to `migration_phase_tracking.md` BEFORE spawning any Phase 5 deployment agents, calling `sql_execute`, or creating a team. If you skip this step, Phase 5 will proceed with an incomplete tracking file — this is a protocol violation. Write the log entry as your NEXT tool call after receiving the "Approved" answer.

#### Phase 4 Required Stats in Tracking File

1. **Configuration Inputs** — Database, Warehouse, Schemas, Connection name, and approach selections
2. **Generated Deliverables** — Numbered table with columns: #, File, Type, Description (list every generated file)
3. **Total File Count** by category (SQL scripts, dbt models/configs, test data, docs)

---

## Phase 5: Validation & Testing

> **ANNOUNCE PROGRESS** — output banner: Phase 5 of 5 — Validation & Testing | `[█████████████████░░░]` 85% | Status: In Progress

### Step 5.0: Pre-Deployment Gate (MANDATORY)

Run these checks before executing any SQL against Snowflake. **Do NOT proceed to Step 5.1 if any check fails.**

**1. dbt node terminal status** (only if Step 4.4 ran):

```bash
uv run --project $MEP_SKILL_DIR python $TRACK_STATUS_PY \
  validate-phase <OUTPUT_DIR>/.ssis-dbt-tracking/session_status.json 4
```

All dbt nodes must have terminal status. Zero nodes in `pending` or `dbt-tested`. If this fails, return to Step 4.4 and resolve or mark blocking nodes as `needs-user`.

**1b. SP terminal status** (only if Step 4.4-SP ran):

```bash
python3 ~/.snowflake/cortex/skills/ssis-migration/scripts/track_sp_status.py \
  validate-phase <OUTPUT_DIR>/.sp-tdd/session_status.json 4
```

All SPs must have terminal status (`test-passed`, `fixed`, `failed`, `needs-user`, `skipped`). Zero SPs in `pending`, `sp-tested`, or `fixing`. If this fails, return to Step 4.4-SP and resolve or mark blocking SPs as `needs-user`.

**2. EWI marker count in orchestration SQL** — zero blocking markers allowed:

```bash
grep -rn "!!!RESOLVE EWI!!!" <OUTPUT_DIR>/implementation/sql/
```

Expected: 0 matches. If any remain, fix them using `references/snowflake_patterns.md` before deploying.

**3. dbt compile check** (if dbt project exists):

```bash
dbt compile --project-dir <OUTPUT_DIR>/implementation/dbt_project/ \
            --profiles-dir ~/.dbt
```

Must exit 0. If it fails, return to Step 4.4 (dbt TDD Fix Loop) to resolve remaining compilation errors.

**3b. SP compile check** (if SP SQL selected for transformations):

```sql
-- For each SP file, run sql_execute with only_compile=true
-- to verify all SPs compile without errors
```

Must succeed for all SP files. If any fail, return to Step 4.4-SP to resolve.

**4. profiles.yml auth field check** (if dbt project exists):

```bash
grep -n "user:\|password:\|authenticator: externalbrowser\|token:\|private_key\|env_var" \
  <OUTPUT_DIR>/implementation/dbt_project/profiles.yml 2>/dev/null
```

Expected: 0 matches. If any of these are found, remove them. `authenticator: oauth` is **correct and required** — do not remove it. The only forbidden authenticator value is `externalbrowser` (and any non-oauth value). Valid `profiles.yml` contains: `type`, `account`, `authenticator: oauth`, `role`, `database`, `warehouse`, `schema`, `threads`.

**If all checks pass**: proceed to Step 5.1.
**If any fail**: report the specific failure with file path and line number. Wait for resolution before deploying.

---

### Step 5.1: Deploy to Snowflake

Execute SQL scripts in numbered order using `sql_execute`, passing `connection=<CONNECTION_NAME>` (the SQL connection confirmed in Step 4.1, e.g., `COCO_JK`) on every call. Fix any deployment errors iteratively.

> **CONNECTION REQUIRED**: Never omit the `connection` parameter. Every `sql_execute` call in Phase 5 must explicitly specify `connection=<CONNECTION_NAME>`.

**Common deployment issues** (from `references/snowflake_patterns.md`):
- SQL UDFs with subqueries can't be used inside SP temp table context → use inline JOINs
- COPY FILES doesn't support bind variables in FILES=() → use EXECUTE IMMEDIATE
- INTO clause with CASE containing subqueries → simplify query
- VARCHAR length mismatches → verify source data lengths against actual data

### Step 5.2: Upload Test Data & Run SP Test

> **When dbt is selected (Snowflake-native)**: The orchestrator SP calls `EXECUTE DBT PROJECT` internally. The dbt project **must** have been deployed in Step 4.2b before this test will succeed. If deployment was skipped, run `snow dbt deploy` now before calling the SP.

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
   > **Note**: `TASK_HISTORY` shows the task's next scheduled cron run (STATE = `SCHEDULED`) — not the immediate EXECUTE TASK trigger. If STATE = `SCHEDULED` with a future timestamp, do not wait for it. Instead, verify execution via the audit table (e.g. `SELECT * FROM dim_audit ORDER BY id DESC LIMIT 3`) — a new row with the current timestamp confirms the task ran successfully.
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

### Step 5.4: Re-run Reset (when repeating E2E tests)

When re-running the full pipeline for a second or subsequent E2E test (e.g., after fixing a bug found in Step 5.3), execute these reset steps **in order** before re-triggering the task DAG. Skipping any step causes 0-row runs or stale state errors.

```sql
-- 1. Restore source files from processed stage
COPY FILES INTO @<source_stage>/ FROM @<processed_stage>/;

-- 2. Refresh stage directory metadata (MANDATORY after COPY FILES)
ALTER STAGE <source_stage> REFRESH;

-- 3. Truncate target tables
TRUNCATE TABLE <target_db>.<schema>.FACT_TRANSACTIONS;  -- or equivalent
TRUNCATE TABLE <target_db>.<schema>.DIM_AUDIT;

-- 4. Reset batch counter so next run starts at batch 1
UPDATE <target_db>.<schema>.CONTROL_VARIABLES
SET VARIABLE_VALUE = TO_VARIANT(0)
WHERE VARIABLE_NAME = 'User_batch_id'
  AND VARIABLE_SCOPE = 'Data_Load';

-- 5. Trigger the pipeline
EXECUTE TASK <target_db>.<schema>.<root_task>;
```

> **COPY FILES flattens paths**: files originally stored in subdirectories (`batch_0/file.csv`) will land at the stage root (`file.csv`) after restore. If any SP uses `LIST @stage PATTERN = '.*/.*\\.csv'`, it will return 0 rows. Fix: use `PATTERN = '.*\\.csv'` (see Step 4.5 check 10).



- [ ] Infrastructure created successfully
- [ ] Sample data loaded to stage
- [ ] SP orchestration runs without errors
- [ ] Task DAG executes end-to-end (if applicable)
- [ ] Transformations produce expected results
- [ ] Row counts match
- [ ] Error handling works
- [ ] File movement operations complete
- [ ] Audit trail captures required metrics

### Step 5.4b: Parallel Validation Queries (spawn read-only verify agents)

Before writing the final test report, run validation queries in parallel to reduce wall-clock time. Spawn **one read-only `general-purpose` background agent per validation domain**. Each agent executes SQL queries against the deployed objects and writes its results to a findings file.

**Spawn one agent per domain** (adapt to what was deployed):

| Agent | Domain | Queries to run | Output file |
|---|---|---|---|
| row-count-verifier | Row counts | COUNT(*) on all target tables vs expected | `validation_row_counts.md` |
| audit-verifier | Audit trail | All dim_audit records have SuccessfulProcessingInd='Y', no nulls in key fields | `validation_audit.md` |
| file-movement-verifier | Stage state | Source stage empty, processed stage has expected file count | `validation_stages.md` |
| udf-verifier | UDF smoke tests | Call each UDF with representative inputs, compare to expected | `validation_udfs.md` |
| task-history-verifier | Task DAG history | SHOW TASK HISTORY, verify all tasks SUCCEEDED, check SCHEDULED_TIME | `validation_tasks.md` |

**Spawn pattern**:
```
Spawn all agents in a single message (parallel).
Agent Wait Protocol: end your turn. Do not write the test report until all agents return.
```

After all agents return:
1. Check each output file exists and contains results
2. Flag any agent that returned 0 rows or an error — re-run that agent's queries manually
3. Collate all findings into Step 5.5 test report

**Constraint**: Agents are read-only (`SELECT`, `SHOW`, `LIST`). They must NOT run `EXECUTE TASK`, `TRUNCATE`, `INSERT`, or any mutating statement.

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

**⚠️ HARD ENFORCEMENT — LOG BEFORE DECLARING DONE**: You MUST write the Phase 5 entry to `migration_phase_tracking.md` BEFORE presenting the final "Migration Complete" message to the user, before deleting teams, and before marking any ctx task as done. If background agents were used, wait for their results, THEN write the log. Do NOT present completion to the user with Phase 5 unlogged — this is a protocol violation.

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
- ✋ Phase 4, Step 4.6: User approves generated implementation scripts (NO deployment until approved)
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
| DIRECTORY(@stage) shows stale data after PUT | Directory metadata not refreshed | `ALTER STAGE <stage> REFRESH` after every PUT — required before any SP or query that reads from `DIRECTORY()` |
| `LIST @stage` shows file but `SELECT $1 FROM @stage` returns 0 rows | Stage storage inaccessible (stale stage object, e.g. after database recreate) | `DROP STAGE`, recreate with `DIRECTORY=TRUE`, re-PUT file, then `ALTER STAGE REFRESH` |
| dbt build fails — "Numeric value 'XXXX' is not recognized" on seed JOIN | Seed column auto-typed as `NUMBER(38,0)`; `VARCHAR`-derived JOIN expression forces implicit cast for non-numeric inputs | Cast seed column in JOIN: `ON SUBSTR(col, 1, 5) = seed_col::VARCHAR` (see Step 4.5 check 8) |
| `snow sql -q` returns truncated or empty results for multi-statement query | `-q` truncates multi-statement output above ~4k chars | Use `snow sql -f <file.sql>` for any query with multiple statements or large output |
| COMMENT on TASK syntax error | Remove COMMENT clause — some task configurations don't support it |
| Task cron won't fire immediately | Use `EXECUTE TASK <root>` for on-demand triggering instead of waiting for schedule |
| `snow dbt deploy` fails — `profiles.yml` has auth fields | Remove `user`, `password`, `token`, `private_key_path`, and all `env_var()` calls. Keep or add `authenticator: oauth` — this is **required** for Snowflake-native dbt. Remove `authenticator: externalbrowser` (browser auth is not available inside Snowflake's execution environment) |
| `snow dbt deploy` fails — `env_var()` in `dbt_project.yml` | Replace `env_var('KEY')` with literal values or `{{ var('key') }}` in models; supply values at runtime via `ARGS='build --vars {"key":"value"}'` in `EXECUTE DBT PROJECT` |
| `LIST @stage PATTERN = '.*/.*\\.csv'` returns 0 rows after restoring files | `COPY FILES` does not preserve subdirectory paths — files restored from a processed stage land at the stage root (`file.csv`, not `batch_0/file.csv`). The pattern `.*/.*` requires a `/` separator so it matches nothing at the root level | Change all SP `LIST` patterns from `.*/.*\\.csv` to `.*\\.csv` — this matches files at any depth (root or subdir). Verify with: `SELECT * FROM DIRECTORY(@stage)` |
| After `COPY FILES`, `EXECUTE DBT PROJECT` processes 0 files | Control variable `User_FilePath` is empty or still points to the last processed file from a prior run | Truncate `dim_audit`, reset `User_batch_id = 0` in `control_variables`, run `ALTER STAGE <source_stg> REFRESH` before triggering the task DAG |
