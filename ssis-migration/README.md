# ssis-migration Skill

A Cortex Code skill that encodes the complete SSIS-to-Snowflake migration framework — from assessment through production validation.

## What It Does

This skill enforces a **5-phase migration workflow** with mandatory Human-in-the-Loop approval gates between phases. It is not a generic AI prompt — it is a structured playbook that:

- Drives Phase 1 assessment using the `snowconvert-assessment` skill
- Annotates detected source types with Snowflake OpenFlow connector availability
- Presents architecture decision questionnaires for 7 dimensions (orchestration, transformations, file storage, file movement, source ingestion, CDC, Oracle strategy)
- Generates a 13-section `MIGRATION_PLAN.md` with full EWI traceability
- Produces numbered, ordered SQL implementation scripts
- Runs automated dbt TDD Fix Loop (test-gen + fixer) and SP TDD Fix Loop for iterative quality
- Runs end-to-end validation with fix-and-retry cycles

## The 5 Phases

| Phase | What Happens |
|-------|-------------|
| **Phase 1 — Assessment** | Verify plugin prerequisites → Run SnowConvert AI + `snowconvert-assessment` skill → DAGs, HTML reports, JSON, OpenFlow annotations |
| **Phase 2 — Planning** | Architecture questionnaire → 7 dimensions selected with CoCo recommendations |
| **Phase 3 — Detailed Mapping** | EWI-driven mapping of every DAG node → `MIGRATION_PLAN.md` (13 sections) |
| **Phase 4 — Implementation** | Ordered SQL scripts, UDFs, SPs, Task DAGs, test data + automated TDD fix loops → approval before deploy |
| **Phase 5 — Validation** | Deploy → UDF smoke tests → E2E orchestration run → row reconciliation |

## Prerequisites

Before starting, ensure you have:

- [ ] SSIS `.dtsx` package files or project folder
- [ ] SnowConvert AI run on the packages (produces `ETL.Elements.csv`, `ETL.Issues.csv`, converted SQL/dbt) — **optional**, the skill also supports starting from scratch
- [ ] Python 3.9+ available locally
- [ ] Snowflake account with appropriate permissions
- [ ] Target database and schema identified
- [ ] `snowflake-migration` plugin installed (required for automated dbt TDD Fix Loop in Phase 4)

### Installing the `snowflake-migration` Plugin

The skill checks for this plugin at the start of Phase 1 (Step 0.1) and will block if it's missing.

Add the following to `~/.snowflake/cortex/settings.json`:

```json
{
  "plugins": [
    "https://github.com/Snowflake-Labs/cortex-code-migrations/tree/preview/plugin"
  ]
}
```

Then restart Cortex Code. The plugin will be fetched and cached automatically under `~/.snowflake/cortex/remote-cache/`.

This provides the `migrate-etl-package` sub-skill containing:
- `dbt-test-gen` — generates dbt tests and seed data from source `.dtsx` files
- `dbt-fixer` — iteratively fixes dbt compilation errors and EWI markers (up to 5 attempts per node)
- `track_status.py` — phase gate validation and session state tracking

## How to Use

### Step 1 — Add the skill to your Cortex Code session

**Option A — From raw GitHub URL:**
```
Read the skill at:
https://raw.githubusercontent.com/sfc-gh-praj/cortex-skills/main/ssis-migration/SKILL.md

Follow this skill's workflow for my SSIS migration.
```

**Option B — From local file (after cloning):**
```bash
git clone https://github.com/sfc-gh-praj/cortex-skills.git
```
Then in your Cortex Code session:
```
Read /path/to/cortex-skills/ssis-migration/SKILL.md
and follow its workflow for my SSIS migration.
```

### Step 2 — Answer the skill's intake questions

The skill will ask for:
1. SSIS source path (`.dtsx` files location)
2. SnowConvert CSV path (`ETL.Elements.csv`, `ETL.Issues.csv`)
3. SnowConvert converted output path
4. Assessment output folder name
5. Target Snowflake database name
6. Target schema (default: `PUBLIC`)
7. Snowflake warehouse name

### Step 3 — Follow the phase gates

The skill will not proceed to the next phase without your explicit approval. Each gate requires you to review artifacts before continuing.

## Architecture Decisions Covered

The Phase 2 questionnaire covers:

| Dimension | Options |
|-----------|---------|
| Orchestration | Stored Procedures / Tasks + Streams / External (Airflow) |
| Transformations | dbt Models / Dynamic Tables / SP Inline SQL / Hybrid |
| File Storage | Internal Stage / External Stage / Hybrid |
| File Movement | Stage Folder Ops / External Process / Skip |
| Source Ingestion | COPY INTO / Snowpipe / OpenFlow Connector / External Tables / Snowpark |
| CDC (conditional — SQL Server only) | OpenFlow SQL Server / Streams + Dynamic Tables / Streams + Tasks / Snowpipe Streaming |
| Oracle Source (conditional) | OpenFlow Oracle (SPCS/BYOC) / Blob Storage Intermediary (Debezium, GoldenGate, JDBC, Data Pump) |

## Component Mapping Reference

| SSIS Component | Snowflake Target |
|----------------|-----------------|
| Execute SQL Task | `EXECUTE IMMEDIATE` in SP |
| ForEachLoop (Files) | `CURSOR FOR DIRECTORY(@stage)` |
| Script Component (C#) | JavaScript/SQL UDFs + seed tables |
| Script Task (C#) | Snowflake Scripting SP block |
| CDC Source / CDC Control Task | SQL Server only — Streams + Dynamic Tables / OpenFlow SQL Server |
| Flat File Source | `COPY INTO` from stage / Snowpipe |
| OLE DB Source (SQL Server) | `SELECT` from table / OpenFlow SQL Server Connector |
| OLE DB Source (Oracle) | OpenFlow Oracle (if reachable) / Blob Storage Intermediary |
| File System Task (Move) | `COPY FILES` + `REMOVE` |
| Send Mail Task | `SYSTEM$SEND_EMAIL` |
| Execute Package Task | `CALL sp_child()` in master SP / Task AFTER dependency |

## File Structure

```
ssis-migration/
├── SKILL.md                          # Main skill workflow (5-phase process)
├── README.md                         # This file
├── scripts/
│   ├── generate_ssis_report.py       # Phase 1 assessment report generator
│   └── track_sp_status.py            # SP TDD status tracking
├── sp-tdd/                           # SP SQL TDD Fix Loop sub-skills
│   ├── README.md
│   ├── sp-test-gen/                  # Generates SP tests + fixtures
│   └── sp-fixer/                     # Iteratively fixes SP compilation/logic errors
├── references/
│   ├── component_mapping_reference.md # SSIS → Snowflake component patterns
│   ├── snowflake_patterns.md         # Implementation patterns + EWI/FDM fix reference
│   ├── phase3_migration_plan_template.md # 13-section plan template (complex packages)
│   ├── sp_test_patterns.md           # SP testing patterns reference
│   └── skill-architecture.md         # Skill architecture documentation
└── tests/                            # Evaluation framework
    ├── eval_config.yaml              # Test configuration
    ├── eval_runner.py                # Test runner
    ├── fixtures/                     # Test fixtures (sample .dtsx, CSVs)
    ├── scenarios/                    # Test scenarios
    └── README.md                     # Testing documentation
```

## SP TDD Fix Loop

When **SP Inline SQL** is selected for Data Flow Transformations in Phase 2 (instead of dbt), the skill runs an SP-specific TDD Fix Loop in Phase 4 (Step 4.4-SP):

1. **sp-test-gen** — generates test fixtures and validation queries for each stored procedure
2. **sp-fixer** — iteratively deploys, tests, and fixes SP compilation errors and logic bugs (up to 5 attempts)
3. **track_sp_status.py** — tracks fix loop state for resumability

This is the SP equivalent of the dbt TDD Fix Loop and uses the same parallel agent pattern.

## Testing

The `tests/` directory contains an evaluation framework for validating the skill's behavior:

- `eval_config.yaml` — defines test scenarios and expected outcomes
- `eval_runner.py` — executes scenarios and checks assertions
- `fixtures/` — sample SSIS packages and SnowConvert outputs for testing
- `scenarios/` — end-to-end test scenarios

Run with:
```bash
uv run --project . python tests/eval_runner.py
```

## Related

- Blog post: [Migrating SSIS to Snowflake with Cortex Code: A Framework That Actually Works](#)
- Cortex Code docs: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
- Sample SSIS project used for testing: https://github.com/amrelauoty/Telecom-ETL-SSIS
