# ssis-migration Skill

A Cortex Code skill that encodes the complete SSIS-to-Snowflake migration framework — from assessment through production validation.

## What It Does

This skill enforces a **5-phase migration workflow** with mandatory Human-in-the-Loop approval gates between phases. It is not a generic AI prompt — it is a structured playbook that:

- Drives Phase 1 assessment using the `snowconvert-assessment` skill
- Annotates detected source types with Snowflake OpenFlow connector availability
- Presents architecture decision questionnaires for 7 dimensions (orchestration, transformations, file storage, file movement, source ingestion, CDC, Oracle strategy)
- Generates a 13-section `MIGRATION_PLAN.md` with full EWI traceability
- Produces numbered, ordered SQL implementation scripts
- Runs end-to-end validation with fix-and-retry cycles

## The 5 Phases

| Phase | What Happens |
|-------|-------------|
| **Phase 1 — Assessment** | Run SnowConvert AI + `snowconvert-assessment` skill → DAGs, HTML reports, JSON, OpenFlow annotations |
| **Phase 2 — Planning** | Architecture questionnaire → 7 dimensions selected with CoCo recommendations |
| **Phase 3 — Detailed Mapping** | EWI-driven mapping of every DAG node → `MIGRATION_PLAN.md` (13 sections) |
| **Phase 4 — Implementation** | Ordered SQL scripts, UDFs, SPs, Task DAGs, test data → approval before deploy |
| **Phase 5 — Validation** | Deploy → UDF smoke tests → E2E orchestration run → row reconciliation |

## Prerequisites

Before starting, ensure you have:

- [ ] SSIS `.dtsx` package files or project folder
- [ ] SnowConvert AI run on the packages (produces `ETL.Elements.csv`, `ETL.Issues.csv`, converted SQL/dbt)
- [ ] Snowflake account with appropriate permissions
- [ ] Target database and schema identified

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

## Related

- Blog post: [Migrating SSIS to Snowflake with Cortex Code: A Framework That Actually Works](#)
- Cortex Code docs: https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code
- Sample SSIS project used for testing: https://github.com/amrelauoty/Telecom-ETL-SSIS
