# SSIS to Snowflake Migration - Requirements Template

## Purpose

This document provides a reusable template for migrating SQL Server Integration Services (SSIS) packages to Snowflake. Use this template to guide Cortex Code through the assessment and migration planning process for any SSIS workload.

> **Note:** Before running the SnowConvert assessment, the SSIS package structure, dependencies, data flows, and DAGs are unknown. This template is designed to work with that constraint.

---

## Prerequisites

Before starting the migration process, ensure you have:

- [ ] SSIS package files (`.dtsx`) or access to the SSIS project. **Always ask the user for this path — never assume or auto-discover.**
- [ ] SnowConvert has already been run on the SSIS packages (this is a pre-requisite — the user runs SnowConvert before starting this workflow)
- [ ] Access to Cortex Code with the `snowconvert-assessment` skill
- [ ] Snowflake account with appropriate permissions
- [ ] Target database and schema identified

### Mandatory Information Gathering (ask before any analysis)

The following must be collected from the user before proceeding. You can ask users if they want coco to use existing output what snowconvert AI generated or start from scratch. If its from scratch then you don't need to ask SnowConvert converted code output path. You might still need the output path for the folder containing `ETL.Elements.csv` and `ETL.Issues.csv`.

1. **SSIS source path** — Path to the SSIS project or `.dtsx` files
2. **SnowConvert CSV path** — Path to the folder containing `ETL.Elements.csv` and `ETL.Issues.csv` (typically under `Reports/SnowConvert/`)
3. **SnowConvert output path** — Path to the converted SQL/dbt output (typically under `Output/SnowConvert/`)
4. **Assessment output path** — Where to place the assessment results (DAGs, reports, JSON). Ask the user for a folder name/location.

---

## Phase 1: Assessment

### Step 1.0: Gather Required Paths (MANDATORY — do this FIRST)

Before any analysis, ask the user for ALL of the following:

1. **SSIS source path** — Where are the `.dtsx` files?
2. **SnowConvert CSV path** — Where are `ETL.Elements.csv` and `ETL.Issues.csv`?
3. **SnowConvert output path** — Where is the converted SQL/dbt output?
4. **Assessment output path** — Where should the assessment results (DAGs, reports) be placed?

> **Important:** SnowConvert must have already been run by the user. Do not attempt to run SnowConvert — only consume its output.

### Step 1.1: Verify SnowConvert Output

Verify the SnowConvert output exists at the user-provided CSV path:

**Required files:**
- `ETL.Elements.*.csv` - Component inventory
- `ETL.Issues.*.csv` - Migration issues and warnings

**Also available (in SnowConvert output path):**
- Converted SQL files (stored procedures, DDLs)
- Converted dbt project (models, macros)
- File format definitions
- ETL configuration framework (control variables, UDFs)

### Step 1.2: Run Cortex Code ETL Assessment (using `snowconvert-assessment` skill)

**This step is MANDATORY before any migration planning.**

Use the `snowconvert-assessment` skill to generate:
- `etl_assessment_analysis.json` - Structured assessment data
- `ssis_assessment_report.html` - Interactive report with component inventory and issue analysis
- `dags/` folder - Control Flow and Data Flow DAG visualizations
- AI-generated migration summary

The assessment skill analyzes the SnowConvert CSVs and SSIS packages to produce DAGs and reports that are essential for accurate migration planning. Also generate a summary and name it as etl_assessment_summary.md based out of etl_assessment_analysis.json. 

**Inputs to the skill:**
- SSIS source path (from Step 1.0)
- SnowConvert CSV path containing `ETL.Elements.csv` and `ETL.Issues.csv` (from Step 1.0)
- Assessment output path (from Step 1.0)

### Step 1.3: Review Assessment Output

After the assessment completes, review the generated artifacts:
- DAG visualizations show the exact SSIS control flow and data flow
- The assessment report identifies all components, their conversion status, and EWI issues
- Components marked as `NotSupported` or with EWI markers need manual migration strategies
- Display the path of the assessment files and folder path.
- Display the path for etl_assessment_summary.md

#### OpenFlow Connector Availability Annotation (Mandatory)

After reviewing the detected source/destination types in `ETL.Elements.csv`, annotate the assessment summary with Snowflake OpenFlow connector availability. This informs Phase 2 Source Ingestion and CDC decisions.

| Detected Source/Destination | OpenFlow Connector | Note |
|-----------------------------|--------------------|------|
| SQL Server (OLE DB / CDC Source) | Yes — `sqlserver` | Managed near-real-time CDC; recommended for SQL Server sources |
| MySQL | Yes — `mysql` | CDC-capable |
| PostgreSQL | Yes — `postgresql` | CDC-capable |
| Oracle (OLE DB / ADO.NET) | Yes — `oracle-embedded-license` or `oracle-independent-license` | Licensing decision required; network connectivity must be assessed |
| SAP BW Source | No | Snowpark Python or certified partner tools |
| OData Source | No | Airflow HTTP Operator or Snowflake External Function |
| Azure Blob / ADLS Source | No — native COPY INTO | Use external stage directly |
| Flat File / ODBC Source | No — native COPY INTO | Snowpipe or COPY INTO from stage |

For each source type with an available connector, include this note in `etl_assessment_summary.md`:
> `"[Source Type] detected — Snowflake OpenFlow [connector name] connector is available. Evaluate for Phase 2 Source Ingestion strategy before defaulting to custom code."`

**Only proceed to Phase 2 after the assessment is complete, reviewed, and OpenFlow annotations are added.**

---

## Phase 2: Migration Planning

### Step 2.1: Request Migration Plan

After the assessment is complete, request a migration plan with your preferred options.

**Prompt Template:**
```
Based on the SSIS assessment, create a migration plan to Snowflake.

Source Assessment Location: [ASSESSMENT_OUTPUT_PATH]

Please provide options for the following decisions:
```

### Step 2.2: Implementation Approach Selection

Present the user with options for each component type and collect their selections.

#### Control Flow Orchestration

| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| **Snowflake Stored Procedures** | Complex orchestration, file loops, conditional logic | Full control, native Snowflake, no external dependencies | More code to maintain |
| **Snowflake Tasks + Streams** | Event-driven, simple orchestration | Serverless, auto-scaling, real-time triggers | Limited complex logic |
| **External Orchestrator (Airflow/dbt Cloud)** | Enterprise scheduling, cross-platform | Rich scheduling, monitoring, alerting | External dependency |

**My Selection:** `[ ]` Stored Procedures `[ ]` Tasks + Streams `[ ]` External Orchestrator

#### Data Flow Transformations

| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| **dbt Models** | Complex transformations, testing, documentation | Version control, lineage, testing framework | Requires dbt setup |
| **Dynamic Tables** | Continuous transformations, simple logic | Auto-refresh, declarative, minimal code | Less flexible for complex logic |
| **Stored Procedure SQL** | Simple transformations, tight coupling with orchestration | Single deployment unit, no external tools | Harder to test/document |
| **Hybrid (dbt + Dynamic Tables)** | Mixed complexity | Best of both worlds | More components to manage |

**My Selection:** `[ ]` dbt `[ ]` Dynamic Tables `[ ]` SP SQL `[ ]` Hybrid

#### File Operations

| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| **Snowflake Internal Stage** | Full Snowflake-native solution | No external storage, folder structure, directory tables | Files must be uploaded to Snowflake |
| **External Stage (S3/Azure/GCS)** | Existing cloud storage, large files | Leverage existing infrastructure | Requires cloud storage setup |
| **Hybrid (External → Internal)** | Landing in cloud, processing in Snowflake | Flexibility | More complexity |

**My Selection:** `[ ]` Internal Stage `[ ]` External Stage `[ ]` Hybrid

#### File Movement Operations

If the SSIS package includes File System Tasks (copy/move/delete files):

| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| **Stage Folder Operations (COPY FILES + REMOVE)** | Snowflake-native file management | No external tools, audit trail | Limited to stage files |
| **External Process (Python/Shell)** | Complex file operations, external systems | Full flexibility | External dependency |
| **Skip File Movement** | Files managed externally | Simpler Snowflake solution | Requires external file management |

**My Selection:** `[ ]` Stage Folder Operations `[ ]` External Process `[ ]` Skip

#### Source Ingestion Pattern

When the assessment identifies how data enters the pipeline (Flat File Sources, OLE DB Sources, CDC Sources), select the ingestion approach for the Snowflake target:

| Option | Best For | Notes |
|--------|----------|-------|
| **COPY INTO (Batch)** | File-based ingestion, scheduled loads | Native Snowflake; works with internal/external stages |
| **Snowpipe (Auto-ingest)** | Near-real-time file ingestion | S3/ADLS event notification triggers automatic loads |
| **OpenFlow Connector** | Database sources (SQL Server, Oracle, MySQL, PostgreSQL) | Managed CDC or bulk replication; see connector availability flagged in Phase 1 assessment |
| **External Tables** | Data stays in cloud storage, queried in-place | No loading cost; query performance trade-off |
| **Snowpark Python** | Custom/complex source systems, transformations at ingest | Full flexibility; requires Python development |

**My Selection:** `[ ]` COPY INTO `[ ]` Snowpipe `[ ]` OpenFlow Connector `[ ]` External Tables `[ ]` Snowpark Python

#### CDC / Change Data Capture (Conditional — complete only when CDC Control Task, CDC Source, or CDC Splitter is detected in assessment)

> **Important:** SSIS CDC components (CDC Control Task, CDC Source, CDC Splitter) are designed exclusively for the **SQL Server CDC feature**. They do not apply to Oracle without the separate Attunity Oracle CDC Service for SQL Server add-on, which is deprecated as of SQL Server 2017. If Oracle CDC was used via SSIS, confirm whether that Attunity add-on was in use before applying these options.

For **SQL Server** CDC sources, select the Snowflake CDC approach:

| Option | Best For | Notes |
|--------|----------|-------|
| **OpenFlow SQL Server Connector** | Managed near-real-time CDC | Recommended; no custom code; handles initial snapshot + streaming changes |
| **Snowflake Streams + Dynamic Tables** | Declarative, low-maintenance CDC | Best for continuous refresh; minimal operational overhead |
| **Snowflake Streams + Tasks** | Imperative CDC, full control | More flexible; more code to maintain |
| **Snowpipe Streaming** | High-frequency micro-batch ingestion | Kafka SDK or API-based; lowest latency option |

**My Selection:** `[ ]` OpenFlow SQL Server `[ ]` Streams + Dynamic Tables `[ ]` Streams + Tasks `[ ]` Snowpipe Streaming

#### Oracle Source Strategy (Conditional — complete only when Oracle Source or Oracle Destination is detected in assessment)

Oracle ingestion strategy depends on network connectivity. Resolve connectivity before selecting an approach.

**Step 1 — Assess Oracle network reachability:**

| Scenario | Recommended Approach |
|----------|----------------------|
| Oracle reachable from Snowflake cloud (public IP / VPN / Direct Connect) | OpenFlow Oracle Connector (SPCS deployment) |
| Oracle on-prem, OpenFlow BYOC runtime can be deployed in customer network | OpenFlow Oracle Connector (BYOC deployment) |
| Oracle on-prem, no VPC or network join possible | Blob Storage Intermediary pattern |

**Step 2 — If OpenFlow Oracle selected:**
- Licensing decision required: Embedded ($110/core/month, 60-day trial, 36-month commit) or BYOL (if Oracle GoldenGate/XStream license already exists)
- Oracle DBA must enable XStream, supplemental logging, and create an XStream Outbound Server before connector deployment
- Tables must have primary keys to be replicated

**Step 3 — If on-prem Oracle with no network connectivity (Blob Storage Intermediary):**

```
Oracle (on-prem)
    │
    ├── Option A: Debezium (on-prem, LogMiner) → Kafka → S3/ADLS → Snowpipe
    │   True CDC: captures INSERTs, UPDATEs, DELETEs. Open source, no Oracle license needed.
    │
    ├── Option B: Oracle GoldenGate → Trail files → S3/ADLS → Snowpipe
    │   True CDC: requires existing GoldenGate license. Most robust for high-volume OLTP.
    │
    ├── Option C: SQL*Plus/JDBC incremental extract → CSV/Parquet → S3/ADLS → COPY INTO
    │   Near-CDC: high-watermark on updated_at column. Misses DELETEs unless soft deletes used.
    │
    └── Option D: Oracle Data Pump export → S3/ADLS → COPY INTO
        Batch only: suitable for nightly full or incremental loads. Simplest to implement.
```

**My Selection:**
`[ ]` OpenFlow Oracle (SPCS) `[ ]` OpenFlow Oracle (BYOC)
`[ ]` Debezium → S3 → Snowpipe `[ ]` GoldenGate → S3 → Snowpipe
`[ ]` JDBC Extract → S3 → COPY INTO `[ ]` Data Pump → S3 → COPY INTO

---

## Phase 3: Review DAGs and Create Detailed Mapping

### Step 3.1: DAG Review

**Prompt Template:**
```
Review the DAGs under [ASSESSMENT_PATH]/dags to understand the exact flow.

Create the Snowflake implementation that accurately maps:
1. Control Flow DAG → Orchestration components
2. Data Flow DAG → Transformation components

Ensure component-to-component traceability.
```

### Step 3.2: Request Detailed Migration Plan

**Prompt Template:**
```
Upgrade the migration plan with detailed DAG-based mappings showing:

1. SSIS Control Flow components → Snowflake SP/Task mapping
2. SSIS Data Flow components → dbt/Dynamic Table/SQL mapping
3. SSIS Variables → Snowflake variables mapping
4. SSIS Connection Managers → Snowflake objects mapping
5. Visual DAG diagrams for both source and target
```

### Step 3.3: Write Migration Plan to File (MANDATORY)

**The migration plan MUST always be written to a `.md` file** in the project directory (e.g., `ssis_migration_review/MIGRATION_PLAN.md`) before proceeding. This allows the user to review the complete plan outside the conversation.

The migration plan file must include ALL of the following sections:

**Required Sections:**

1. **Workload Summary** — Package inventory with classification, effort estimates, and SnowConvert conversion status
2. **Source SSIS DAG Structures** — ASCII art diagrams for each package showing:
   - Control Flow DAGs (all packages) with component types, precedence constraints, and EWI markers
   - Data Flow DAGs (for packages with Pipeline/DataFlow tasks) showing sources, transforms, lookups, script components, row counts, and destinations
   - Clear labeling of starter/end tasks, parallel paths, and loop boundaries
3. **Target Snowflake Architecture** — ASCII art diagram showing the end-to-end target architecture:
   - Stages (source/processed), Task DAG, Stored Procedures, dbt project structure, and target tables
   - Data flow direction from ingestion through transformation to final tables
4. **Detailed Component Mapping** — Per-package tables mapping every SSIS component to its Snowflake implementation:
   - Control Flow → Stored Procedure steps or Task DAG entries (with source: SnowConvert vs manual)
   - Data Flow → dbt models (with SQL logic summary per model)
   - Connection Managers → Snowflake objects (stages, file formats, database connections)
5. **SSIS Variables Mapping** — Table of all `User::` variables with type, package, purpose, and Snowflake equivalent (SP variables, session vars, control_variables table)
6. **User-Selected Implementation Approach** — Documented selections for orchestration, transformations, file storage, and file operations
7. **Target Database Schema** — Tables, stages, file formats, UDFs with their source DDLs
8. **EWI Issue Resolution Plan** — Every SnowConvert issue with specific resolution strategy
9. **Script Migration Designs** — For any C# Script Tasks/Components: original behavior, Snowflake SQL replacement code, UDF definitions, lookup table seed data
10. **Implementation Steps** — Ordered steps with file names and specific modifications needed
11. **SnowConvert Output Usage Summary** — Table showing each SnowConvert artifact and the action (use as-is, modify, rewrite, skip)
12. **Open Decisions** — Items requiring user input before implementation
13. **Deliverables** — Complete file tree of what will be created in Phase 4

### Step 3.4: User Approval Gate (MANDATORY)

**Implementation (Phase 4) MUST NOT begin until the user explicitly reviews and approves the migration plan.**

After writing the plan to the `.md` file:
1. Inform the user that the plan has been written and where it is located
2. Ask the user to review the plan
3. **Wait for explicit approval** (e.g., "looks good", "approved", "go ahead") before proceeding
4. If the user requests changes, update the plan file and repeat the review cycle

**Do NOT proceed to Phase 4 without clear user consent.**

---

## Phase 4: Implementation

### Step 4.1: Generate Implementation Files

**Prompt Template:**
```
Generate the implementation files for the migration:

Target Database: [DATABASE_NAME]
Target Schema: [SCHEMA_NAME]
Snowflake Connection: [CONNECTION_NAME]
Warehouse: [WAREHOUSE_NAME]

Implementation Approach:
- Orchestration: [SELECTED_OPTION]
- Transformations: [SELECTED_OPTION]
- File Storage: [SELECTED_OPTION]
- File Operations: [SELECTED_OPTION]

Create all necessary SQL scripts and configuration files.
```

### Step 4.2: Expected Deliverables

The exact set of deliverables depends on the source SSIS project analysis (Phase 1) and architecture selections (Phase 2). Deliverables may include, but are not limited to:

- **Infrastructure DDL** — Databases, schemas, stages, file formats, tables, sequences
- **Stored Procedures** — Orchestration, file processing, audit management
- **UDFs / UDTFs** — Reusable functions migrated from C# scripts, expressions, or custom logic
- **Views** — Reusable query layers, staging views, reporting views
- **Dynamic Tables** — Continuous transformation pipelines (if selected)
- **dbt Project** — Models (staging, intermediate, marts), macros, sources, profiles
- **Tasks / Task DAGs** — Scheduled or event-driven orchestration
- **Streams** — Change tracking for event-driven patterns
- **Seed / Reference Data** — Lookup tables, configuration data extracted from SSIS packages or C# scripts
- **Testing Scripts** — Validation queries for all generated components
- **Test Data** — Sample input files matching source file formats

The migration plan (Phase 3) must list the exact deliverables for the specific project before implementation begins.
At the end of creating all the required file, list out the summary to a file name `ssis_migration_review/solution_artifacts_generated.md` .

### Step 4.3: User Approval Gate (MANDATORY)
Validation and Testing (Phase 5) MUST NOT begin until the user explicitly reviews and approves the code artifacts which are generated.

After creating the code artifacts:

Inform the user where the artifacts are located
Ask the user to review the code
Wait for explicit approval (e.g., "looks good", "approved", "go ahead") before proceeding
If the user requests changes, update the code and repeat the review cycle
Do NOT proceed to Phase 5 without clear user consent.
---

## Phase 5: Validation and Testing

### Step 5.1: Deployment

**Prompt Template:**
```
Deploy the migration to Snowflake:

Connection: [CONNECTION_NAME]
Environment: [DEV/TEST]

Execute scripts in order and verify each step. At the end of deploying all the objects perform a end to end testing and log the end - to end testing process and the results to a file name `ssis_migration_review\Solution_End_End_Testing.md`
```

### Step 5.2: Testing Checklist

- [ ] Infrastructure created successfully
- [ ] Sample data loaded to stage
- [ ] Orchestration runs without errors
- [ ] Transformations produce expected results
- [ ] Row counts match source system
- [ ] Error handling works correctly
- [ ] File movement operations complete
- [ ] Audit/logging captures required metrics
- [ ] Scheduled tasks execute on time

---

## Quick Start - Single Prompt

For a streamlined experience, use this consolidated prompt after SnowConvert completes:

```
Perform SSIS assessment and create a complete Snowflake migration for:

Source: [SSIS_PACKAGE_PATH]
SnowConvert Output: [SNOWCONVERT_OUTPUT_PATH]
Target Database: [DATABASE].[SCHEMA]
Connection: [SNOWFLAKE_CONNECTION]

Implementation Preferences:
- Orchestration: [Stored Procedures | Tasks | External]
- Transformations: [dbt | Dynamic Tables | SQL | Hybrid]
- File Storage: [Internal Stage | External Stage]
- File Operations: [Stage Folders | External | Skip]

Deliverables needed:
1. Assessment report with DAG visualizations
2. Detailed migration plan with component mappings
3. All implementation SQL scripts
4. dbt project (if selected)
5. Testing scripts
6. Deployment instructions
```

---

## Reference: SSIS to Snowflake Component Mapping

| SSIS Component Type | Snowflake Options |
|--------------------|-------------------|
| **Execute SQL Task** | SP: `EXECUTE IMMEDIATE`, Task: inline SQL |
| **For Each Loop (Files)** | SP: `CURSOR FOR DIRECTORY(@STAGE)` |
| **For Each Loop (Rows)** | SP: `FOR rec IN cursor DO` |
| **Data Flow Task** | dbt models, Dynamic Tables, or SP SQL |
| **Flat File Source** | `COPY INTO` from stage / Snowpipe auto-ingest |
| **OLEDB Source (SQL Server)** | `SELECT` from source table / OpenFlow SQL Server Connector |
| **OLEDB Source (Oracle)** | `SELECT` from source / OpenFlow Oracle Connector (if reachable) / Blob intermediary (if on-prem isolated) |
| **OLEDB Destination** | `INSERT INTO` / `MERGE` / dbt model |
| **CDC Control Task** | Snowflake Streams (SQL Server CDC feature only) / OpenFlow SQL Server Connector |
| **CDC Source** | Snowflake Streams + Dynamic Tables / Streams + Tasks / OpenFlow SQL Server Connector (SQL Server only) |
| **CDC Splitter** | `CASE WHEN METADATA$ACTION = 'INSERT'` on Stream / SP conditional logic |
| **Derived Column** | dbt: `SELECT ... AS new_col` / SQL expressions |
| **Lookup** | dbt: `LEFT JOIN` / SQL join |
| **Conditional Split** | dbt: `CASE WHEN` / `WHERE` filters |
| **Row Count** | `COUNT(*)` / `SQLROWCOUNT` |
| **File System Task (Copy)** | `COPY FILES INTO @stage/dest/ FROM @stage/src/` |
| **File System Task (Move)** | `COPY FILES` + `REMOVE @stage/src/file` |
| **File System Task (Delete)** | `REMOVE @stage/path/file` |
| **Script Task** | Snowflake Scripting / Snowpark |
| **Send Mail Task** | `SYSTEM$SEND_EMAIL` / External notification |
| **Sequence Container** | SP: `BEGIN...END` block |
| **Precedence Constraints** | SP: conditional logic / Task dependencies |

---

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| SnowConvert CSVs not found | Verify path to `/Reports/SnowConvert` folder |
| DAGs not generated | Re-run SnowConvert with visualization enabled |
| Complex SSIS expressions | May require manual translation to Snowflake SQL |
| Custom SSIS components | Assess functionality and implement equivalent logic |
| External system connections | Configure appropriate Snowflake integrations |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-03-18 | Initial template |
| 1.1 | 2026-03-21 | Added mandatory Step 1.0 (gather paths before analysis), explicit prerequisite that SnowConvert must be pre-run, mandatory snowconvert-assessment skill execution before planning, assessment review gate before Phase 2 |
| 1.2 | 2026-03-21 | Added Step 3.3: migration plan must always be written to a `.md` file. Added Step 3.4: explicit user approval gate — Phase 4 implementation must not begin until user reviews and approves the plan |
| 1.3 | 2026-03-21 | Enhanced Step 3.3: expanded migration plan required sections to 13 mandatory items including Source SSIS DAG Structures (ASCII art), Target Snowflake Architecture diagram, Detailed Component Mapping (per-package tables), SSIS Variables Mapping, Script Migration Designs, and SnowConvert Output Usage Summary |

---

*Template created for use with Cortex Code SSIS Assessment Skill*
