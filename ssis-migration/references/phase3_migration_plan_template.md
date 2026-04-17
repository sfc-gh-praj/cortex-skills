# Phase 3: Migration Plan Template

The MIGRATION_PLAN.md must contain ALL 13 sections below. This is the blueprint for Phase 4 implementation.

---

## Section 1: Workload Summary

Table format:

| Package | Classification | Components | Data Flows | Scripts | SnowConvert Status | Effort (hrs) |
|---------|---------------|------------|------------|---------|-------------------|-------------|

Classifications: Configuration & Control, Ingestion, Transformation, Mixed, Export, Orchestration

Include overall conversion rates: Success %, Partial %, Not Supported %.

## Section 2: Source SSIS DAG Structures

ASCII art diagrams for EACH package:

- **Control Flow DAGs** — Show component types, precedence constraints (OnSuccess/OnFailure), EWI markers, loop boundaries (ForEachLoop), parallel paths
- **Data Flow DAGs** — Show FlatFileSource → DerivedColumn → Lookup → ScriptComponent → RowCount → OLEDBDestination with Error Output paths
- Label: component name, type in parentheses, EWI codes in brackets

Example structure:
```
┌─────────────────────────────────────────┐
│       PACKAGE_NAME.dtsx                 │
├─────────────────────────────────────────┤
│  ┌──────────────┐                       │
│  │  Task Name   │ (TaskType)            │
│  │  [EWI: XXX]  │                       │
│  └──────┬───────┘                       │
│         │ OnSuccess                     │
│         ▼                               │
│  ┌──────────────┐                       │
│  │  Next Task   │ (TaskType)            │
│  └──────────────┘                       │
└─────────────────────────────────────────┘
```

## Section 3: Target Snowflake Architecture

ASCII art showing end-to-end target architecture:
- Internal Stages (source + processed)
- Task DAG or SP orchestration chain
- SP call sequence with inline steps
- dbt project structure (if applicable)
- Target tables
- UDFs
- Dynamic Tables (if applicable)

Show data flow direction from ingestion → transformation → final tables.

## Section 4: Detailed Component Mapping

Per-package tables mapping EVERY SSIS component to Snowflake:

**Control Flow:**
| SSIS Component | Type | Snowflake Target | Implementation | Source (SC/Manual) |
|---------------|------|-----------------|----------------|-------------------|

**Data Flow:**
| SSIS Component | Type | Snowflake Target | SQL Logic Summary | Source |
|---------------|------|-----------------|-------------------|--------|

Include ALL components — none should be missing.

## Section 5: SSIS Variables Mapping

| Variable | Type | Package | Purpose | Snowflake Equivalent |
|----------|------|---------|---------|---------------------|

Include User:: variables, System:: variables, and SSIS expression translations.

## Section 6: User-Selected Implementation Approach

Document the Phase 2 selections:
| Decision | Selection |
|----------|-----------|
| Orchestration | [user choice] |
| Transformations | [user choice] |
| File Storage | [user choice] |
| File Operations | [user choice] |

## Section 7: Target Database Schema

### Tables
| Table | Purpose | Key Columns |
|-------|---------|-------------|

### Stages, File Formats, Sequences, UDFs, SPs, Tasks, Dynamic Tables
Each with name, purpose, and configuration details.

## Section 8: EWI Issue Resolution Plan (CRITICAL)

**IMPORTANT**: Every issue from `ETL.Issues.*.csv` must be listed INDIVIDUALLY. Do NOT group duplicates. Each row in the CSV = one row in this table.

| # | Code | Type | Severity | Component | Package | Description | Resolution Strategy |
|---|------|------|----------|-----------|---------|-------------|-------------------|

Count must match the CSV exactly. If the CSV has 16 rows, this table must have 16 rows.

## Section 9: Script Migration Designs

For each C# Script Task or Script Component:

1. **Original C# Behavior** — Bullet list of what the script does
2. **Snowflake Replacement** — Specific implementation:
   - UDF definitions with signatures
   - Seed table schemas and data
   - SQL expressions for inline logic
   - Snowflake Scripting blocks (IF/THEN/RAISE)

Include actual code snippets for the replacement implementations.

## Section 10: Implementation Steps

Ordered numbered list:
| Step | File | Description |
|------|------|-------------|

Must cover all SQL scripts, dbt files, and test data in deployment order.

## Section 11: SnowConvert Output Usage Summary

| SnowConvert Artifact | Action | Reason |
|---------------------|--------|--------|

Actions: Use as-is, Modify, Rewrite, Skip. Include specific modifications needed.

## Section 12: Open Decisions

| # | Decision | Options | Impact |
|---|----------|---------|--------|

Items requiring user input: DB/schema names, warehouse, schedule, error handling strategy, file retention, etc.

## Section 13: Deliverables

Complete file tree of what Phase 4 will create:
```
<output_dir>/
├── implementation/
│   ├── sql/
│   │   └── [all numbered SQL scripts]
│   ├── dbt_project/  (if applicable)
│   │   └── [full structure]
│   ├── test_data/
│   └── solution_artifacts_generated.md
```
