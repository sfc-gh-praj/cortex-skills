# SSIS to Snowflake Component Mapping Reference

> Source: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README)

## Control Flow Components

| SSIS Component | Type | Snowflake Equivalent | Notes |
|---------------|------|---------------------|-------|
| Microsoft.Pipeline (Data Flow Task) | Task | dbt Project (`EXECUTE DBT PROJECT`) or SP inline SQL | Central transformation unit |
| Microsoft.ExecuteSQLTask | Task | Inline SQL or Stored Procedure | Simple → inline; complex with result sets → SP |
| Microsoft.ExecutePackageTask | Task | `CALL sp_name()` or inline TASK execution | Local single-ref → inline; reusable (2+ refs) → SP (FDM SSC-FDM-SSIS0005) |
| Microsoft.SendMailTask | Task | `SYSTEM$SEND_EMAIL` with Notification Integration | CC/BCC merged to recipients; no attachments/HTML/priority |
| Microsoft.BulkInsertTask | Task | `COPY INTO` with inline `FILE_FORMAT` | Requires stage setup; native format not supported |
| STOCK:SEQUENCE (Sequence Container) | Container | `BEGIN...END` block (inline in parent) | FDM SSC-FDM-SSIS0003; variable scoping differs |
| STOCK:FORLOOP (For Loop Container) | Container | Sequential execution (manual iteration) | EWI SSC-EWI-SSIS0004; InitExpr/EvalExpr/AssignExpr need manual WHILE loop |
| STOCK:FOREACHLOOP (ForEach File Enumerator) | Container | `LIST @stage` + CURSOR pattern | EWI SSC-EWI-SSIS0014; replace `<STAGE_PLACEHOLDER>` post-migration |
| STOCK:FOREACHLOOP (other enumerators) | Container | Not supported | EWI SSC-EWI-SSIS0004; implement manually |
| Event Handlers (OnError, OnWarning, etc.) | Container | Not converted | Implement manually using Snowflake exception handling |
| Script Task (C#) | Task | Snowflake Scripting: `IF/THEN/RAISE` | EWI SSC-EWI-SSIS0004 |
| File System Task (Copy) | Task | `COPY FILES INTO @dest FROM @src/file` | Use EXECUTE IMMEDIATE for dynamic paths |
| File System Task (Move) | Task | `COPY FILES` + `REMOVE` | Two-step: copy then remove |
| File System Task (Delete) | Task | `REMOVE @stage/path/file` | Single command |
| File System Task (CreateDirectory) | Task | `COPY INTO` with `.dummy` placeholder | FDM SSC-FDM-SSIS0026; Snowflake stages are prefix-based |
| Precedence Constraint (OnSuccess) | Constraint | Sequential execution or `IF` check | Only Success constraints fully supported |
| Precedence Constraint (OnFailure) | Constraint | `EXCEPTION WHEN OTHER THEN` | Failure/Completion constraints need manual adjustment |
| CDC Control Task | Task | Snowflake Streams state management or OpenFlow SQL Server Connector | **SQL Server CDC feature only.** Not converted by SnowConvert — EWI SSC-EWI-SSIS0004. Manages LSN range for CDC processing windows. Snowflake equivalent: track high-watermark via a `cdc_state` table + Streams. |

### Container Conversion Details

- **Sequence Containers**: Converted inline within parent TASK with `BEGIN...END` blocks. No separate SP created. Only Success precedence constraints are fully supported.
- **For Loop Containers**: Executes contained tasks once by default. InitExpression, EvalExpression, and AssignExpression require manual conversion to Snowflake `WHILE` loop.
- **ForEach Loop (File Enumerator)**: Converts to `LIST @stage` + cursor pattern. Replace `<STAGE_PLACEHOLDER>` with actual stage name post-migration.
- **ForEach Loop (other enumerators)**: Item, ADO, NodeList enumerators are not supported — implement manually.

### Execute Package Task Details

| Package Type | Conversion | Notes |
|-------------|-----------|-------|
| Local (single reference) | Inline execution in parent TASK | Logic expanded inline |
| Reusable (2+ references or parameters) | `CALL` to stored procedure | Synchronous execution; FDM SSC-FDM-SSIS0005 |
| External | `CALL` with path resolution | EWI SSC-EWI-SSIS0008 for manual verification |

**Note**: TASK-based Execute Package conversions run asynchronously. For synchronous behavior, packages are converted to SPs.

## Data Flow Components

| SSIS Component | Type | Snowflake Equivalent | dbt Model Naming | Notes |
|---------------|------|---------------------|-----------------|-------|
| Microsoft.OLEDBSource | Source | Staging model | `stg_raw__{component_name}` | Direct table reference |
| Microsoft.FlatFileSource | Source | Staging model (`COPY INTO` from stage) | `stg_raw__{component_name}` | Uses file format |
| Microsoft.DerivedColumn | Transform | `SELECT` with expressions | `int_{component_name}` | `col_expr AS new_col` |
| Microsoft.DataConvert | Transform | `CAST` expressions | `int_{component_name}` | Type conversion |
| Microsoft.Lookup | Transform | `LEFT JOIN` | `int_{component_name}` | FDM SSC-FDM-SSIS0001 for ORDER BY |
| Microsoft.UnionAll | Transform | `UNION ALL` | `int_{component_name}` | Combine sources |
| Microsoft.Merge | Transform | `UNION ALL` | `int_{component_name}` | FDM SSC-FDM-SSIS0002 for sorted output |
| Microsoft.MergeJoin | Transform | `JOIN` | `int_{component_name}` | FDM SSC-FDM-SSIS0004 for ORDER BY |
| Microsoft.ConditionalSplit | Transform | Router pattern with CTEs | `int_{component_name}` | `CASE WHEN` / `WHERE` filter |
| Microsoft.Multicast | Transform | `SELECT` pass-through | `int_{component_name}` | Reference same source multiple times |
| Microsoft.RowCount | Transform | Intermediate model with macro | `int_{component_name}` | Uses `m_update_row_count_variable` macro |
| Microsoft.OLEDBDestination | Destination | Mart model (table materialization) | `{target_table_name}` | Final target |
| Microsoft.FlatFileDestination | Destination | Mart model (table materialization) | `{target_table_name}` | Final target |
| Script Component (C#) | Transform | JavaScript/SQL UDFs + seed tables | N/A | EWI SSC-EWI-SSIS0001 |
| Sort | Transform | `ORDER BY` | N/A | Usually not needed in set-based SQL |
| Aggregate | Transform | `GROUP BY` | N/A | Standard SQL aggregation |
| Microsoft.CDCSource | Source | Snowflake Stream on source table | `stg_cdc__{table_name}` | **SQL Server CDC feature only.** Not converted by SnowConvert — EWI SSC-EWI-SSIS0001. Reads INSERT/UPDATE/DELETE changes from SQL Server CDC change tables. Snowflake equivalent: `SELECT * FROM STREAM(table_stream)` or OpenFlow SQL Server Connector. |
| Microsoft.CDCSplitter | Transform | Stream + `METADATA$ACTION` filter | `int_cdc__{table_name}` | **SQL Server CDC feature only.** Not converted — EWI SSC-EWI-SSIS0001. Splits net changes into INSERT/UPDATE/DELETE paths. Snowflake equivalent: `CASE WHEN METADATA$ACTION = 'INSERT' ... WHEN METADATA$ACTION = 'DELETE'` on Stream rows, or three separate CTEs. |

**Note**: Unlisted Control Flow elements generate EWI SSC-EWI-SSIS0004. Unlisted Data Flow components generate EWI SSC-EWI-SSIS0001.

### dbt Project Execution in Orchestration

Data Flow Tasks are executed via Snowflake's `EXECUTE DBT PROJECT` command:
```sql
EXECUTE DBT PROJECT schema.project_name ARGS='build --target dev'
```
Deploy dbt projects first using: `snow dbt deploy --schema <schema> --database <db> --force <package_name>`

## CDC Component Migration Patterns

> **Scope note:** SSIS CDC components (CDC Control Task, CDC Source, CDC Splitter) are designed exclusively for the **SQL Server CDC feature**. They do NOT apply to Oracle databases without the separate deprecated "Microsoft Change Data Capture Designer and Service for Oracle by Attunity" add-on (supported only through SQL Server 2017). If Oracle CDC was used via SSIS, confirm with the customer whether that Attunity add-on was in use before applying these patterns.

### CDC Architecture Comparison

```
SSIS CDC Pattern:
  CDC Control Task → establishes LSN processing range
       ↓
  CDC Source       → reads change rows from SQL Server change tables
       ↓
  CDC Splitter     → routes INSERTs / UPDATEs / DELETEs to separate paths
       ↓
  OLE DB Destinations (INSERT, MERGE, DELETE targets)

Snowflake Equivalent (Streams + Tasks):
  Snowflake Stream  → tracks INSERT/UPDATE/DELETE on source table
       ↓
  Task (scheduled) → reads stream, applies METADATA$ACTION filter
       ↓
  CASE WHEN METADATA$ACTION = 'INSERT' → INSERT INTO target
  CASE WHEN METADATA$ACTION = 'DELETE' → DELETE FROM target
  CASE WHEN METADATA$ACTION = 'UPDATE' → MERGE INTO target
       ↓
  Target table (same semantic result)
```

### Component-by-Component Migration

#### CDC Control Task → State Table Pattern

The CDC Control Task tracks the LSN (Log Sequence Number) processing window. Snowflake has no LSN concept — state is managed via a control table or Stream offsets.

```sql
-- Create a CDC state tracking table (replaces CDC Control Task state store)
CREATE TABLE IF NOT EXISTS cdc_state (
    table_name       VARCHAR NOT NULL,
    last_processed   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    stream_offset    VARCHAR,   -- Snowflake stream offset token if needed
    status           VARCHAR DEFAULT 'idle',
    PRIMARY KEY (table_name)
);

-- At start of CDC Task run (replaces "Mark Initial Load Start" CDC Control mode)
UPDATE cdc_state SET status = 'running', last_processed = CURRENT_TIMESTAMP()
WHERE table_name = 'your_table';

-- At end of CDC Task run (replaces "Mark CDC Processing End" mode)
UPDATE cdc_state SET status = 'idle' WHERE table_name = 'your_table';
```

**When to use OpenFlow instead:** If the source is SQL Server and near-real-time CDC is required, use the **OpenFlow SQL Server Connector** — it handles LSN tracking, initial snapshot, and streaming changes natively with no custom state management code.

#### CDC Source → Snowflake Stream

The CDC Source reads change rows from SQL Server CDC change tables. Snowflake Streams provide the equivalent — a change feed on any table.

```sql
-- Create a stream on the source table (replaces CDC Source)
CREATE OR REPLACE STREAM sales_stream ON TABLE sales_source
    SHOW_INITIAL_ROWS = TRUE;   -- captures initial snapshot rows on first consumption

-- Read all pending changes (replaces CDC Source output)
SELECT
    *,
    METADATA$ACTION,       -- 'INSERT' or 'DELETE' (UPDATE = DELETE + INSERT pair)
    METADATA$ISUPDATE,     -- TRUE if this row is part of an UPDATE operation
    METADATA$ROW_ID        -- unique row identifier within the stream
FROM sales_stream;
```

**Important:** Snowflake Streams represent UPDATEs as a DELETE + INSERT pair when `SHOW_INITIAL_ROWS = FALSE`. Use `METADATA$ISUPDATE = TRUE` to distinguish UPDATE deletes from true deletes.

#### CDC Splitter → METADATA$ACTION Filter

The CDC Splitter routes INSERTs, UPDATEs, and DELETEs to separate output paths. In Snowflake, this is done with conditional logic on `METADATA$ACTION`.

```sql
-- Replaces CDC Splitter — three separate CTEs for each change type
WITH stream_data AS (
    SELECT *, METADATA$ACTION, METADATA$ISUPDATE
    FROM sales_stream
),
inserts AS (
    SELECT * FROM stream_data
    WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
),
updates AS (
    -- UPDATEs arrive as DELETE+INSERT pair; capture only the INSERT half
    SELECT * FROM stream_data
    WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
),
deletes AS (
    SELECT * FROM stream_data
    WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE
)
-- Apply each CTE to its target operation:
-- INSERT INTO target SELECT * FROM inserts;
-- MERGE INTO target USING updates ...;
-- DELETE FROM target WHERE id IN (SELECT id FROM deletes);
```

### Recommended Snowflake CDC Approaches

| Tier | Approach | When to Use |
|------|----------|-------------|
| **Tier 1 — Direct SQL Server CDC** | OpenFlow SQL Server Connector | SQL Server source; reads CDC change tables directly; no custom code; recommended first choice |
| **Tier 2 — Snowflake-side tracking** | Streams + Dynamic Tables | Data already bulk-loaded into Snowflake staging; need downstream declarative change propagation |
| **Tier 2 — Snowflake-side tracking** | Streams + Tasks | Same as above; need imperative INSERT/UPDATE/DELETE routing or multi-target fan-out |

> **Important:** Snowflake Streams track changes on *Snowflake tables* — they cannot read from SQL Server CDC change tables directly. Tier 2 requires a prior bulk replication step to land data into Snowflake first.

### Full Streams + Tasks CDC Implementation Pattern

For complex SSIS CDC packages migrated to Streams + Tasks:

```sql
-- 1. Create stream on replicated source table
CREATE OR REPLACE STREAM sales_cdc_stream ON TABLE source_db.sales;

-- 2. Create task to process changes on schedule
CREATE OR REPLACE TASK process_sales_cdc
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('sales_cdc_stream')
AS
DECLARE
    v_insert_count INT DEFAULT 0;
    v_update_count INT DEFAULT 0;
    v_delete_count INT DEFAULT 0;
BEGIN
    -- Inserts (replaces CDC Splitter INSERT path)
    INSERT INTO target_db.sales
    SELECT col1, col2, col3, CURRENT_TIMESTAMP() AS load_ts
    FROM sales_cdc_stream
    WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE;
    v_insert_count := SQLROWCOUNT;

    -- Updates (replaces CDC Splitter UPDATE path)
    MERGE INTO target_db.sales AS tgt
    USING (
        SELECT col1, col2, col3
        FROM sales_cdc_stream
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE
    ) AS src ON tgt.id = src.col1
    WHEN MATCHED THEN UPDATE SET tgt.col2 = src.col2, tgt.col3 = src.col3;
    v_update_count := SQLROWCOUNT;

    -- Deletes (replaces CDC Splitter DELETE path)
    DELETE FROM target_db.sales
    WHERE id IN (
        SELECT col1 FROM sales_cdc_stream
        WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE
    );
    v_delete_count := SQLROWCOUNT;

    -- Audit log (replaces SSIS CDC package OnPostExecute event handler)
    INSERT INTO cdc_audit_log (table_name, inserts, updates, deletes, processed_at)
    VALUES ('sales', :v_insert_count, :v_update_count, :v_delete_count, CURRENT_TIMESTAMP());

    RETURN 'CDC complete: ' || :v_insert_count || ' ins, ' ||
           :v_update_count || ' upd, ' || :v_delete_count || ' del';
END;
```

### Oracle CDC via SSIS — Special Case

If the SSIS project used Oracle as a CDC source via the **Attunity Oracle CDC Service for SQL Server** (a separate deprecated add-on), the migration path depends on Oracle network accessibility:

| Scenario | Snowflake Target Pattern |
|----------|-------------------------|
| Oracle reachable from Snowflake cloud | OpenFlow Oracle Connector (SPCS) — XStream-based CDC |
| Oracle on-prem, BYOC allowed | OpenFlow Oracle Connector (BYOC) |
| Oracle on-prem, no network join | Blob Storage Intermediary: Debezium (LogMiner) → Kafka → S3 → Snowpipe, or GoldenGate → S3 → Snowpipe |

See Phase 2 Oracle Source Strategy questionnaire for full decision tree.

## Script Component Migration Patterns

When a C# Script Component processes rows:

1. **Identify per-row logic** — Each transformation becomes a UDF or inline SQL expression
2. **Identify lookup dictionaries** — Hardcoded C# dictionaries become seed/lookup tables
3. **Identify validation algorithms** — Complex algorithms (e.g., checksum, regex, custom validation) become JavaScript UDFs
4. **Identify categorization logic** — CASE-based mapping becomes SQL UDF or inline CASE

**UDF Type Selection:**
- **JavaScript UDF**: For algorithmic logic (loops, array operations, checksums)
- **SQL UDF**: For simple CASE mappings, lookups (but beware: SQL UDFs with subqueries can't be called inside SP temp table creation)
- **Inline SQL**: For expressions that reference lookup tables (use LEFT JOIN)

### C# Script Task → Snowflake Scripting

When a C# Script Task controls flow:

```sql
DECLARE v_count INT;
BEGIN
    SELECT COUNT(*) INTO :v_count FROM <table>;
    IF (v_count = 0) THEN
        RETURN 'FAILED: <descriptive message>';
    END IF;
END;
```

## Connection Manager Mapping

| SSIS Connection Type | Snowflake Object |
|---------------------|-----------------|
| OLEDB (SQL Server) | Database.Schema reference |
| OLEDB (Oracle) | OpenFlow Oracle Connector (if reachable) / Blob Storage Intermediary (if on-prem isolated) |
| CDC (SQL Server CDC feature) | Snowflake Stream on replicated table / OpenFlow SQL Server Connector |
| Flat File | Stage + File Format |
| FILE (directory) | Stage with DIRECTORY enabled |
| SMTP | Notification Integration + `SYSTEM$SEND_EMAIL` |
| Excel | `excel_source_udf` + Stage |

## Variable Mapping

| SSIS Variable Pattern | Snowflake Equivalent |
|----------------------|---------------------|
| User::variable_name | SP `DECLARE var_name TYPE` or dbt `{{ var('name') }}` |
| System::PackageName | Literal string `'PackageName'` |
| SSIS Expression (REVERSE + FINDSTRING for filename) | `SPLIT_PART(path, '/', -1)` |
| Expression (ternary ? :) | `IFF(condition, true_val, false_val)` |
| Variable as ResultSet binding | `SELECT ... INTO :variable` |
| OUTPUT INSERTED.id | Sequence `.NEXTVAL` or `MAX(id)` pattern |

## EWI / FDM Code Reference

> Full reference: [EWI Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/conversion-issues/ssisEWI) | [FDM Codes](https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/ssisFDM)

**IMPORTANT**: Do NOT rely solely on this reference. SnowConvert codes evolve across versions. Always dynamically read the project's actual `ETL.Issues.*.csv` and `Issues.*.csv` files to get the real codes, severities, and descriptions for the specific migration.

### EWI Codes (Errors, Warnings, Information)

| Code | Severity | Meaning | Resolution |
|------|----------|---------|------------|
| SSC-EWI-SSIS0001 | Critical | Component not supported (e.g., Script Component, CDC Source, CDC Splitter) | Manual rewrite as UDFs + seed tables (Script Component) or Streams + METADATA$ACTION filter (CDC Source/Splitter) |
| SSC-EWI-SSIS0004 | High | Control Flow element not supported (Script Task, For Loop, CDC Control Task, etc.) | Rewrite as Snowflake Scripting, WHILE loop, or cdc_state table pattern |
| SSC-EWI-SSIS0005 | Medium | Async execution — TASK runs async, not sync like SSIS | Convert to SP for synchronous behavior |
| SSC-EWI-SSIS0008 | Medium | External package reference needs verification | Verify path and convert to CALL |
| SSC-EWI-SSIS0011 | Medium | Result set binding on non-query | Use sequence NEXTVAL or MAX(id) pattern |
| SSC-EWI-SSIS0014 | Medium | Folder path needs stage mapping (ForEach File) | Replace `<STAGE_PLACEHOLDER>` with actual stage |
| SSC-EWI-SSIS0015 | Medium | File attachments not supported (Send Mail) | Use staged files with presigned URLs |
| SSC-EWI-SSIS0016 | Low | Email priority not supported | Ignore or add to message body |
| SSC-EWI-SSIS0017 | Medium | FileConnection message source not supported (Send Mail) | Read file content and pass as string |
| SSC-EWI-SSIS0018 | Medium | HTML email body not supported | Use plain text |
| SSC-EWI-SSIS0020 | High | Native/WideNative data format not supported (Bulk Insert) | Export to CSV first |
| SSC-EWI-SSIS0021 | Medium | LastRow filtering not supported (Bulk Insert) | Load to staging, filter with ROW_NUMBER |
| SSC-EWI-SSIS0022 | Medium | FireTriggers not supported (Bulk Insert) | Use Streams + Tasks for trigger-like behavior |
| SSC-EWI-SSIS0023 | Medium | Format file not supported (Bulk Insert) | Use inline FILE_FORMAT options |
| SSC-EWI-SSIS0024 | High | Stage not included in translation (Bulk Insert) | Create stage and upload files manually |
| SSC-EWI-SSIS0025 | Medium | Stage path variable needs mapping | Set to `@stage_name` paths |
| SSC-EWI-SSIS0032 | High | Excel Source variable-based access mode | Use dbt vars for dynamic sheet names |
| SSC-EWI-SSIS0033 | Medium | Excel Source SQL filtering not preserved | Add filtering as downstream CTEs |
| SSC-EWI-SSIS0037 | Medium | Expression function not reviewed | Translate to Snowflake SQL equivalent |
| SSC-EWI-SSIS0039 | Low | Overwrite behavior difference | Accept Snowflake semantics or add pre-check |
| SSC-EWI-SSIS0044 | Medium | COPY FILES destination as prefix | Use explicit stage path |
| SSC-EWI-0021 | Medium | OUTPUT clause not supported | Use sequence pattern |

### FDM Codes (Functional Differences)

| Code | Meaning | Resolution |
|------|---------|------------|
| SSC-FDM-SSIS0001 | Lookup needs ORDER BY for deterministic match | Add `ORDER BY` in JOIN or use `QUALIFY ROW_NUMBER()` |
| SSC-FDM-SSIS0002 | Merge sorted output not guaranteed | Add ORDER BY if sort order matters |
| SSC-FDM-SSIS0003 | Sequence Container variable scoping differs | Variables accessible throughout parent TASK, not just container |
| SSC-FDM-SSIS0004 | MergeJoin ORDER BY requirements | Add ORDER BY for deterministic results |
| SSC-FDM-SSIS0005 | Package converted to SP (informational) | Expected behavior — confirm |
| SSC-FDM-SSIS0007 | SMTP connection managed by Snowflake | No action needed |
| SSC-FDM-SSIS0008 | FROM address not supported in email | Prepended to message body |
| SSC-FDM-SSIS0009 | CC not supported in email | CC recipients added to main recipients |
| SSC-FDM-SSIS0010 | BCC not supported (privacy concern) | Send separate emails for privacy |
| SSC-FDM-SSIS0011 | MaximumErrors → ON_ERROR mapping | Review ON_ERROR behavior |
| SSC-FDM-SSIS0012 | BatchSize automatic in Snowflake | No action needed |
| SSC-FDM-SSIS0014 | TableLock not needed (MVCC) | No action needed |
| SSC-FDM-SSIS0015 | SortedData not available | No action needed |
| SSC-FDM-SSIS0016 | CheckConstraints always enforced | No action needed |
| SSC-FDM-SSIS0017 | KeepIdentity=False behavior differs | Review identity column handling |
| SSC-FDM-SSIS0025 | Variable must contain valid stage path | Set to `@stage_name` paths |
| SSC-FDM-SSIS0026 | CreateDirectory uses `.dummy` placeholder | Snowflake stages are prefix-based |
