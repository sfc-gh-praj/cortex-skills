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

**Note**: Unlisted Control Flow elements generate EWI SSC-EWI-SSIS0004. Unlisted Data Flow components generate EWI SSC-EWI-SSIS0001.

### dbt Project Execution in Orchestration

Data Flow Tasks are executed via Snowflake's `EXECUTE DBT PROJECT` command:
```sql
EXECUTE DBT PROJECT schema.project_name ARGS='build --target dev'
```
Deploy dbt projects first using: `snow dbt deploy --schema <schema> --database <db> --force <package_name>`

## Script Component Migration Patterns

### C# Script Component → Snowflake UDFs

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
| SSC-EWI-SSIS0001 | Critical | Component not supported (e.g., Script Component) | Manual rewrite as UDFs + seed tables |
| SSC-EWI-SSIS0004 | High | Control Flow element not supported (Script Task, For Loop, etc.) | Rewrite as Snowflake Scripting or WHILE loop |
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
