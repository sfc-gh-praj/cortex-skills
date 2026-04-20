# Snowflake Implementation Patterns for SSIS Migration

Proven patterns from real SSIS-to-Snowflake migrations. Use these as templates when generating Phase 4 code.

> Source: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README)

---

## EWI / FDM Fix Reference

SnowConvert embeds two types of inline markers in converted SQL files.

### Marker Format

**Blocking — must be fixed before SQL can run:**
```sql
!!!RESOLVE EWI!!! /*** SSC-EWI-SSIS0014 - THE FOLDER PATH REQUIRES MANUAL MAPPING TO A SNOWFLAKE STAGE. ***/!!!
```

**Informational — code still runs, but behaviour differs from SSIS:**
```sql
--** SSC-FDM-SSIS0005 - PACKAGE WAS CONVERTED TO STORED PROCEDURE BECAUSE IT IS BEING REUSED BY OTHER PACKAGES. **
```

Scan converted SQL files with:
```bash
grep -rn "!!!RESOLVE EWI!!!" <OUTPUT_DIR>/
```

For any code not in the table below, look up the full description in the public docs:
- EWI codes: https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/conversion-issues/ssisEWI
- FDM codes: https://docs.snowflake.com/en/migrations/snowconvert-docs/general/technical-documentation/issues-and-troubleshooting/functional-difference/ssisFDM

---

### Control Flow EWI Codes

| Code | Description | Fix Action |
|------|-------------|------------|
| `SSC-EWI-SSIS0001` | SSIS component not supported by SnowConvert | Full manual rewrite required. Identify the component type (ScriptTask, third-party, custom), read original C# from `.dtsx`, translate to Snowflake Scripting SP or JavaScript UDF |
| `SSC-EWI-SSIS0004` | Control Flow element cannot be converted to Snowflake Scripting | Rewrite the element manually. For ScriptTask: read the original C# body and reimplement as a Snowflake Scripting block or JS SP. For unsupported containers: restructure logic using IF/ELSE + CALL pattern |
| `SSC-EWI-SSIS0005` | ForEach Loop file enumerator type not supported | Use the ForEachLoop Python SP pattern — see `ForEachLoop File Enumerator` section below |
| `SSC-EWI-SSIS0011` | Result binding configured for non-query statement | Remove result binding. For DDL/DML: use `EXECUTE IMMEDIATE`. For value capture: `LET v := (SELECT col FROM ...)` |
| `SSC-EWI-SSIS0014` | ForEach File Enumerator folder path requires manual stage mapping | Replace the `<STAGE_PLACEHOLDER>` with the actual Snowflake stage path: `@<db>.<schema>.<stage>/folder/`. See ForEachLoop pattern |
| `SSC-EWI-SSIS0019` | Script Task C# code not converted | Read the C# body from the original `.dtsx` file. Translate logic to Snowflake Scripting (SQL SP) or JavaScript SP. Common patterns: file validation → DIRECTORY query; string manipulation → Snowflake string functions; logging → INSERT into audit table |
| `SSC-EWI-SSIS0025` | Flat File Source stage path variable requires manual mapping | Replace the stage path variable reference with a literal: `@<db>.<schema>.<stage>/path/filename.csv` or pass as SP argument |
| `SSC-EWI-SSIS0026` | ForEach ADO.NET Schema Rowset Enumerator not supported | Replace with a `SHOW TABLES / INFORMATION_SCHEMA` query fed into a cursor loop |
| `SSC-EWI-SSIS0037` | SSIS expression function not reviewed for Snowflake equivalence | Look up the specific function name in the comment. Common mappings: `REVERSE` → `REVERSE()`, `FINDSTRING` → `POSITION()` or `CHARINDEX()-1`, `SUBSTRING` → `SUBSTR()`, `UPPER/LOWER` → direct, `TRIM` → `TRIM()`, `LEN` → `LEN()`, `REPLACE` → `REPLACE()` |
| `SSC-EWI-SSIS0039` | FileSystemTask overwrite=false not supported — Snowflake silently overwrites | Option A (strict): add a `LIST @stage WHERE name = '<file>'` check before COPY FILES and raise an exception if file exists. Option B (accepted): document the behaviour difference and remove the marker |
| `SSC-EWI-SSIS0044` | FileSystemTask COPY FILES — destination treated as directory prefix, source filename appended | If the destination is intentionally a directory: no action needed. If a specific output filename is required: construct the full path explicitly in the COPY FILES call |

---

### Send Mail Task EWI/FDM Codes

| Code | Description | Fix Action |
|------|-------------|------------|
| `SSC-EWI-SSIS0015` | SMTP connection manager not supported | Replace with Snowflake Notification Integration + `SYSTEM$SEND_EMAIL()`. See Send Mail Task pattern |
| `SSC-EWI-SSIS0016` | Email priority setting ignored | Remove priority — no Snowflake equivalent; acceptable behaviour difference |
| `SSC-EWI-SSIS0017` | Email attachment not supported | If attachment is required: write data to a stage file first, then share the stage URL in the email body |
| `SSC-EWI-SSIS0018` | HTML email body not supported — plain text only | Convert HTML body to plain text. If formatting is critical, include a Snowsight URL link instead |
| `SSC-FDM-SSIS0008` | FROM address prepended to email body (Snowflake cannot set FROM) | Informational — no code fix. The FROM field is fixed to the Notification Integration's configured sender |
| `SSC-FDM-SSIS0009` | CC recipients merged into TO recipients | Informational — no code fix. All recipients (TO + CC) receive the same message |

---

### Bulk Insert Task EWI Codes

| Code | Description | Fix Action |
|------|-------------|------------|
| `SSC-EWI-SSIS0020` | Native format not supported — must export to delimited first | Add a pre-step to export the source as CSV/Parquet to a stage, then use `COPY INTO` |
| `SSC-EWI-SSIS0021` | LastRow parameter not supported | Load full file to a staging table, then filter target rows with `ROW_NUMBER() OVER (ORDER BY ...)` |
| `SSC-EWI-SSIS0022` | FireTriggers not supported | Implement trigger-like logic with a Stream + Task on the destination table |
| `SSC-EWI-SSIS0024` | Bulk Insert Task general conversion issue | Review the generated `COPY INTO` statement. Verify FILE_FORMAT options, stage path, and column mapping match the source file |

---

### General EWI Codes

| Code | Description | Fix Action |
|------|-------------|------------|
| `SSC-EWI-0021` | SQL OUTPUT / OUTPUT INTO clause not supported in Snowflake | Replace with one of: `SELECT MAX(id)` before/after INSERT, `INSERT INTO ... SELECT NEXTVAL('seq')`, or a RETURNING workaround via a staging query |

---

### FDM Informational Codes (no code fix required — review only)

| Code | Description | Action |
|------|-------------|--------|
| `SSC-FDM-SSIS0001` | Non-deterministic ordering — NULL in ORDER BY | Add explicit `ORDER BY` column(s). Use `NULLS FIRST` or `NULLS LAST` if needed |
| `SSC-FDM-SSIS0005` | Package converted to stored procedure because it is reused by other packages | Informational — the generated SP structure is intentional. No change needed |
| `SSC-FDM-SSIS0025` | Variable value must be a valid Snowflake stage path | Update the variable value or SP argument to use `@<db>.<schema>.<stage>/path/` format |
| `SSC-FDM-SSIS0026` | CreateDirectory not supported — .dummy placeholder used | Informational — the `.dummy` file approach is the correct Snowflake equivalent. No change needed |

---

## ForEachLoop File Enumerator → LIST + Cursor Pattern

SnowConvert generates a `LIST @stage` + cursor pattern. When implementing manually with SPs, use DIRECTORY:

**SnowConvert output style (TASK-based):**
```sql
LIST @<STAGE_PLACEHOLDER>/folder_path PATTERN = '.*/.*\.csv';

LET file_cursor CURSOR FOR
   SELECT REGEXP_SUBSTR($1, '[^/]+$') AS FILE_VALUE
   FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
   WHERE $1 NOT LIKE '%folder_path/%/%';

FOR file_row IN file_cursor DO
   v_current_file := :file_row.FILE_VALUE;
   -- Per-file processing here
END FOR;
```

**SP-based style — Python SP (DIRECTORY, recommended):**

> **WARNING**: SQL Scripting cursor record field access (`rec.RELATIVE_PATH`) fails
> at **compile time** with `invalid identifier 'REC.RELATIVE_PATH'` when the cursor
> source is a table function like `DIRECTORY(@stage)`. Use a Python SP instead.

```python
CREATE OR REPLACE PROCEDURE sp_process_files()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session):
    session.sql("ALTER STAGE source_stage REFRESH").collect()

    files = session.sql(
        "SELECT RELATIVE_PATH "
        "FROM DIRECTORY(@source_stage) "
        "WHERE RELATIVE_PATH ILIKE '%.csv' "
        "ORDER BY RELATIVE_PATH"
    ).collect()

    files_processed = 0
    for row in files:
        # Escape single quotes in path (defensive — stage paths are trusted)
        safe_path = row['RELATIVE_PATH'].replace("'", "''")
        session.sql(f"CALL PUBLIC.sp_process_file('{safe_path}')").collect()
        files_processed += 1

    return {"status": "success", "files_processed": files_processed}
$$;
```

**Key points:**
- Always `ALTER STAGE REFRESH` before reading DIRECTORY
- Stage must have `DIRECTORY = (ENABLE = TRUE)`: `ALTER STAGE s SET DIRECTORY = (ENABLE = TRUE)`
- `RELATIVE_PATH` includes subdirectory prefixes (e.g., `batch_0/file.csv`)
- Python SP requires a warehouse — ensure the Task or caller specifies one
- Replace `@source_stage` and `sp_process_file` with actual names post-migration
- Replace `<STAGE_PLACEHOLDER>` with actual stage name post-migration

## Dynamic Stage Path in CTAS (avoiding bind variable issues)

```sql
EXECUTE IMMEDIATE '
    CREATE OR REPLACE TEMPORARY TABLE stg_temp AS
    SELECT $1::VARCHAR AS col1, $2::VARCHAR AS col2
    FROM @source_stage/' || v_file_path || '
        (FILE_FORMAT => ''my_format'')
';
```

**Why EXECUTE IMMEDIATE:** Stage paths in `FROM @stage/path` don't support bind variables (`:var`). Must concatenate.

## COPY FILES + REMOVE (File Movement)

```sql
EXECUTE IMMEDIATE 'COPY FILES INTO @processed_stage/archive/ FROM @source_stage/' || v_file_path;
EXECUTE IMMEDIATE 'REMOVE @source_stage/' || v_file_path;
```

**Known limitation:** `COPY FILES INTO @stage FROM @stage FILES = (:var)` fails with `invalid value [TOK_CONSTANT_LIST]`. Always use string concatenation.

## CreateDirectory → .dummy Placeholder (FDM SSC-FDM-SSIS0026)

Snowflake stages are prefix-based — empty directories don't exist. SnowConvert uses a `.dummy` file:

```sql
EXECUTE IMMEDIATE CONCAT(
    'COPY INTO ', :v_directory_path, '/.dummy ',
    'FROM (SELECT ''empty'') ',
    'FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE) OVERWRITE = TRUE SINGLE = TRUE'
);
```

## Audit Pattern (replaces SQL Server OUTPUT INSERTED)

```sql
-- SQL Server: INSERT ... OUTPUT INSERTED.id INTO @audit_id
-- Snowflake: Use MAX(id) after INSERT (or sequence NEXTVAL before INSERT)

-- Pattern 1: MAX after INSERT
INSERT INTO audit_log (batch_id, file_name, ...) VALUES (:v_batch_id, :v_file_name, ...);
SELECT MAX(id) INTO :v_audit_id FROM audit_log WHERE batch_id = :v_batch_id AND file_name = :v_file_name;

-- Pattern 2: Sequence NEXTVAL before INSERT
SELECT seq_audit_id.NEXTVAL INTO :v_audit_id;
INSERT INTO audit_log (id, batch_id, ...) VALUES (:v_audit_id, :v_batch_id, ...);
```

## Send Mail Task → SYSTEM$SEND_EMAIL (EWI SSC-EWI-SSIS0015/0016/0017/0018)

SnowConvert converts Send Mail Tasks to `SYSTEM$SEND_EMAIL` with Notification Integration:

```sql
BEGIN
   LET integration_sql STRING := 'CREATE OR REPLACE NOTIFICATION INTEGRATION email_notify
       TYPE=EMAIL ENABLED=TRUE
       ALLOWED_RECIPIENTS=(''admin@example.com'', ''team@example.com'')';
   EXECUTE IMMEDIATE :integration_sql;

   CALL SYSTEM$SEND_EMAIL('email_notify', 'admin@example.com,team@example.com', 'Subject', 'Message body');
END;
```

**Limitations & workarounds:**
- **No attachments**: Upload to stage, use `GET_PRESIGNED_URL(@stage, 'file', 3600)` and include link in body
- **No CC/BCC**: CC merged into recipients (FDM SSC-FDM-SSIS0009); for BCC privacy, send separate emails
- **No HTML body**: Plain text only (EWI SSC-EWI-SSIS0018)
- **No priority**: Ignored (EWI SSC-EWI-SSIS0016)
- **Fixed sender**: FROM address prepended to body (FDM SSC-FDM-SSIS0008)

**Prerequisites**: `CREATE INTEGRATION ON ACCOUNT` grant; all recipients must be verified in Snowflake.

## CR-only Line Endings (\r — old Mac format)

Source CSV files created on older Mac systems or certain ETL tools may use CR-only
(`\r`) line terminators instead of `\n` or `\r\n`. Snowflake's default file format
expects `\n` or `\r\n` — **CR-only files return 0 rows silently** with no error.

```sql
-- Diagnose: if COPY INTO or a stage query returns 0 rows on a non-empty file,
-- check line endings with: xxd source_file.csv | head -3
-- CR-only shows: 0d (hex) at end of each field block with no 0a

CREATE OR REPLACE FILE FORMAT telecom_csv_format
    TYPE             = CSV
    FIELD_DELIMITER  = '|'
    RECORD_DELIMITER = '\r'    -- required for CR-only (\r) files
    SKIP_HEADER      = 1
    NULL_IF          = ('', 'NULL');

-- For \r\n files (Windows): RECORD_DELIMITER = '\r\n'  (Snowflake default)
-- For \n files (Unix/Linux): RECORD_DELIMITER = '\n'   (also default)
-- For \r files (old Mac):    RECORD_DELIMITER = '\r'   <-- must set explicitly
```

**Tip:** Verify before creating the format:
```sql
SELECT $1, $2, $3
FROM @source_stage/sample_file.csv
    (FILE_FORMAT => (TYPE=CSV FIELD_DELIMITER='|' RECORD_DELIMITER='\r' SKIP_HEADER=1))
LIMIT 5;
```

---

## Bulk Insert Task → COPY INTO (EWI SSC-EWI-SSIS0024)

SnowConvert converts Bulk Insert Tasks to `COPY INTO` with inline FILE_FORMAT:

```sql
COPY INTO target_table
FROM '@my_stage'
PATTERN = '.*data_file.*'
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\r\n',
    SKIP_HEADER = 1,
    NULL_IF = ('', 'NULL', 'null'),
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE;
```

**Post-migration steps**: Create stage, upload files via `PUT`, replace `{STAGE_PLACEHOLDER}`.

**Unsupported features**:
- Native format → export to CSV first (EWI SSC-EWI-SSIS0020)
- LastRow → load to staging, filter with ROW_NUMBER (EWI SSC-EWI-SSIS0021)
- FireTriggers → use Streams + Tasks (EWI SSC-EWI-SSIS0022)

## JavaScript UDF (for algorithmic logic)

```sql
CREATE OR REPLACE FUNCTION udf_validate_checksum(input_value VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
COMMENT = 'Generic checksum/validation algorithm — adapt to your domain'
AS $$
    if (!INPUT_VALUE || INPUT_VALUE.length === 0) return false;
    var digits = INPUT_VALUE.replace(/\D/g, '');
    if (digits.length === 0) return false;
    var sum = 0;
    for (var i = 0; i < digits.length; i++) {
        var d = parseInt(digits[i]);
        if (i % 2 === 1) { d *= 2; if (d > 9) d -= 9; }
        sum += d;
    }
    return (sum % 10 === 0);
$$;
```

**When to use JavaScript vs SQL UDFs:**
- JavaScript: loops, arrays, regex, checksums, complex algorithms
- SQL: Simple CASE mappings, single expressions
- **CRITICAL**: SQL UDFs containing subqueries (SELECT from tables) CANNOT be called inside SP temp table creation. Use inline JOINs instead.

## Inline JOIN Pattern (replacing UDF with subqueries)

When a SQL UDF does `SELECT ... FROM lookup_table`, and you need to call it inside a SP temp table CTAS:

```sql
CREATE TEMP TABLE result AS
SELECT src.*,
    COALESCE(lk.category, 'Unknown') AS category
FROM source_table src
LEFT JOIN lookup_table lk ON src.code = lk.code;
```

## Raising Custom Exceptions in Snowflake Scripting

> **WARNING**: `RAISE EXCEPTION 'message'` is **not valid** Snowflake Scripting syntax
> and causes a SQL compilation error. You must declare a named exception first.

```sql
-- WRONG — causes: SQL compilation error: syntax error ... unexpected 'message'
IF (:v_count = 0) THEN
    RAISE EXCEPTION 'Reference table is empty';
END IF;

-- CORRECT — declare a named exception in the DECLARE block, then raise it
DECLARE
    v_count     INT;
    empty_ref   EXCEPTION (-20001, 'Reference table is empty. Load data before running ETL.');
BEGIN
    SELECT COUNT(*) INTO :v_count FROM PUBLIC.dim_reference;
    IF (:v_count = 0) THEN
        RAISE empty_ref;
    END IF;
    -- ...
END;
```

**Rules:**
- Exception code must be in range `-20000` to `-20999` (user-defined range)
- Each named exception needs a unique code within the procedure
- Multiple exceptions can be declared in the same `DECLARE` block
- Caught via `EXCEPTION WHEN OTHER THEN` (Snowflake does not support named WHEN clauses)

---

## Validation SP Pattern (replaces SSIS setup/validation package)

```sql
CREATE OR REPLACE PROCEDURE sp_validate_env()
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS $$
DECLARE v_count INT;
BEGIN
    SELECT COUNT(*) INTO :v_count
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME IN ('TABLE1', 'TABLE2', 'TABLE3');
    IF (v_count < 3) THEN RETURN 'FAILED: Required tables not found'; END IF;

    SELECT COUNT(*) INTO :v_count FROM reference_table;
    IF (v_count = 0) THEN RETURN 'FAILED: No reference data'; END IF;

    ALTER STAGE IF EXISTS source_stage REFRESH;
    SELECT COUNT(*) INTO :v_count FROM DIRECTORY(@source_stage) WHERE RELATIVE_PATH LIKE '%.csv';
    IF (v_count = 0) THEN RETURN 'WARNING: No CSV files found'; END IF;

    RETURN 'SUCCESS: Validation passed.';
END; $$;
```

**IMPORTANT**: Do NOT use `CASE WHEN (subquery) THEN...` inside `SELECT...INTO`. Snowflake throws `INTO clause is not allowed in this context`. Use simple COUNT + IF pattern.

## Orchestrator SP Pattern

```sql
CREATE OR REPLACE PROCEDURE sp_orchestrator()
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS $$
DECLARE v_setup VARCHAR; v_load VARCHAR;
BEGIN
    CALL sp_validate_env() INTO :v_setup;
    IF (LEFT(v_setup, 6) = 'FAILED') THEN
        RETURN 'ORCHESTRATOR FAILED at Validation: ' || v_setup;
    END IF;
    CALL sp_process_files() INTO :v_load;
    RETURN 'COMPLETED. Validation: ' || v_setup || ' | Processing: ' || v_load;
END; $$;
```

## dbt Project Execution (SnowConvert output)

When SnowConvert generates dbt projects, Data Flow Tasks are executed via:

```sql
EXECUTE DBT PROJECT schema.project_name ARGS='build --target dev'
```

Deploy first using: `snow dbt deploy --schema <schema> --database <db> --force <package_name>`

## Empty String → NULL Replacement

```sql
-- SSIS: DRV - Replace empty fields with Nulls
IFF(col = '', NULL::VARCHAR, col) AS col
-- Or:
NULLIF(TRIM(col), '') AS col
```

## Lookup with Deterministic Ordering (FDM SSC-FDM-SSIS0001)

```sql
LEFT JOIN (
    SELECT key_col, value_col
    FROM reference_table
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key_col ORDER BY id ASC) = 1
) r ON r.key_col = s.key_col
```

## Task DAG Pattern (replaces SQL Server Agent scheduling)

```sql
CREATE OR REPLACE TASK task_root
    WAREHOUSE = WH_NAME
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS SELECT 'started';

CREATE OR REPLACE TASK task_validate
    WAREHOUSE = WH_NAME
    AFTER task_root
AS CALL sp_validate_env();

CREATE OR REPLACE TASK task_process
    WAREHOUSE = WH_NAME
    AFTER task_validate
AS CALL sp_process_files();

-- Enable: child tasks first, then root
-- ALTER TASK task_process RESUME;
-- ALTER TASK task_validate RESUME;
-- ALTER TASK task_root RESUME;
```

> **WARNING**: Do NOT use non-ASCII characters (em dashes `—`, smart quotes `""`,
> accented letters, etc.) in `COMMENT` clauses on `CREATE TASK`. They cause a
> SQL compilation error: `syntax error ... unexpected 'COMMENT'`.
> Use plain ASCII only in all COMMENT strings.

```sql
-- WRONG — em dash causes compile error
CREATE OR REPLACE TASK task_process
    WAREHOUSE = WH_NAME
    AFTER task_root
    COMMENT = 'Loads files — mirrors Data_Load.dtsx'   -- em dash breaks this
AS CALL sp_process_files();

-- CORRECT — plain ASCII only
CREATE OR REPLACE TASK task_process
    WAREHOUSE = WH_NAME
    AFTER task_root
    COMMENT = 'Loads files - mirrors Data_Load.dtsx'   -- hyphen is fine
AS CALL sp_process_files();
```

## Stream + Task Pattern (replaces SSIS trigger-like behavior)

```sql
CREATE OR REPLACE STREAM source_stream ON TABLE staging_table;

CREATE OR REPLACE TASK process_changes
    WAREHOUSE = WH_NAME
    SCHEDULE = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('source_stream')
AS
    INSERT INTO target_table
    SELECT * FROM source_stream WHERE METADATA$ACTION = 'INSERT';
```

## Dynamic Table Pattern (optional analytics layer)

```sql
CREATE OR REPLACE DYNAMIC TABLE dt_summary
    TARGET_LAG = '1 hour'
    WAREHOUSE = WH_NAME
AS
SELECT category, status,
    COUNT(*) AS total_records, AVG(metric_value) AS avg_metric
FROM target_table
GROUP BY ALL;
```

## Common VARCHAR Length Issues

SnowConvert may infer incorrect VARCHAR lengths from SSIS metadata. Always verify column widths against actual source data before deployment.

**Tip:** Run a quick check on sample source files:
```sql
SELECT MAX(LENGTH($1)) AS max_col1, MAX(LENGTH($2)) AS max_col2
FROM @source_stage/sample.csv (FILE_FORMAT => my_format);
```
