# Snowflake Implementation Patterns for SSIS Migration

Proven patterns from real SSIS-to-Snowflake migrations. Use these as templates when generating Phase 4 code.

> Source: [SnowConvert AI - SSIS Translation Reference](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/ssis/README)

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

**SP-based style (DIRECTORY):**
```sql
CREATE OR REPLACE PROCEDURE sp_process_files()
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS $$
DECLARE
    v_file_path VARCHAR;
    v_file_name VARCHAR;
    c1 CURSOR FOR
        SELECT RELATIVE_PATH
        FROM DIRECTORY(@source_stage)
        WHERE RELATIVE_PATH LIKE '%.csv'
        ORDER BY RELATIVE_PATH;
BEGIN
    ALTER STAGE source_stage REFRESH;
    OPEN c1;
    FOR rec IN c1 DO
        v_file_path := rec.RELATIVE_PATH;
        v_file_name := SPLIT_PART(v_file_path, '/', -1);
        -- Per-file processing here
    END FOR;
    CLOSE c1;
    RETURN 'SUCCESS';
END; $$;
```

**Key points:**
- Always `ALTER STAGE REFRESH` before reading DIRECTORY
- Stage must have `DIRECTORY = (ENABLE = TRUE)`
- `RELATIVE_PATH` includes subdirectory prefixes (e.g., `batch_0/file.csv`)
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

-- Enable: ALTER TASK task_root RESUME;
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
