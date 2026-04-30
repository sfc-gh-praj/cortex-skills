# Known Issues — SSAS Tabular → Snowflake Migration

General reference for recurring failure patterns encountered during SSAS Tabular model
migrations.  Each entry applies to any SSAS model — none are project-specific.

> **Usage:** When a migration phase fails, scan this table for the symptom.  If found,
> apply the fix and add a note to `migration_status.md`.  If a new issue is discovered,
> add it here so future migrations benefit.

---

## 1 — Column Naming Issues

### 1.1 Reserved-word column alias

| Field | Detail |
|---|---|
| **Symptom** | `invalid identifier 'DATE'` (or `TIME`, `YEAR`, `MONTH`, `ORDER`, `STATUS`, `VALUE`, `NAME`, `TYPE`, …) in DDL deployment or semantic view deployment |
| **Root cause** | A column was aliased to a Snowflake SQL reserved word in the generated `CREATE VIEW` statement |
| **Detection** | Run `generate_semantic_view.py` — it now emits `⚠ RESERVED WORD` warnings at generation time.  Or scan the DDL for `AS "DATE"`, `AS DATE`, etc. |
| **Fix** | In `ssas_inventory.json`, set `sf_column_map["<OrigName>"] = "<SAFE_ALIAS>"` (e.g. `"Date" → "FULLDATE"`).  Regenerate DDL and semantic view YAML. |

---

### 1.2 `sf_column_map` entry is `null`

| Field | Detail |
|---|---|
| **Symptom** | Column appears in the semantic view YAML with its SSAS name instead of the Snowflake physical name; deploy fails with `invalid identifier` |
| **Root cause** | Column was renamed in Power Query / M code before loading.  `parse_bim.py` column reconciliation could not auto-match it and left the entry `null` |
| **Detection** | `grep '"sf_column_map"' ssas_inventory.json` — look for `"ColumnName": null` entries |
| **Fix** | Manually set `inventory["tables"][i]["sf_column_map"]["OrigName"] = "PHYSICAL_NAME"` in the JSON, then regenerate DDL and YAML |

---

### 1.3 Physical name mismatch after table rename

| Field | Detail |
|---|---|
| **Symptom** | `Object 'TABLENAME' does not exist` during semantic view or DDL deployment |
| **Root cause** | SSAS model table name differs from the Snowflake physical table name (Power Query step renamed the source) |
| **Detection** | Compare `inventory["tables"][i]["name"]` with actual Snowflake table name: `SHOW TABLES IN SCHEMA <DB.SCHEMA>;` |
| **Fix** | Re-run `parse_bim.py --source-db` to refresh `sf_column_map` and `sf_row_count`; or manually correct `base_table.table` in the semantic view YAML |

---

## 2 — Semantic View YAML Issues

### 2.1 Fact expression contains aggregate function

| Field | Detail |
|---|---|
| **Symptom** | `Invalid fact definition for '<table>.<col>': A fact must directly refer to a column … without an aggregate` |
| **Root cause** | A DAX calculated column was translated with `SUM()` / `COUNT()` wrapping (LLM treated it as a measure rather than a row-level expression) |
| **Detection** | `generate_semantic_view.py` now emits `⚠ FACT AGGREGATE STRIPPED` warnings and auto-fixes these at YAML generation time |
| **Fix** | Remove the aggregate wrapper: `SUM(A) - SUM(B)` → `A - B`.  Automatic since skill update; re-run `generate_semantic_view.py` on existing YAML if needed |

---

### 2.2 Metric expression has a cross-table column reference

| Field | Detail |
|---|---|
| **Symptom** | `invalid identifier 'OTHERTABLE.COLUMNNAME'` during semantic view deployment |
| **Root cause** | DAX measure referenced a column from another table using `'TableName'[Column]` syntax; the LLM translated it literally as `OtherTable.Column` |
| **Detection** | `generate_semantic_view.py` now emits `⚠ CROSS-TABLE REF` warnings at generation time |
| **Fix** | Replace the cross-table ref with the equivalent column accessible in the current table's context (e.g. a date column on the fact table: `DimDate.FULLDATE` → `ORDERDATE`) |

---

### 2.3 Metric expression references another metric by name

| Field | Detail |
|---|---|
| **Symptom** | `invalid identifier 'METRICNAME'` during semantic view deployment |
| **Root cause** | A compound DAX measure referenced another measure name, which the LLM translated as a bare SQL identifier (not supported in semantic view metric exprs) |
| **Detection** | `generate_semantic_view.py` now emits `⚠ METRIC REF` warnings at generation time.  Also visible as bare `[MetricName]` in the YAML |
| **Fix** | Use `build_dax_dag.py` + `convert_dax.py --dag` so compound measures are translated with their dependencies' SQL inlined.  Alternatively, manually substitute the dependency's SQL expression inline |

---

### 2.4 LLM returns SQL wrapped in markdown code fences

| Field | Detail |
|---|---|
| **Symptom** | `sql_translation` field in `ssas_measures_translated.json` contains `` ```sql … ``` `` fences; semantic view deploy fails with parse errors |
| **Root cause** | `SNOWFLAKE.CORTEX.COMPLETE` ignores the "no markdown" instruction and wraps the SQL in a code fence (or prefixes with "Wait, let me reconsider…") |
| **Detection** | `grep '^\`\`\`' ssas_measures_translated.json` — any matches indicate contaminated output |
| **Fix** | Automatic since skill update — `_clean_llm_sql()` in `convert_dax.py` strips fences at translation time.  For existing output: re-run `convert_dax.py` (or manually remove fences in the JSON) |

---

### 2.5 Semantic view YAML contains `$$` in expression strings

| Field | Detail |
|---|---|
| **Symptom** | `deploy_semantic_view.py` fails with a Snowflake SQL quoting error |
| **Root cause** | A metric expression contains `$$` (Snowflake's dollar-quote delimiter), which conflicts with the outer `$$…$$` quoting used by `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` |
| **Detection** | `grep '\$\$' ssas_semantic_view.yaml` |
| **Fix** | `deploy_semantic_view.py` auto-detects this and falls back to single-quote escaping.  If the fallback also fails, manually replace `$$` in the expression with an equivalent that avoids the delimiter |

---

## 3 — Deployment Issues

### 3.1 `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` — "Object does not exist"

| Field | Detail |
|---|---|
| **Symptom** | `Object 'TABLENAME' does not exist` error when calling the procedure, even though the view exists |
| **Root cause** | The procedure resolves `base_table` names against the **current session schema**.  If `USE DATABASE` / `USE SCHEMA` were not set before the CALL, Snowflake looks in the wrong schema |
| **Detection** | Check `deploy_semantic_view.py` logs — `USE DATABASE` and `USE SCHEMA` statements should appear before the `CALL` |
| **Fix** | Already fixed in `deploy_semantic_view.py` — `USE DATABASE` and `USE SCHEMA` are set automatically before the CALL.  If calling manually, always set session context first |

---

### 3.2 Migration restarts from Phase 1 on session resume

| Field | Detail |
|---|---|
| **Symptom** | A new chat session begins Phase 1 from scratch even though several phases were already completed |
| **Root cause** | `migration_status.md` was not read before starting; the skill defaulted to Phase 1 |
| **Detection** | Check whether `<OUTPUT_DIR>/migration_status.md` exists |
| **Fix** | Run `update_migration_status.py --read` at the start of every new session; the skill now checks for an existing status file and shows a resume banner.  Skip all ✅ Completed phases |

---

### 3.3 Translation order produces broken compound metrics

| Field | Detail |
|---|---|
| **Symptom** | A compound measure's `sql_translation` contains bare metric names like `InternetPreviousQuarterSales` instead of SQL, because the dependency was not yet translated when the compound measure was processed |
| **Root cause** | `convert_dax.py` processes tables in iteration order; if a dependency measure appears in a later table, it hasn't been translated yet when the compound measure is encountered |
| **Detection** | Inspect `ssas_measures_translated.json` — compound measures contain `sql_translation` values that are not valid SQL expressions |
| **Fix** | Use `build_dax_dag.py` to generate `dax_dag.json`, then run `convert_dax.py --dag dax_dag.json`.  This guarantees topological translation order with dependency SQL context |

---

*Last updated: 2026-04-29 — based on AdventureWorks DW migration session.  All patterns are general and apply to any SSAS Tabular model.*
