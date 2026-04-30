# SSAS Tabular — Complete Feature Reference

This reference covers every SSAS Tabular feature encountered during migration and its Snowflake equivalent. Load this when a feature needs deeper context during the migration workflow.

---

## 1. Compatibility Levels

| Level | Platform | Format | Key Features Unlocked |
|---|---|---|---|
| 1100 / 1103 | SQL Server 2012–2017 | **XML/ASSL** | Base tabular model |
| 1200 | SQL Server 2016+ / AAS | **JSON** | TOM API, improved DAX engine |
| 1400 | SQL Server 2017+ / AAS | JSON + M | Power Query (M) partitions, OLS, `getitem` relationships |
| 1500 | SQL Server 2019+ / AAS / PBI | JSON + M | **Calculation groups**, many-to-many relationships |
| 1600 | SQL Server 2022 / AAS / PBI | JSON + M | Enhanced time intelligence |
| 1700 | SQL Server 2025 / AAS / PBI | JSON + M | Latest — use for new models |

**Action for 1100/1103:** Export to JSON via SSMS → right-click database → Script Database as → CREATE TO → File. Or upgrade in Visual Studio.

---

## 2. Calculation Groups (compat 1500+)

Calculation groups reduce measure proliferation by applying a reusable DAX pattern across all measures.

### Structure in model.bim
```json
{
  "name": "Time Intelligence",
  "calculationGroup": {
    "precedence": 20,
    "calculationItems": [
      { "name": "MTD",  "expression": "CALCULATE(SELECTEDMEASURE(), DATESMTD(DimDate[Date]))" },
      { "name": "QTD",  "expression": "CALCULATE(SELECTEDMEASURE(), DATESQTD(DimDate[Date]))" },
      { "name": "YTD",  "expression": "CALCULATE(SELECTEDMEASURE(), DATESYTD(DimDate[Date]))" },
      { "name": "PY",   "expression": "CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(DimDate[Date]))" },
      { "name": "Current", "expression": "SELECTEDMEASURE()" }
    ]
  }
}
```

### Key DAX functions in calc groups
| Function | Meaning |
|---|---|
| `SELECTEDMEASURE()` | Placeholder for whichever base measure is in context |
| `SELECTEDMEASURENAME()` | Name of the measure in context |
| `ISSELECTEDMEASURE(m1, m2)` | Is the current measure one of m1 or m2? |
| `SELECTEDMEASUREFORMATSTRING()` | Format string of the current measure |

### Snowflake migration strategy
`convert_dax.py` **expands** each item × each base measure → N×M individual metrics:

| Calc item | Base measure | → Snowflake metric name |
|---|---|---|
| MTD | Total Sales | `total_sales_mtd` |
| YTD | Total Sales | `total_sales_ytd` |
| MTD | Margin | `margin_mtd` |
| YTD | Margin | `margin_ytd` |

Each expanded metric replaces `SELECTEDMEASURE()` with the base measure's translated SQL, then applies the time intelligence expression.

---

## 3. Object-Level Security (OLS) — compat 1400+

OLS hides tables or columns from specific roles at the metadata level (users cannot even see the object exists).

### Table-level OLS
```json
"roles": [{ "tablePermissions": [{ "name": "Salary", "metadataPermission": "none" }] }]
```
→ Snowflake: `REVOKE SELECT ON SCHEMA.SALARY FROM ROLE users_role;`

### Column-level OLS
```json
"columnPermissions": [{ "name": "Base Rate", "metadataPermission": "none" }]
```
→ Snowflake options:
- **Masking policy** (data masked but column visible in schema): `CREATE MASKING POLICY ...`
- **Scoped view** (column omitted entirely): create a role-specific view without the column
- **Column-level GRANT**: `GRANT SELECT (col1, col2, col4) ON TABLE T TO ROLE r;` (omit restricted col)

### Restrictions in SSAS
- Cannot OLS a table that is in the middle of a relationship chain (breaks joins)
- RLS and OLS cannot be combined across different roles on the same table
- `generate_ddl.py` emits both REVOKE and masking policy comments in the DDL

---

## 4. Storage Modes

Each table (and each partition) has an independent storage mode.

| Mode | SSAS behaviour | Snowflake target |
|---|---|---|
| `import` | Cached in VertiPaq RAM; refreshed on schedule | Regular table or **Interactive Table** |
| `directQuery` | Every query hits source live; no cache | **Interactive Table** (low-latency expected) |
| `dual` | Cached when used with import tables; DQ when used with DQ tables | **Interactive Table** |

### Interactive Table decision tree (from assess_deployment.py)

```
Is it a calculated table?           → VIEW  (always)
storage_mode = directQuery?         → INTERACTIVE TABLE  (+40 pts)
Has date-range partitions?          → +20 pts
Is marked as Date Table?            → +15 pts
Is a dimension (one-side only)?     → +15 pts
Has hierarchies?                    → +10 pts
Bidirectional cross-filter?         → −15 pts
Large fact, no partitions?          → −10 pts

Concurrent users > 100?             → +30 pts
Concurrent users > 1000?            → +50 pts total
Queries mostly selective?           → +25 pts
Latency SLA < 1 second?             → +20 pts
Real-time / hourly refresh?         → +10 pts

Score ≥ 70 → INTERACTIVE TABLE
Score 40–69 → REGULAR TABLE with CLUSTER BY  (offer upgrade)
Score < 40  → REGULAR TABLE
```

### Interactive Table constraints (from Snowflake docs)
- **Mandatory:** `CLUSTER BY` clause — choose columns matching your most common WHERE filters
- **Minimum `TARGET_LAG`:** 60 seconds
- **Interactive warehouse minimum auto-suspend:** 24 hours (affects cost floor)
- **Limited benefit for:** `SELECT *`, large fact-fact joins, full year-range scans
- **Maximum benefit for:** date-filtered queries, IN-list filters, < 20% row selectivity

---

## 5. Partitioning Patterns → Snowflake

| SSAS partition pattern | Snowflake equivalent |
|---|---|
| Rolling window (annual) | `CLUSTER BY (YEAR(date_col))` + scheduled `ALTER TABLE ... ADD/DROP PARTITION`-style MERGE |
| Rolling window (monthly) | `CLUSTER BY (DATE_TRUNC('month', date_col))` |
| Incremental (current + history) | Interactive Table with `TARGET_LAG` for hot partition |
| Multi-source (hourly + daily) | Two source tables → UNION ALL in a Dynamic Table |
| DirectQuery partition | Regular Snowflake table + live query |

### Extracting TARGET_LAG from partition expressions
`assess_deployment.py` reads the user's refresh cadence answer and maps it:
- Real-time → `60 seconds`
- Hourly → `1 hour`
- Daily → `1 day`
- Weekly → `7 days`

---

## 6. Perspectives → Separate Semantic Views

Perspectives are viewable subsets of the model for specific business domains (Sales, Finance, HR).

**Important:** Perspectives are NOT a security mechanism — they don't restrict data access.

### In model.bim
```json
"perspectives": [
  { "name": "Sales", "perspectiveTables": [
      { "name": "FactSales", "measures": [{"name": "Total Sales"}], "columns": [{"name": "Region"}] }
  ]}
]
```

### Snowflake migration
Each perspective → a separate Snowflake Semantic View scoped to that perspective's tables/measures:
```yaml
name: sales_semantic_view
tables:
  - name: fact_sales
    base_table: MY_DB.MY_SCHEMA.FACT_SALES
    # Only the columns/metrics in the Sales perspective
```

`generate_semantic_view.py` can accept a `--perspective` flag to filter output to one perspective.

---

## 7. Bidirectional Cross-Filtering

`crossFilteringBehavior: bothDirections` allows filter context to flow in both directions across a relationship (equivalent to a many-to-many workaround).

### Impact on migration
- In SQL views: use **INNER JOIN** (not LEFT JOIN) to propagate filters in both directions
- In Interactive Tables: bidirectional filters create complex join patterns → **reduces suitability score by −15**
- `assess_deployment.py` detects these and factors them into the scoring

### In model.bim
```json
"relationships": [{ "crossFilteringBehavior": "bothDirections", ... }]
```

---

## 8. Calculated Tables

Tables whose data is entirely defined by a DAX expression (not sourced from a data source).

### Detection
Single partition with `source.type == "calculated"` or a top-level `expression` key on the table object.

### Snowflake migration
| Pattern | Target |
|---|---|
| Static lookup (e.g. calendar spine, date table) | `CREATE OR REPLACE VIEW` |
| Aggregated rollup that refreshes | `DYNAMIC TABLE` with `TARGET_LAG` |
| Filtered subset of another table | `CREATE OR REPLACE VIEW` with `WHERE` clause |

`generate_ddl.py` always emits calculated tables as `CREATE OR REPLACE VIEW` with a `/* TODO */` if the DAX wasn't translated.

---

## 9. Date Tables

A table marked as the model's primary date dimension via `"isDateTable": true`.

**Why it matters for migration:**
- Always used as a filter/slicer → primary CLUSTER BY candidate
- Time intelligence functions (TOTALYTD, SAMEPERIODLASTYEAR) require a marked date table
- `assess_deployment.py` adds +15 to Interactive Table score for date tables
- The date key column should be the first CLUSTER BY column

### Snowflake best practice
Create a `DIM_DATE` table or use a date spine CTE:
```sql
-- Simple date spine
CREATE OR REPLACE TABLE DIM_DATE AS
WITH spine AS (
  SELECT DATEADD('day', SEQ4(), '2000-01-01'::DATE) AS date_key
  FROM TABLE(GENERATOR(ROWCOUNT => 18263))  -- 50 years
)
SELECT
  date_key,
  YEAR(date_key) AS year,
  MONTH(date_key) AS month_num,
  DAYOFWEEK(date_key) AS weekday_num,
  DATE_TRUNC('month', date_key) AS month_start,
  DATE_TRUNC('quarter', date_key) AS quarter_start,
  DATE_TRUNC('year', date_key) AS year_start,
  LAST_DAY(date_key) AS month_end
FROM spine;
```

---

## 10. Translations / Localization

Models targeting multiple languages store alternate names for tables, columns, and measures.

### In model.bim
```json
"model": {
  "cultures": [{ "name": "fr-FR", "translations": { "objects": [...] } }]
}
```

### Snowflake migration
- Primary language → table/column/metric names
- Secondary languages → `synonyms` in semantic view YAML:
  ```yaml
  metrics:
    - name: total_sales
      synonyms: ["chiffre d'affaires", "umsatz", "revenue"]
  ```
- Or use column `COMMENT` strings for documentation-level translations

---

## Deployment Type Quick Reference

| Deployment | How to get model.bim | Memory DMV |
|---|---|---|
| On-prem / VM | `C:\...\OLAP\Data\<db>\<model>.bim` or SSMS → Script | SSMS → `$System.DISCOVER_OBJECT_MEMORY_USAGE` |
| Azure Analysis Services | SSMS → connect to `asazure://...` → Script | Azure Portal → Metrics blade |
| Power BI Premium | SSMS → connect to `powerbi://api.powerbi.com/...` → Script | PBI Premium Metrics app |

### Memory → Snowflake warehouse sizing

| SSAS in-memory size | Recommended Snowflake warehouse |
|---|---|
| < 5 GB | XS for queries; S for initial load |
| 5–50 GB | S for queries; M for initial load |
| 50–200 GB | M for queries; L for initial load; consider clustering |
| > 200 GB | L–XL; clustering mandatory; consider Interactive Tables |
