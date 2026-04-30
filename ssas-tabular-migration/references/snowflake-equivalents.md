# SSAS Tabular → Snowflake Feature Equivalents

## Concept Mapping

| SSAS Tabular Concept | Snowflake Equivalent | Notes |
|---|---|---|
| **Table** | `TABLE` | Direct 1:1 |
| **Calculated Column** | Column in `VIEW` (SQL expression) | Cannot be in base table; emitted as a view |
| **Measure** | Metric in Snowflake Semantic View | Also emitted as SQL in view |
| **Calculation Group item** | N×M expanded metrics in semantic view | convert_dax.py expands each item × each base measure |
| **Relationship (one-directional)** | `LEFT JOIN` in view / semantic view `joins` | Inactive relationships flagged; handle manually |
| **Relationship (bidirectional)** | `INNER JOIN` in view | Bidirectional = filter flows both ways |
| **Role (RLS filter)** | `Row Access Policy` | One policy per filtered table per role |
| **Row-Level Security filter** | `ROW ACCESS POLICY` expression | DAX `USERNAME()` → `CURRENT_USER()` |
| **OLS — table-level** | `REVOKE SELECT ON TABLE` for role | `metadataPermission: none` on tablePermission |
| **OLS — column-level** | Column masking policy or scoped GRANT SELECT | `metadataPermission: none` on columnPermission |
| **Perspective** | Separate Snowflake Semantic View | Not a security boundary — UX only |
| **Hierarchy** | Dimension columns + naming convention | Parent-child → recursive CTE |
| **KPI** | Semantic view metric with threshold annotation | No native KPI object in Snowflake |
| **Partition** | `CLUSTER BY` key + scheduled refresh | Partition filter column → clustering column |
| **Calculated Table** | `VIEW` or `DYNAMIC TABLE` | Static → VIEW; self-refreshing → DYNAMIC TABLE |
| **Date Table** | Pre-built `DIM_DATE` table | Mark primary date key as first CLUSTER BY column |
| **Translations** | `synonyms` in semantic view YAML | Alternate language names as synonyms |
| **Impersonation** | Service account or CURRENT_USER passthrough | Match to Snowflake auth strategy |
| **VertiPaq in-memory** | Snowflake result cache + clustering | No explicit action; result cache is automatic |
| **DirectQuery** | Already SQL — simplest migration | Just redirect connection to Snowflake |
| **Composite model** | Mixed table types per assessment | Import → regular/interactive; DQ → interactive |

---

## Data Type Mapping

| SSAS / Power BI Type | Snowflake Type |
|---|---|
| `Int64` | `BIGINT` |
| `Double` | `FLOAT` |
| `Decimal` | `NUMBER(38, 10)` |
| `Currency` | `NUMBER(19, 4)` |
| `String` | `VARCHAR` |
| `DateTime` | `TIMESTAMP_NTZ` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Boolean` | `BOOLEAN` |
| `Binary` | `BINARY` |
| `Variant` (PBI auto) | `VARIANT` |

---

## Storage Mode → Snowflake Target

| Storage Mode | Signals | Snowflake Table Type |
|---|---|---|
| `import` + high concurrency + selective queries | assess score ≥ 70 | **`INTERACTIVE TABLE`** + `TARGET_LAG` |
| `import` + moderate usage | assess score 40–69 | Regular `TABLE` + `CLUSTER BY` |
| `import` + low concurrency or full scans | assess score < 40 | Regular `TABLE` |
| `directQuery` | Always interactive (score +40) | **`INTERACTIVE TABLE`** |
| `dual` | Serves both modes | **`INTERACTIVE TABLE`** |
| Calculated table | Detected via partition type | **`VIEW`** (always) |

---

## Interactive Table — Syntax Reference

### When to use
From Snowflake docs: optimised for **selective workloads** with **high concurrency**.
- Best for: WHERE clauses filtering < 20% of rows, repeated query shapes, real-time dashboards
- Limited benefit: `SELECT *`, large fact-to-fact joins, full-year date ranges

### Create syntax
```sql
-- Step 1: Create with a standard warehouse
CREATE INTERACTIVE TABLE MY_DB.MY_SCHEMA.FACT_SALES
  CLUSTER BY (order_date, region_code)    -- match your most common WHERE filters
  TARGET_LAG = '1 hour'                   -- minimum 60 seconds
  WAREHOUSE = maintenance_wh              -- standard WH for refreshes
  INITIALIZATION_WAREHOUSE = xl_init_wh  -- larger WH for initial load (optional)
AS SELECT * FROM source_table;

-- Step 2: Create interactive warehouse (XSMALL / SMALL / MEDIUM)
CREATE OR REPLACE INTERACTIVE WAREHOUSE bi_serving_wh
  TABLES (FACT_SALES, DIM_DATE, DIM_PRODUCT)
  WAREHOUSE_SIZE = 'SMALL';

-- Step 3: Resume the interactive warehouse
ALTER WAREHOUSE bi_serving_wh RESUME;

-- Manual refresh (ad-hoc)
ALTER INTERACTIVE TABLE FACT_SALES REFRESH;
```

### Interactive warehouse sizing guide
| Concurrent users | Recommended size |
|---|---|
| < 50 | XSMALL |
| 50–500 | SMALL |
| > 500 | MEDIUM |

### Cost note
Interactive warehouses have a **minimum auto-suspend of 24 hours**. This means the warehouse stays running (billing) for at least 24 hours after last use. Factor this into the cost model when recommending to users.

---

## Row Access Policy Template (RLS)

```sql
-- One policy per role × table combination
CREATE OR REPLACE ROW ACCESS POLICY rap_{table}_{role}
  AS ({filter_col} {col_type}) RETURNS BOOLEAN ->
    -- Match column value to current user's permitted values
    EXISTS (
      SELECT 1 FROM {mapping_table}
      WHERE username = CURRENT_USER()
        AND {filter_col} = :1
    )
    -- Bypass for admins
    OR IS_ROLE_IN_SESSION('SYSADMIN');

ALTER TABLE {schema}.{table}
  ADD ROW ACCESS POLICY rap_{table}_{role} ON ({filter_col});
```

---

## Column-Level OLS → Snowflake Options

| Approach | When to use | SQL |
|---|---|---|
| **Masking policy** | Column should be visible in schema but data masked | `CREATE MASKING POLICY; ALTER TABLE ... MODIFY COLUMN ... SET MASKING POLICY` |
| **Scoped view** | Column should be completely invisible | Create role-specific view omitting the column |
| **Column-level GRANT** | Grant only specific columns, not the whole table | `GRANT SELECT (col1, col2) ON TABLE t TO ROLE r;` (omit restricted col) |

---

## Partition → Clustering Key Examples

```sql
-- Annual partition → cluster by year
ALTER TABLE FACT_SALES CLUSTER BY (YEAR(order_date));

-- Monthly partition → cluster by month truncation
ALTER TABLE FACT_SALES CLUSTER BY (DATE_TRUNC('month', order_date));

-- Discrete key partition (e.g. by region)
ALTER TABLE FACT_SALES CLUSTER BY (region_code);

-- Composite: date + region
ALTER TABLE FACT_SALES CLUSTER BY (DATE_TRUNC('month', order_date), region_code);
```

---

## Parent-Child Hierarchy → Recursive CTE

```sql
-- SSAS parent-child hierarchy (e.g. employee org chart)
WITH RECURSIVE org_hierarchy AS (
  -- Anchor: top-level nodes (no manager)
  SELECT employee_id, manager_id, name, 0 AS depth, name AS path
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  -- Recursive: children
  SELECT e.employee_id, e.manager_id, e.name,
         h.depth + 1,
         h.path || ' > ' || e.name
  FROM employees e
  JOIN org_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM org_hierarchy ORDER BY path;
```

---

## Semantic View YAML — Quick Reference

```yaml
name: my_semantic_view

tables:
  - name: fact_sales                       # logical name (used in joins)
    base_table: MY_DB.MY_SCHEMA.FACT_SALES # fully qualified physical table
    primary_key: [sale_id]
    dimensions:
      - name: region_code
        expr: region_code
        data_type: VARCHAR
        synonyms: ["region", "territory"]
    facts:
      - name: amount
        expr: amount
        data_type: FLOAT
    metrics:
      - name: total_sales
        expr: SUM(amount)
        synonyms: ["revenue", "total revenue", "chiffre d'affaires"]

joins:
  - name: fact_sales_to_dim_product
    left_table:  fact_sales
    right_table: dim_product
    relationship: many_to_one
    join_type: left                        # use inner for bidirectional filters
    join_condition:
      left_expr: product_key
      right_expr: product_key
```
