# DAX → SQL Pattern Reference

## Simple Aggregations (auto-translated by pattern matching)

| DAX | SQL |
|---|---|
| `SUM(Table[Col])` | `SUM(col)` |
| `COUNT(Table[Col])` | `COUNT(col)` |
| `DISTINCTCOUNT(Table[Col])` | `COUNT(DISTINCT col)` |
| `AVERAGE(Table[Col])` | `AVG(col)` |
| `MAX(Table[Col])` | `MAX(col)` |
| `MIN(Table[Col])` | `MIN(col)` |
| `COUNTROWS(Table)` | `COUNT(*)` |
| `COUNTA(Table[Col])` | `COUNT(col)` |

## Arithmetic & Conditionals (auto-translated)

| DAX | SQL |
|---|---|
| `DIVIDE(a, b)` | `IFF(b = 0, NULL, a / b)` |
| `DIVIDE(a, b, alt)` | `IFF(b = 0, alt, a / b)` |
| `IF(cond, true, false)` | `CASE WHEN cond THEN true ELSE false END` |
| `SWITCH(expr, v1, r1, v2, r2, else)` | `CASE expr WHEN v1 THEN r1 WHEN v2 THEN r2 ELSE else END` |
| `SWITCH(TRUE(), cond1, r1, cond2, r2)` | `CASE WHEN cond1 THEN r1 WHEN cond2 THEN r2 END` |
| `BLANK()` | `NULL` |
| `ISBLANK(x)` | `x IS NULL` |
| `TRUE()` | `TRUE` |
| `FALSE()` | `FALSE` |
| `AND(a, b)` | `a AND b` |
| `OR(a, b)` | `a OR b` |
| `NOT(x)` | `NOT x` |
| `ABS(x)` | `ABS(x)` |
| `ROUND(x, n)` | `ROUND(x, n)` |
| `INT(x)` | `FLOOR(x)` |
| `FLOOR(x, 1)` | `FLOOR(x)` |
| `CEILING(x, 1)` | `CEIL(x)` |
| `POWER(x, n)` | `POWER(x, n)` |
| `SQRT(x)` | `SQRT(x)` |
| `MOD(x, n)` | `MOD(x, n)` |

## Text Functions (auto-translated)

| DAX | SQL |
|---|---|
| `LEN(x)` | `LENGTH(x)` |
| `LEFT(x, n)` | `LEFT(x, n)` |
| `RIGHT(x, n)` | `RIGHT(x, n)` |
| `MID(x, start, len)` | `SUBSTR(x, start, len)` |
| `UPPER(x)` | `UPPER(x)` |
| `LOWER(x)` | `LOWER(x)` |
| `TRIM(x)` | `TRIM(x)` |
| `SUBSTITUTE(x, old, new)` | `REPLACE(x, old, new)` |
| `CONCATENATE(a, b)` | `a \|\| b` |
| `a & b` | `a \|\| b` |
| `FORMAT(x, "0.00")` | `TO_CHAR(x, '0.00')` |
| `VALUE(x)` | `TRY_TO_NUMBER(x)` |

## Date Functions (auto-translated)

| DAX | SQL |
|---|---|
| `TODAY()` | `CURRENT_DATE()` |
| `NOW()` | `CURRENT_TIMESTAMP()` |
| `YEAR(d)` | `YEAR(d)` |
| `MONTH(d)` | `MONTH(d)` |
| `DAY(d)` | `DAY(d)` |
| `HOUR(d)` | `HOUR(d)` |
| `MINUTE(d)` | `MINUTE(d)` |
| `WEEKDAY(d)` | `DAYOFWEEK(d)` |
| `WEEKNUM(d)` | `WEEKOFYEAR(d)` |
| `EOMONTH(d, 0)` | `LAST_DAY(d)` |
| `DATE(y, m, d)` | `DATE_FROM_PARTS(y, m, d)` |
| `DATEDIFF(interval, d1, d2)` — e.g. "DAY" | `DATEDIFF('day', d1, d2)` |
| `EDATE(d, n)` | `DATEADD('month', n, d)` |

## Relationship Navigation — requires LLM fallback

```
RELATED(DimProduct[Category])
→ Must resolve via the relationship graph.
  In a view: JOIN DimProduct ON FactSales.ProductKey = DimProduct.ProductKey
  Expression becomes: DimProduct.Category

RELATEDTABLE(FactSales)
→ Becomes a correlated subquery or aggregation joined back to the dimension.
  SUM(RELATEDTABLE(FactSales)[Amount]) → SUM(FactSales.Amount) with appropriate GROUP BY
```

## CALCULATE — requires LLM fallback

```
CALCULATE(SUM(Sales[Amount]), Sales[Region] = "West")
→ SUM(CASE WHEN region = 'West' THEN amount END)

CALCULATE(SUM(Sales[Amount]), DATESINPERIOD(...))
→ SUM with a date-range WHERE / window condition

CALCULATE(expr, ALL(Table))
→ Remove the filter on that table — becomes a subquery or window OVER ()

CALCULATE(expr, ALLEXCEPT(Table, Col))
→ Retain filter only on Col — partial scope removal
```

## Time Intelligence — requires LLM fallback

```
TOTALYTD(SUM(Sales[Amount]), Dates[Date])
→ SUM(amount) WHERE date >= DATE_TRUNC('year', CURRENT_DATE) AND date <= CURRENT_DATE
  Or as a window: SUM(amount) OVER (PARTITION BY YEAR(date) ORDER BY date ROWS UNBOUNDED PRECEDING)

SAMEPERIODLASTYEAR(Dates[Date])
→ Date condition: YEAR(date) = YEAR(CURRENT_DATE) - 1 AND MONTH(date) = MONTH(CURRENT_DATE) AND DAY(date) = DAY(CURRENT_DATE)

DATEADD(Dates[Date], -1, YEAR)
→ DATEADD('year', -1, date_col)

DATESMTD / DATESQTD / DATESYTD
→ Date truncation to start of month/quarter/year with range to current

PREVIOUSMONTH(Dates[Date])
→ date BETWEEN DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE)) AND LAST_DAY(DATEADD('month', -1, CURRENT_DATE))
```

## Ranking — requires LLM fallback

```
RANKX(ALL(Products), [Total Sales])
→ RANK() OVER (ORDER BY total_sales DESC)

TOPN(10, Products, [Total Sales], DESC)
→ ... ORDER BY total_sales DESC LIMIT 10
```

## Other Complex Patterns — requires LLM fallback

```
FILTER(Table, condition)
→ Subquery: SELECT * FROM table WHERE condition

VALUES(Table[Col])
→ SELECT DISTINCT col FROM table

HASONEVALUE(Col)
→ COUNT(DISTINCT col) = 1 (or use in HAVING)

CONCATENATEX(Table, Table[Col], ", ")
→ LISTAGG(col, ', ') WITHIN GROUP (ORDER BY col)

MAXX(Table, expression)
→ MAX(expression) over the table rows

MINX / SUMX / AVERAGEX / COUNTX
→ MAX/MIN/SUM/AVG/COUNT applied to an expression column

SELECTEDVALUE(Col, default)
→ Context-dependent; in Snowflake SQL: COALESCE(col, default) or a parameter
```

## RLS Filter Expressions → Row Access Policies

```
DAX role filter on Sales[RegionCode]:
  [RegionCode] = USERNAME()
→ Snowflake Row Access Policy:
  CREATE OR REPLACE ROW ACCESS POLICY rap_sales_region
    AS (region_code VARCHAR) RETURNS BOOLEAN ->
      region_code = CURRENT_USER()
      OR EXISTS (SELECT 1 FROM admin_override WHERE username = CURRENT_USER());

DAX role filter with a mapping table:
  [RegionCode] IN VALUES(UserRegionMap[RegionCode])
→ Row Access Policy with subquery:
  EXISTS (SELECT 1 FROM user_region_map WHERE username = CURRENT_USER() AND region_code = :1)
```
