USE [Vonovia_ESG_DB];
GO
/* File: 12 QA – comprehensive health checks.sql (v2)
   Fix: reference derived_unit (not unit) for v_ghg_balance_yearly-anchored checks.
*/

------------------------------------------------------------
-- A) Header map sanity (3 years per workbook)
------------------------------------------------------------
SELECT
  year_label,
  y1_isnull = CASE WHEN y1 IS NULL THEN 1 ELSE 0 END,
  y2_isnull = CASE WHEN y2 IS NULL THEN 1 ELSE 0 END,
  y3_isnull = CASE WHEN y3 IS NULL THEN 1 ELSE 0 END
FROM core.v_ghg_balance_header_map
ORDER BY year_label DESC;
GO

------------------------------------------------------------
-- B) Null density after parsing (yearly fact)
------------------------------------------------------------
SELECT
  year_label, [year],
  rows_total = COUNT(*),
  rows_null_value = SUM(CASE WHEN value_num IS NULL AND NULLIF(value_text,N'') IS NOT NULL THEN 1 ELSE 0 END)
FROM core.v_ghg_balance_yearly
GROUP BY year_label, [year]
ORDER BY year_label DESC, [year] DESC;
GO

-- Sample the actual nulls (up to 100)
SELECT TOP (100) year_label, [year], row_num, label,
       derived_unit AS unit, value_text
FROM core.v_ghg_balance_yearly
WHERE value_num IS NULL AND NULLIF(value_text,N'') IS NOT NULL
ORDER BY year_label DESC, [year] DESC, row_num;
GO

------------------------------------------------------------
-- C) Duplicate primary keys in materialized yearly table
------------------------------------------------------------
WITH d AS (
  SELECT year_label, [year], row_num, label, cnt = COUNT(*)
  FROM core.ghg_balance_yearly
  GROUP BY year_label, [year], row_num, label
)
SELECT * FROM d WHERE cnt > 1
ORDER BY year_label DESC, [year] DESC, row_num;
GO

------------------------------------------------------------
-- D) Country split vs totals (all labels, full reconciliation)
------------------------------------------------------------
WITH ctry AS (
  SELECT year_label, [year], label, SUM(value_num) AS sum_countries
  FROM core.ghg_balance_yearly_country
  GROUP BY year_label, [year], label
),
tot AS (
  SELECT year_label, [year], label, value_num AS total_value
  FROM core.v_ghg_balance_yearly
)
SELECT
  c.year_label, c.[year], c.label,
  c.sum_countries, t.total_value,
  delta = c.sum_countries - t.total_value
FROM ctry c
LEFT JOIN tot t
  ON t.year_label = c.year_label AND t.[year] = c.[year] AND t.label = c.label
ORDER BY c.year_label DESC, c.[year] DESC, c.label;
GO

------------------------------------------------------------
-- E) Classification coverage & unmatched examples
------------------------------------------------------------
SELECT
  total_rows = COUNT(*),
  matched_rows = SUM(CASE WHEN scope IS NOT NULL OR segment IS NOT NULL OR is_total=1 OR is_subtotal=1 THEN 1 ELSE 0 END),
  unmatched_rows = SUM(CASE WHEN (scope IS NULL AND segment IS NULL AND is_total=0 AND is_subtotal=0) THEN 1 ELSE 0 END)
FROM core.v_ghg_balance_tagged;
GO

-- Unmatched list (top 150)
SELECT TOP (150) year_label, [year], row_num, label,
       -- v_ghg_balance_tagged already outputs a column named [unit]; keep it,
       -- but if not present in your current build, select NULL as unit.
       CASE WHEN COLUMNPROPERTY(OBJECT_ID('core.v_ghg_balance_tagged'),'unit','ColumnId') IS NOT NULL
            THEN unit ELSE NULL END AS unit,
       value_num
FROM core.v_ghg_balance_tagged
WHERE (scope IS NULL AND segment IS NULL AND is_total=0 AND is_subtotal=0)
ORDER BY year_label DESC, [year] DESC, row_num;
GO

------------------------------------------------------------
-- F) Mart vs source cross-checks
------------------------------------------------------------
-- F1: mart.v_ghg_total_by_year vs the grand total line in yearly view
WITH src AS (
  SELECT [year], value_num
  FROM core.v_ghg_balance_yearly
  WHERE label LIKE N'%Total portfolio + business operations%'
),
m AS (
  SELECT [year], total_value FROM mart.v_ghg_total_by_year
)
SELECT
  COALESCE(m.[year], s.[year]) AS [year],
  src_total = s.value_num,
  mart_total = m.total_value,
  delta = (m.total_value - s.value_num)
FROM m
FULL OUTER JOIN src s ON s.[year] = m.[year]
ORDER BY [year] DESC;
GO

-- F2: scope-year sums should add up to the same grand total (where scope assigned)
WITH sc AS (
  SELECT [year], SUM(value_num) AS sum_scopes
  FROM core.v_ghg_balance_tagged
  WHERE scope IS NOT NULL
  GROUP BY [year]
),
gt AS (
  SELECT [year], SUM(value_num) AS grand_total
  FROM core.v_ghg_balance_tagged
  WHERE is_total = 1
  GROUP BY [year]
)
SELECT sc.[year], sc.sum_scopes, gt.grand_total, delta = (sc.sum_scopes - gt.grand_total)
FROM sc INNER JOIN gt ON sc.[year] = gt.[year]
ORDER BY sc.[year] DESC;
GO

------------------------------------------------------------
-- G) Unit consistency checks
------------------------------------------------------------
SELECT DISTINCT derived_unit AS unit
FROM core.v_ghg_balance_yearly
ORDER BY unit;
GO

SELECT DISTINCT unit
FROM core.ghg_balance_yearly_country
ORDER BY unit;
GO

------------------------------------------------------------
-- H) Index presence confirmation
------------------------------------------------------------
SELECT
  obj = OBJECT_NAME(i.object_id),
  i.name,
  cols = STUFF((
      SELECT ',' + c.name
      FROM sys.index_columns ic
      JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
      WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
      ORDER BY ic.key_ordinal
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'')
FROM sys.indexes i
WHERE i.object_id IN (OBJECT_ID('core.ghg_balance_yearly'), OBJECT_ID('core.ghg_balance_yearly_country'))
  AND i.is_hypothetical = 0 AND i.type_desc <> 'HEAP'
ORDER BY obj, i.name;
GO
