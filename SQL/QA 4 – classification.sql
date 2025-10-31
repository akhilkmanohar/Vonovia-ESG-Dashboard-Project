USE [Vonovia_ESG_DB];
GO

-- 1) Rows with no rule match (we will refine rules until this list is small)
SELECT year_label, [year], row_num, label, unit, value_num
FROM core.v_ghg_balance_tagged
WHERE (scope IS NULL AND segment IS NULL AND is_total=0 AND is_subtotal=0)
ORDER BY year_label DESC, [year] DESC, row_num;

-- 2) Coverage stats
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN scope IS NOT NULL OR segment IS NOT NULL OR is_total=1 OR is_subtotal=1 THEN 1 ELSE 0 END) AS matched_rows
FROM core.v_ghg_balance_tagged;

-- 3) Quick rollup by scope/segment
SELECT [year], scope, segment, SUM(value_num) AS value_sum
FROM core.v_ghg_balance_tagged
GROUP BY [year], scope, segment
ORDER BY [year] DESC, scope, segment;
GO
