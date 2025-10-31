USE [Vonovia_ESG_DB];
GO
SELECT * FROM core.v_ghg_balance_country_map ORDER BY year_label, col_key;
SELECT TOP (50) * FROM core.v_ghg_balance_yearly_country ORDER BY year_label DESC, [year] DESC, country_name, row_num;
WITH ctry AS (
  SELECT year_label, [year], label, SUM(value_num) AS sum_countries
  FROM core.v_ghg_balance_yearly_country
  GROUP BY year_label, [year], label
),
tot AS (
  SELECT year_label, [year], label, value_num AS total_value
  FROM core.v_ghg_balance_yearly
)
SELECT c.year_label, c.[year], c.label, c.sum_countries, t.total_value, (c.sum_countries - t.total_value) AS delta
FROM ctry c
LEFT JOIN tot t ON t.year_label = c.year_label AND t.[year] = c.[year] AND t.label = c.label
ORDER BY c.year_label DESC, c.[year] DESC, c.label;
SELECT * FROM core.v_ghg_balance_yearly_country WHERE value_num IS NULL AND NULLIF(value_text, N'') IS NOT NULL;
GO
