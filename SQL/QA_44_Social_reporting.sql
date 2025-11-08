SET NOCOUNT ON;

WITH view_list AS (
  SELECT * FROM (VALUES
   ('rpt.v_social_workforce_wide'),
   ('rpt.v_social_hs_counts_wide'),
   ('rpt.v_social_hs_rates_wide'),
   ('rpt.v_social_training_wide'),
   ('rpt.v_social_cards_latest'),
   ('rpt.v_social_import_catalog')
  ) AS v(name)
)
SELECT 'exists_check' AS section, v.name AS view_name,
       CASE WHEN OBJECT_ID(v.name,'V') IS NOT NULL THEN 1 ELSE 0 END AS exists_flag
FROM view_list v;

WITH yrs AS (
  SELECT [year] FROM rpt.v_social_workforce_wide
  UNION SELECT [year] FROM rpt.v_social_hs_counts_wide
  UNION SELECT [year] FROM rpt.v_social_hs_rates_wide
  UNION SELECT [year] FROM rpt.v_social_training_wide
), lim AS (SELECT MAX([year]) AS y FROM yrs)
SELECT 'coverage_recent' AS section, y.[year],
       wf = CASE WHEN w.[year] IS NOT NULL THEN 1 ELSE 0 END,
       hc = CASE WHEN c.[year] IS NOT NULL THEN 1 ELSE 0 END,
       hr = CASE WHEN r.[year] IS NOT NULL THEN 1 ELSE 0 END,
       tr = CASE WHEN t.[year] IS NOT NULL THEN 1 ELSE 0 END
FROM (SELECT [year] FROM yrs CROSS JOIN lim WHERE [year] >= lim.y - 14) y
LEFT JOIN rpt.v_social_workforce_wide w ON w.[year] = y.[year]
LEFT JOIN rpt.v_social_hs_counts_wide  c ON c.[year] = y.[year]
LEFT JOIN rpt.v_social_hs_rates_wide   r ON r.[year] = y.[year]
LEFT JOIN rpt.v_social_training_wide   t ON t.[year] = y.[year]
ORDER BY y.[year] DESC;

SELECT TOP (20) 'latest_cards' AS section, *
FROM rpt.v_social_cards_latest
ORDER BY card, metric;

SELECT 'schema_check' AS section, TABLE_SCHEMA + '.' + TABLE_NAME AS view_name,
       COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='rpt'
  AND TABLE_NAME IN ('v_social_workforce_wide','v_social_hs_counts_wide','v_social_hs_rates_wide','v_social_training_wide');

DECLARE @latest INT = (SELECT MAX([year]) FROM (
  SELECT [year] FROM rpt.v_social_workforce_wide
  UNION SELECT [year] FROM rpt.v_social_hs_counts_wide
  UNION SELECT [year] FROM rpt.v_social_hs_rates_wide
  UNION SELECT [year] FROM rpt.v_social_training_wide
) u);
SELECT 'density' AS section, 'workforce' AS src, COUNT(*) AS non_nulls
FROM (SELECT persons, fte FROM rpt.v_social_workforce_wide WHERE [year] = @latest) d
UNION ALL
SELECT 'density','hs_counts', COUNT(*) FROM (SELECT incidents, lost_days, fatalities FROM rpt.v_social_hs_counts_wide WHERE [year] = @latest) d
UNION ALL
SELECT 'density','hs_rates', COUNT(*) FROM (SELECT ltir, trir, severity_rate FROM rpt.v_social_hs_rates_wide WHERE [year] = @latest) d
UNION ALL
SELECT 'density','training', COUNT(*) FROM (SELECT hours_total, participants_total, hours_per_employee FROM rpt.v_social_training_wide WHERE [year] = @latest) d;
