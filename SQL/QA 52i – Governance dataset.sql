SET NOCOUNT ON;
WITH y AS (
    SELECT MAX(y) AS latest_year FROM (
        SELECT MAX([year]) AS y FROM rpt.v_gov_pbix_dataset
    ) s
),
sums AS (
    SELECT
        SUM(CASE WHEN stream = 'counts'  THEN 1 ELSE 0 END) AS cnt_counts,
        SUM(CASE WHEN stream = 'rates'   THEN 1 ELSE 0 END) AS cnt_rates,
        SUM(CASE WHEN stream = 'amounts' THEN 1 ELSE 0 END) AS cnt_amounts,
        COUNT_BIG(*) AS cnt_total
    FROM rpt.v_gov_pbix_dataset
)
-- coverage
SELECT 'coverage' AS section, 'rpt.v_gov_pbix_dataset' AS source, CAST(NULL AS int) AS [year],
       cnt_total AS rows, CAST(NULL AS nvarchar(200)) AS metric, CAST(NULL AS decimal(18,6)) AS value_num
FROM sums

UNION ALL
SELECT 'coverage_parts','counts',NULL, COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_pbix_dataset WHERE stream='counts'
UNION ALL
SELECT 'coverage_parts','rates',NULL, COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_pbix_dataset WHERE stream='rates'
UNION ALL
SELECT 'coverage_parts','amounts',NULL, COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_pbix_dataset WHERE stream='amounts'

UNION ALL
-- last5 unified
SELECT 'last5','rpt.v_gov_pbix_dataset', d.[year], CAST(NULL AS bigint), CONCAT(d.stream, ' | ', d.metric), d.value_num
FROM rpt.v_gov_pbix_dataset d CROSS JOIN y
WHERE d.[year] >= y.latest_year - 4

UNION ALL
-- density unified
SELECT 'density','rpt.v_gov_pbix_dataset', [year], COUNT_BIG(*), stream, NULL
FROM rpt.v_gov_pbix_dataset
GROUP BY stream, [year]
ORDER BY section, source, [year] DESC;
