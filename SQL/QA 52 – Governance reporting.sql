SET NOCOUNT ON;
WITH y AS (
    SELECT MAX(y) AS latest_year FROM (
        SELECT MAX([year]) AS y FROM rpt.v_gov_counts_long
        UNION ALL SELECT MAX([year]) FROM rpt.v_gov_rates_long
        UNION ALL SELECT MAX([year]) FROM rpt.v_gov_amounts_long
    ) s
)
SELECT 'coverage' AS section, 'rpt.v_gov_counts_long' AS source, CAST(NULL AS int) AS [year],
       COUNT_BIG(*) AS rows, CAST(NULL AS nvarchar(200)) AS metric, CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_gov_counts_long
UNION ALL
SELECT 'coverage','rpt.v_gov_rates_long',NULL,COUNT_BIG(*),NULL,NULL FROM rpt.v_gov_rates_long
UNION ALL
SELECT 'coverage','rpt.v_gov_amounts_long',NULL,COUNT_BIG(*),NULL,NULL FROM rpt.v_gov_amounts_long
UNION ALL
SELECT 'latest_cards', CONCAT('rpt.v_gov_cards_latest_and_last5 (',cl.stream,')'),
       cl.[year], CAST(NULL AS bigint), cl.metric, cl.value_num
FROM rpt.v_gov_cards_latest_and_last5 cl
CROSS APPLY (SELECT latest_year FROM y) L
WHERE cl.section = 'latest_by_stream' AND cl.[year] = L.latest_year
UNION ALL
SELECT 'last5_counts','rpt.v_gov_counts_long', c.[year], CAST(NULL AS bigint), c.[measure_group], c.[value_num]
FROM rpt.v_gov_counts_long c CROSS JOIN y
WHERE c.[year] >= y.latest_year - 4
UNION ALL
SELECT 'last5_rates','rpt.v_gov_rates_long', r.[year], CAST(NULL AS bigint), r.[rate_metric], r.[value_num]
FROM rpt.v_gov_rates_long r CROSS JOIN y
WHERE r.[year] >= y.latest_year - 4
UNION ALL
SELECT 'last5_amounts','rpt.v_gov_amounts_long', a.[year], CAST(NULL AS bigint), a.[measure_group], TRY_CONVERT(decimal(18,4), a.[amount_eur])
FROM rpt.v_gov_amounts_long a CROSS JOIN y
WHERE a.[year] >= y.latest_year - 4
UNION ALL
SELECT 'catalog','rpt.v_gov_import_catalog', NULL, CAST(NULL AS bigint),
       CONCAT([schema],'.',view_name), CAST(NULL AS decimal(18,6))
FROM rpt.v_gov_import_catalog
UNION ALL
SELECT 'density','rpt.v_gov_counts_long', [year], COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_counts_long GROUP BY [year]
UNION ALL
SELECT 'density','rpt.v_gov_rates_long',  [year], COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_rates_long  GROUP BY [year]
UNION ALL
SELECT 'density','rpt.v_gov_amounts_long',[year], COUNT_BIG(*), NULL, NULL
FROM rpt.v_gov_amounts_long GROUP BY [year];
