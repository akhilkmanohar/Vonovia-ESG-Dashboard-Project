SET NOCOUNT ON;
SELECT 'synonyms_pbix' AS metric,
       COUNT(*) AS val
FROM sys.synonyms sy
JOIN sys.schemas s ON s.schema_id = sy.schema_id
WHERE s.name = N'rpt' AND sy.name LIKE N'pbix_%'
UNION ALL
SELECT 'esg_rows_total',
       COUNT_BIG(*) FROM rpt.v_esg_pbix_dataset
UNION ALL
SELECT 'latest_year',
       MAX([year]) FROM rpt.v_esg_pbix_dataset;
