SET NOCOUNT ON;
SELECT 'pbix_release_ready' AS metric,
       CAST(value AS nvarchar(200)) AS val
FROM fn_listextendedproperty (N'pbix_release_ready', NULL, NULL, NULL, NULL, NULL, NULL)
UNION ALL
SELECT 'pbix_release_timestamp',
       CAST(value AS nvarchar(200))
FROM fn_listextendedproperty (N'pbix_release_timestamp', NULL, NULL, NULL, NULL, NULL, NULL)
UNION ALL
SELECT 'synonyms_pbix',
       CAST(COUNT(*) AS nvarchar(200))
FROM sys.synonyms sy
JOIN sys.schemas s ON s.schema_id = sy.schema_id
WHERE s.name = N'rpt' AND sy.name LIKE N'pbix_%'
UNION ALL
SELECT 'esg_rows_total',
       CAST(COUNT_BIG(*) AS nvarchar(200))
FROM rpt.v_esg_pbix_dataset
UNION ALL
SELECT 'latest_year',
       CAST(MAX([year]) AS nvarchar(200))
FROM rpt.v_esg_pbix_dataset;
