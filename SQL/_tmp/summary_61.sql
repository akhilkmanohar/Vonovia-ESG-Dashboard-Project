SET NOCOUNT ON;
SELECT 'soc_rows_total' AS metric, COUNT_BIG(*) AS val
FROM rpt.v_soc_pbix_dataset
UNION ALL
SELECT 'soc_stream_counts', COUNT(DISTINCT stream)
FROM rpt.v_soc_pbix_dataset
UNION ALL
SELECT 'soc_latest_year', MAX([year])
FROM rpt.v_soc_pbix_dataset;
