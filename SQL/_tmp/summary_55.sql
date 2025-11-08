SET NOCOUNT ON;

SELECT 'dim_year_count' AS metric,
       COUNT(*)         AS val
FROM rpt.v_dim_year
UNION ALL
SELECT 'dim_year_min',
       MIN([year])
FROM rpt.v_dim_year
UNION ALL
SELECT 'dim_year_max',
       MAX([year])
FROM rpt.v_dim_year
UNION ALL
SELECT 'esg_rows_total',
       COUNT_BIG(*)
FROM rpt.v_esg_pbix_dataset
UNION ALL
SELECT 'manifest_rows',
       COUNT_BIG(*)
FROM rpt.v_pbix_import_manifest;
