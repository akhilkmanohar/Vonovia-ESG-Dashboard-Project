SET NOCOUNT ON;

SELECT 'pillar_detect_E' AS metric,
       CASE WHEN EXISTS (SELECT 1 FROM rpt.v_esg_pbix_dataset WHERE pillar = 'E') THEN 1 ELSE 0 END AS val
UNION ALL
SELECT 'pillar_detect_S',
       CASE WHEN EXISTS (SELECT 1 FROM rpt.v_esg_pbix_dataset WHERE pillar = 'S') THEN 1 ELSE 0 END
UNION ALL
SELECT 'pillar_detect_G',
       CASE WHEN EXISTS (SELECT 1 FROM rpt.v_esg_pbix_dataset WHERE pillar = 'G') THEN 1 ELSE 0 END
UNION ALL
SELECT 'esg_rows_total',
       COUNT_BIG(*) FROM rpt.v_esg_pbix_dataset
UNION ALL
SELECT 'catalog_rows',
       COUNT(*) FROM rpt.v_esg_import_catalog;
