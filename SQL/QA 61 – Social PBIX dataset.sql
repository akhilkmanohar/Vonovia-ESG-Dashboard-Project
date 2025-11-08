SET NOCOUNT ON;

WITH ly AS (SELECT MAX([year]) AS latest_year FROM rpt.v_soc_pbix_dataset)
SELECT 'coverage_by_stream' AS section,
       stream               AS source,
       CAST(NULL AS int)    AS [year],
       COUNT_BIG(*)         AS rows,
       CAST(NULL AS nvarchar(200)) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_soc_pbix_dataset
GROUP BY stream

UNION ALL

SELECT 'latest5' AS section,
       stream     AS source,
       d.[year],
       CAST(NULL AS bigint) AS rows,
       d.metric,
       d.value_num
FROM rpt.v_soc_pbix_dataset d
CROSS JOIN ly
WHERE ly.latest_year IS NOT NULL
  AND d.[year] >= ly.latest_year - 4

UNION ALL

SELECT 'density' AS section,
       stream     AS source,
       [year],
       COUNT_BIG(*) AS rows,
       CAST(NULL AS nvarchar(200)) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_soc_pbix_dataset
GROUP BY stream, [year]

UNION ALL

SELECT 'catalog' AS section,
       [schema]   AS source,
       CAST(NULL AS int) AS [year],
       CAST(NULL AS bigint) AS rows,
       CONCAT([schema],'.',view_name) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_soc_import_catalog
ORDER BY section, source, [year] DESC;
