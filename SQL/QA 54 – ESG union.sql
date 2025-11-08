SET NOCOUNT ON;

WITH latest AS (
    SELECT MAX([year]) AS latest_year
    FROM rpt.v_esg_pbix_dataset
)
SELECT section,
       pillar,
       stream,
       [year],
       rows,
       metric,
       value_num
FROM (
    SELECT 'coverage' AS section,
           pillar,
           stream,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset
    GROUP BY pillar, stream

    UNION ALL

    SELECT 'last5' AS section,
           d.pillar,
           d.stream,
           d.[year],
           CAST(NULL AS bigint) AS rows,
           TRY_CAST(CONCAT(d.stream, N' | ', d.metric) AS nvarchar(200)) AS metric,
           TRY_CONVERT(decimal(18,6), d.value_num) AS value_num
    FROM rpt.v_esg_pbix_dataset d
    CROSS JOIN latest l
    WHERE l.latest_year IS NOT NULL
      AND d.[year] >= l.latest_year - 4

    UNION ALL

    SELECT 'density' AS section,
           pillar,
           CAST(N'all' AS nvarchar(20)) AS stream,
           [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset
    GROUP BY pillar, [year]

    UNION ALL

    SELECT 'catalog' AS section,
           pillar,
           CAST(NULL AS nvarchar(20)) AS stream,
           CAST(NULL AS int) AS [year],
           CAST(NULL AS bigint) AS rows,
           TRY_CAST(CONCAT([schema], N'.', view_name) AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_import_catalog
) AS results
ORDER BY section,
         pillar,
         stream,
         [year] DESC,
         metric;
