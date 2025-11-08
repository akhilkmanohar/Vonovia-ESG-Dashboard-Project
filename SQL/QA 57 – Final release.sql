SET NOCOUNT ON;

WITH ly AS (SELECT latest_year FROM rpt.v_esg_latest_year)
SELECT section,
       source,
       [year],
       rows,
       metric,
       value_num
FROM (
    SELECT 'coverage' AS section,
           'rpt.v_esg_pbix_dataset' AS source,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset

    UNION ALL

    SELECT 'coverage' AS section,
           'rpt.v_dim_year' AS source,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_dim_year

    UNION ALL

    SELECT 'coverage' AS section,
           'rpt.v_pbix_import_manifest' AS source,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_pbix_import_manifest

    UNION ALL

    SELECT 'coverage' AS section,
           'rpt.v_esg_cards_latest_and_last5' AS source,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_cards_latest_and_last5

    UNION ALL

    SELECT 'latest_year' AS section,
           'latest_year' AS source,
           CAST(ly.latest_year AS int),
           CAST(NULL AS bigint),
           CAST(NULL AS nvarchar(200)),
           CAST(NULL AS decimal(18,6))
    FROM ly

    UNION ALL

    SELECT 'streams_present' AS section,
           stream AS source,
           CAST(ly.latest_year AS int) AS [year],
           COUNT_BIG(*) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset d
    CROSS JOIN ly
    WHERE d.[year] = ly.latest_year
    GROUP BY stream, ly.latest_year

    UNION ALL

    SELECT 'gaps' AS section,
           pillar AS source,
           CAST([year] AS int) AS [year],
           CAST(NULL AS bigint) AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_year_gaps

    UNION ALL

    SELECT 'duplicates' AS section,
           CONCAT(pillar, N':', stream) AS source,
           CAST([year] AS int) AS [year],
           dup_rows AS rows,
           CAST(metric AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_duplicates_scan

    UNION ALL

    SELECT 'catalog' AS section,
           COALESCE(CAST(pillar AS nvarchar(20)), N'ESG') AS source,
           CAST(NULL AS int) AS [year],
           CAST(NULL AS bigint) AS rows,
           CAST(CONCAT([schema], N'.', view_name) AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_pbix_model_catalog

    UNION ALL

    SELECT 'density_latest5' AS section,
           CONCAT(pillar, N':', stream) AS source,
           CAST(d.[year] AS int) AS [year],
           COUNT_BIG(*) AS rows,
           stream AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset d
    CROSS JOIN ly
    WHERE ly.latest_year IS NOT NULL
      AND d.[year] >= ly.latest_year - 4
    GROUP BY pillar, stream, d.[year]
) AS results
ORDER BY section,
         source,
         [year] DESC,
         metric;
