SET NOCOUNT ON;

WITH yr AS (
    SELECT MIN([year]) AS min_year,
           MAX([year]) AS max_year,
           COUNT(*)     AS year_count
    FROM rpt.v_dim_year
),
latest AS (
    SELECT MAX([year]) AS latest_year
    FROM rpt.v_esg_pbix_dataset
)
SELECT section,
       source,
       [year],
       rows,
       metric,
       value_num
FROM (
    -- Dimension coverage (min/max/count)
    SELECT 'dim_year_coverage' AS section,
           'rpt.v_dim_year'    AS source,
           CAST(NULL AS int)   AS [year],
           year_count          AS rows,
           TRY_CAST(CONCAT(min_year, N'-', max_year) AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM yr

    UNION ALL

    -- Detect dataset years that are missing from dimension
    SELECT 'dim_year_vs_dataset' AS section,
           'missing_in_dim_year' AS source,
           d.[year],
           CAST(NULL AS bigint)  AS rows,
           CAST(NULL AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM (
        SELECT DISTINCT TRY_CONVERT(int, [year]) AS [year]
        FROM rpt.v_esg_pbix_dataset
        WHERE [year] IS NOT NULL
    ) AS d
    LEFT JOIN rpt.v_dim_year y
        ON y.[year] = d.[year]
    WHERE y.[year] IS NULL

    UNION ALL

    -- Density: rows per pillar/stream for last five years
    SELECT 'density_latest5' AS section,
           CONCAT(e.pillar, N':', e.stream) AS source,
           e.[year],
           COUNT_BIG(*) AS rows,
           TRY_CAST(e.stream AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset e
    CROSS JOIN latest l
    WHERE l.latest_year IS NOT NULL
      AND e.[year] >= l.latest_year - 4
    GROUP BY e.pillar, e.stream, e.[year]

    UNION ALL

    -- Manifest reveal
    SELECT 'manifest' AS section,
           COALESCE(pillar, N'esg') AS source,
           CAST(NULL AS int)        AS [year],
           CAST(NULL AS bigint)     AS rows,
           TRY_CAST(CONCAT([schema], N'.', view_name) AS nvarchar(200)) AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_pbix_import_manifest
) AS q
ORDER BY section,
         source,
         [year] DESC,
         metric;
