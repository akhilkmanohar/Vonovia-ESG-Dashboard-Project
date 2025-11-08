SET NOCOUNT ON;

WITH latest AS (
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
    SELECT 'dim_pillar' AS section,
           pillar       AS source,
           CAST(NULL AS int) AS [year],
           COUNT_BIG(*) AS rows,
           pillar_name  AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_dim_pillar
    GROUP BY pillar, pillar_name

    UNION ALL

    SELECT 'dim_stream',
           stream,
           CAST(NULL AS int),
           COUNT_BIG(*),
           stream_name,
           CAST(NULL AS decimal(18,6))
    FROM rpt.v_dim_stream
    GROUP BY stream, stream_name

    UNION ALL

    SELECT 'last5',
           CONCAT(c.pillar, N':', c.stream) AS source,
           c.[year],
           CAST(NULL AS bigint) AS rows,
           c.metric,
           TRY_CONVERT(decimal(18,6), c.value_num) AS value_num
    FROM rpt.v_esg_cards_latest_and_last5 c
    CROSS JOIN latest l
    WHERE c.section = 'last5'
      AND l.latest_year IS NOT NULL
      AND c.[year] >= l.latest_year - 4

    UNION ALL

    SELECT 'latest_by_stream',
           CONCAT(pillar, N':', stream),
           [year],
           CAST(NULL AS bigint),
           metric,
           TRY_CONVERT(decimal(18,6), value_num)
    FROM rpt.v_esg_cards_latest_and_last5
    WHERE section = 'latest_by_stream'

    UNION ALL

    SELECT 'model_catalog',
           category,
           CAST(NULL AS int),
           CAST(NULL AS bigint),
           CONCAT([schema], N'.', view_name),
           CAST(NULL AS decimal(18,6))
    FROM rpt.v_pbix_model_catalog

    UNION ALL

    SELECT 'density',
           CONCAT(pillar, N':', stream),
           [year],
           COUNT_BIG(*) AS rows,
           stream AS metric,
           CAST(NULL AS decimal(18,6)) AS value_num
    FROM rpt.v_esg_pbix_dataset
    GROUP BY pillar, stream, [year]
) q
ORDER BY section,
         source,
         [year] DESC,
         metric;
