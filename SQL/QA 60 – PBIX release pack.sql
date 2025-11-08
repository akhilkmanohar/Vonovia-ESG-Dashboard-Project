SET NOCOUNT ON;

WITH ready AS (
    SELECT name, CAST(value AS nvarchar(200)) AS val
    FROM fn_listextendedproperty (NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    WHERE name IN (N'pbix_release_ready', N'pbix_release_timestamp')
)
SELECT CAST('ready_flags' AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS section,
       CAST(name AS nvarchar(200)) COLLATE DATABASE_DEFAULT AS source,
       CAST(NULL AS int) AS [year],
       CAST(1 AS bigint) AS rows,
       val AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM ready

UNION ALL

SELECT CAST('manifest_all' AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS section,
       CAST(COALESCE(pillar, N'-') AS nvarchar(200)) COLLATE DATABASE_DEFAULT AS source,
       CAST(NULL AS int),
       CAST(NULL AS bigint),
       CONCAT([schema], N'.', view_or_synonym, N' (', object_type, N')') AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_pbix_manifest_all

UNION ALL

SELECT CAST('latest_presence' AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS section,
       CAST(CONCAT(pillar, N'|', stream) AS nvarchar(200)) COLLATE DATABASE_DEFAULT AS source,
       latest_year,
       CAST(is_present_latest AS bigint) AS rows,
       CAST(NULL AS nvarchar(200)) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_esg_latest_presence

UNION ALL

SELECT CAST('model_health' AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS section,
       CAST(metric AS nvarchar(200)) COLLATE DATABASE_DEFAULT AS source,
       CAST(NULL AS int),
       CAST(val AS bigint) AS rows,
       CAST(NULL AS nvarchar(200)) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_model_health_summary

UNION ALL

SELECT CAST('density_last5' AS nvarchar(40)) COLLATE DATABASE_DEFAULT AS section,
       CAST(CONCAT(pillar, N':', stream) AS nvarchar(200)) COLLATE DATABASE_DEFAULT AS source,
        d.[year],
       COUNT_BIG(*) AS rows,
       stream AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_esg_pbix_dataset d
CROSS APPLY (SELECT MAX([year]) AS latest_year FROM rpt.v_esg_pbix_dataset) ly
WHERE ly.latest_year IS NOT NULL
  AND d.[year] >= ly.latest_year - 4
GROUP BY pillar, stream, d.[year]
ORDER BY section, source, [year] DESC, metric;
