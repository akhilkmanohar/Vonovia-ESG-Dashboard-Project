SET NOCOUNT ON;

SELECT 'synonyms_exists' AS section,
       sy.name           AS source,
       CAST(NULL AS int) AS [year],
       1                 AS rows,
       sy.base_object_name AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM sys.synonyms sy
JOIN sys.schemas s ON s.schema_id = sy.schema_id
WHERE s.name = N'rpt'
  AND sy.name LIKE N'pbix_%'

UNION ALL

SELECT 'synonym_targets' AS section,
       sy.name,
       CAST(NULL AS int),
       1 AS rows,
       CASE WHEN OBJECT_ID(sy.base_object_name, N'V') IS NOT NULL THEN N'ok'
            ELSE N'missing_target' END AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM sys.synonyms sy
JOIN sys.schemas s ON s.schema_id = sy.schema_id
WHERE s.name = N'rpt'
  AND sy.name LIKE N'pbix_%'

UNION ALL

SELECT 'role_members' AS section,
       dp2.name        AS source,
       CAST(NULL AS int),
       1 AS rows,
       N'bi_ro'        AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM sys.database_role_members rm
JOIN sys.database_principals dp1 ON dp1.principal_id = rm.role_principal_id AND dp1.name = N'bi_ro'
JOIN sys.database_principals dp2 ON dp2.principal_id = rm.member_principal_id

UNION ALL

SELECT 'manifest' AS section,
       COALESCE(pillar, N'ESG') AS source,
       CAST(NULL AS int),
       CAST(NULL AS bigint),
       CONCAT([schema], N'.', view_name) AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_pbix_import_manifest

UNION ALL

SELECT 'latest5_check' AS section,
       CONCAT(pillar, N':', stream) AS source,
       [year],
       CAST(NULL AS bigint) AS rows,
       metric,
       value_num
FROM rpt.v_esg_cards_latest_and_last5
WHERE section = N'last5'

UNION ALL

SELECT 'density' AS section,
       CONCAT(pillar, N':', stream) AS source,
       [year],
       COUNT_BIG(*) AS rows,
       stream AS metric,
       CAST(NULL AS decimal(18,6)) AS value_num
FROM rpt.v_esg_pbix_dataset
GROUP BY pillar, stream, [year]
ORDER BY section, source, [year] DESC, metric;
