SET NOCOUNT ON;

/* Security checks */
SELECT 'security' AS section,
       'role_bi_ro' AS item,
       CASE WHEN EXISTS (
                SELECT 1
                FROM sys.database_principals
                WHERE name = N'bi_ro'
                  AND type = 'R'
            )
            THEN 1 ELSE 0 END AS ok,
       CAST(NULL AS nvarchar(4000)) AS detail
UNION ALL
SELECT 'security',
       'grant_select_on_rpt',
       CASE WHEN EXISTS (
                SELECT 1
                FROM sys.database_permissions p
                JOIN sys.schemas s
                  ON p.major_id = s.schema_id
                JOIN sys.database_principals r
                  ON p.grantee_principal_id = r.principal_id
                WHERE s.name = N'rpt'
                  AND r.name = N'bi_ro'
                  AND p.permission_name = N'SELECT'
            )
            THEN 1 ELSE 0 END,
       CAST(NULL AS nvarchar(4000));

/* Catalog snapshot */
DECLARE @catalog TABLE (
    pillar nvarchar(5),
    [schema] sysname,
    view_name sysname,
    category nvarchar(200),
    notes nvarchar(4000)
);

INSERT INTO @catalog (pillar, [schema], view_name, category, notes)
EXEC rpt.sp_import_catalog_all @pillar_filter = NULL;

SELECT 'catalog' AS section,
       pillar,
       CONCAT([schema], N'.', view_name) AS item,
       category,
       notes
FROM @catalog
ORDER BY pillar, item;

/* Governance density across unified dataset */
SELECT 'gov_density' AS section,
       stream,
       CAST([year] AS nvarchar(10)) AS item,
       COUNT_BIG(*) AS rows
FROM rpt.v_gov_pbix_dataset
GROUP BY stream, [year]
ORDER BY stream, [year] DESC;

/* Governance last five years (per stream/metric) */
DECLARE @latest_year int = (SELECT MAX([year]) FROM rpt.v_gov_pbix_dataset);

SELECT 'gov_latest5' AS section,
       stream,
       [year],
       metric,
       value_num
FROM rpt.v_gov_pbix_dataset
WHERE @latest_year IS NOT NULL
  AND [year] >= @latest_year - 4
ORDER BY stream, [year] DESC, metric;

/* Master refresh dry-run plan */
DECLARE @plan TABLE (
    ordinal int,
    proc_name sysname,
    will_execute bit,
    status nvarchar(200),
    duration_ms int
);

INSERT INTO @plan (ordinal, proc_name, will_execute, status, duration_ms)
EXEC rpt.sp_refresh_all_reporting @areas = NULL, @dry_run = 1;

SELECT 'dry_run_plan' AS section,
       ordinal,
       proc_name,
       will_execute,
       status,
       duration_ms
FROM @plan
ORDER BY ordinal;
