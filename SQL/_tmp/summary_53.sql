SET NOCOUNT ON;

DECLARE @catalog_rows bigint = 0;

IF OBJECT_ID(N'rpt.v_gov_import_catalog_plus', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_gov_import_catalog_plus;
END
ELSE IF OBJECT_ID(N'rpt.v_gov_import_catalog', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_gov_import_catalog;
END;

IF OBJECT_ID(N'rpt.v_soc_import_catalog', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_soc_import_catalog;
END
ELSE IF OBJECT_ID(N'rpt.v_social_import_catalog', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_social_import_catalog;
END;

IF OBJECT_ID(N'rpt.v_env_import_catalog', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_env_import_catalog;
END
ELSE IF OBJECT_ID(N'rpt.v_energy_import_catalog', 'V') IS NOT NULL
BEGIN
    SELECT @catalog_rows = @catalog_rows + COUNT(*)
    FROM rpt.v_energy_import_catalog;
END;

SELECT 'has_bi_ro' AS metric,
       CASE WHEN EXISTS (
                SELECT 1
                FROM sys.database_principals
                WHERE name = N'bi_ro'
                  AND type = 'R'
            )
            THEN 1 ELSE 0 END AS val
UNION ALL
SELECT 'rpt_views',
       COUNT(*)
FROM sys.views
WHERE schema_id = SCHEMA_ID(N'rpt')
UNION ALL
SELECT 'gov_pbix_rows',
       COUNT_BIG(*)
FROM rpt.v_gov_pbix_dataset
UNION ALL
SELECT 'catalog_rows',
       @catalog_rows;
