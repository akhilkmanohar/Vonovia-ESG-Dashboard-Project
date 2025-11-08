IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- Ensure unified ESG catalog view exists (placeholder when module 54 has not been applied)
IF OBJECT_ID(N'rpt.v_esg_import_catalog', N'V') IS NULL
BEGIN
    EXEC(N'
        CREATE OR ALTER VIEW rpt.v_esg_import_catalog
        AS
        SELECT
            CAST(NULL AS nvarchar(4))    AS pillar,
            CAST(NULL AS sysname)        AS [schema],
            CAST(NULL AS sysname)        AS view_name,
            CAST(NULL AS nvarchar(200))  AS category,
            CAST(NULL AS nvarchar(4000)) AS notes
        WHERE 1 = 0;
    ');
END;
GO

/* Year dimension (detect from unified ESG dataset if present; else union pillar datasets) */
DECLARE @sql nvarchar(max) = N'';
IF OBJECT_ID(N'rpt.v_esg_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_dim_year
        AS
        SELECT DISTINCT TRY_CONVERT(int, [year]) AS [year]
        FROM rpt.v_esg_pbix_dataset
        WHERE [year] IS NOT NULL;';
END
ELSE
BEGIN
    DECLARE @parts nvarchar(max) = N'';

    IF OBJECT_ID(N'rpt.v_gov_pbix_dataset', N'V') IS NOT NULL
        SET @parts = @parts + N' UNION ALL SELECT DISTINCT TRY_CONVERT(int,[year]) AS [year] FROM rpt.v_gov_pbix_dataset';
    IF OBJECT_ID(N'rpt.v_soc_pbix_dataset', N'V') IS NOT NULL
        SET @parts = @parts + N' UNION ALL SELECT DISTINCT TRY_CONVERT(int,[year]) FROM rpt.v_soc_pbix_dataset';
    IF OBJECT_ID(N'rpt.v_social_pbix_dataset', N'V') IS NOT NULL
        SET @parts = @parts + N' UNION ALL SELECT DISTINCT TRY_CONVERT(int,[year]) FROM rpt.v_social_pbix_dataset';
    IF OBJECT_ID(N'rpt.v_energy_pbix_dataset', N'V') IS NOT NULL
        SET @parts = @parts + N' UNION ALL SELECT DISTINCT TRY_CONVERT(int,[year]) FROM rpt.v_energy_pbix_dataset';
    IF OBJECT_ID(N'rpt.v_env_pbix_dataset', N'V') IS NOT NULL
        SET @parts = @parts + N' UNION ALL SELECT DISTINCT TRY_CONVERT(int,[year]) FROM rpt.v_env_pbix_dataset';

    IF LEN(@parts) > 0
        SET @parts = STUFF(@parts, 1, LEN(N' UNION ALL '), N'');

    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_dim_year
        AS
        SELECT DISTINCT [year]
        FROM (' + CASE WHEN LEN(@parts) > 0
                        THEN @parts
                        ELSE N'SELECT CAST(NULL AS int) AS [year] WHERE 1 = 0'
                   END + N') u
        WHERE [year] IS NOT NULL;';
END;
EXEC sys.sp_executesql @sql;
GO

/* PBIX import manifest (union pillar catalogs + unified entries) */
DECLARE @manifest nvarchar(max);

IF OBJECT_ID(N'rpt.v_esg_import_catalog', N'V') IS NOT NULL
BEGIN
    SET @manifest = N'
        CREATE OR ALTER VIEW rpt.v_pbix_import_manifest
        AS
        SELECT pillar, [schema], view_name, category, notes
        FROM rpt.v_esg_import_catalog
        UNION ALL
        SELECT NULL AS pillar, N''rpt'' AS [schema], N''v_dim_year'' AS view_name,
               N''helper'' AS category, N''Year dimension derived from ESG/Pillar datasets'' AS notes
        WHERE OBJECT_ID(N''rpt.v_dim_year'', N''V'') IS NOT NULL
        UNION ALL
        SELECT NULL, N''rpt'', N''v_esg_pbix_dataset'', N''esg-long'',
               N''Unified ESG long dataset (pillar/stream/metric/value_num)''
        WHERE OBJECT_ID(N''rpt.v_esg_pbix_dataset'', N''V'') IS NOT NULL;';
END
ELSE
BEGIN
    SET @manifest = N'
        CREATE OR ALTER VIEW rpt.v_pbix_import_manifest
        AS
        SELECT
            CAST(NULL AS nvarchar(4))    AS pillar,
            CAST(NULL AS sysname)        AS [schema],
            CAST(NULL AS sysname)        AS view_name,
            CAST(NULL AS nvarchar(200))  AS category,
            CAST(NULL AS nvarchar(4000)) AS notes
        WHERE 1 = 0;';
END;

EXEC sys.sp_executesql @manifest;
GO

/* Light smoketest proc (no heavy scans) */
CREATE OR ALTER PROCEDURE rpt.sp_pbix_smoketest
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @minYear int = (SELECT MIN([year]) FROM rpt.v_dim_year);
    DECLARE @maxYear int = (SELECT MAX([year]) FROM rpt.v_dim_year);
    DECLARE @esgRows  bigint = CASE WHEN OBJECT_ID(N'rpt.v_esg_pbix_dataset', N'V') IS NOT NULL
                                    THEN (SELECT COUNT_BIG(*) FROM rpt.v_esg_pbix_dataset)
                                    ELSE 0 END;

    SELECT 'dim_year_min' AS metric, @minYear AS val
    UNION ALL SELECT 'dim_year_max', @maxYear
    UNION ALL SELECT 'esg_rows_total', @esgRows;
END;
GO
