SET NOCOUNT ON;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* ESG unified PBIX dataset */
DECLARE @segments nvarchar(max) = N'';
DECLARE @sql nvarchar(max);

IF OBJECT_ID(N'rpt.v_gov_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @segments = @segments + N' UNION ALL SELECT TRY_CONVERT(int,[year]) AS [year], N''G'' AS pillar, TRY_CAST(stream AS nvarchar(20)) AS stream, TRY_CAST(metric AS nvarchar(200)) AS metric, TRY_CONVERT(decimal(18,6), value_num) AS value_num FROM rpt.v_gov_pbix_dataset';
END;

IF OBJECT_ID(N'rpt.v_soc_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @segments = @segments + N' UNION ALL SELECT TRY_CONVERT(int,[year]), N''S'', TRY_CAST(stream AS nvarchar(20)), TRY_CAST(metric AS nvarchar(200)), TRY_CONVERT(decimal(18,6), value_num) FROM rpt.v_soc_pbix_dataset';
END;

IF OBJECT_ID(N'rpt.v_social_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @segments = @segments + N' UNION ALL SELECT TRY_CONVERT(int,[year]), N''S'', TRY_CAST(stream AS nvarchar(20)), TRY_CAST(metric AS nvarchar(200)), TRY_CONVERT(decimal(18,6), value_num) FROM rpt.v_social_pbix_dataset';
END;

IF OBJECT_ID(N'rpt.v_energy_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @segments = @segments + N' UNION ALL SELECT TRY_CONVERT(int,[year]), N''E'', TRY_CAST(stream AS nvarchar(20)), TRY_CAST(metric AS nvarchar(200)), TRY_CONVERT(decimal(18,6), value_num) FROM rpt.v_energy_pbix_dataset';
END;

IF OBJECT_ID(N'rpt.v_env_pbix_dataset', N'V') IS NOT NULL
BEGIN
    SET @segments = @segments + N' UNION ALL SELECT TRY_CONVERT(int,[year]), N''E'', TRY_CAST(stream AS nvarchar(20)), TRY_CAST(metric AS nvarchar(200)), TRY_CONVERT(decimal(18,6), value_num) FROM rpt.v_env_pbix_dataset';
END;

IF LEN(@segments) > 0
BEGIN
    SET @segments = STUFF(@segments, 1, LEN(N' UNION ALL '), N'');
    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_esg_pbix_dataset
        AS ' + @segments + N';';
END
ELSE
BEGIN
    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_esg_pbix_dataset
        AS
        SELECT
            CAST(NULL AS int)           AS [year],
            CAST(NULL AS nvarchar(1))   AS pillar,
            CAST(NULL AS nvarchar(20))  AS stream,
            CAST(NULL AS nvarchar(200)) AS metric,
            CAST(NULL AS decimal(18,6)) AS value_num
        WHERE 1=0;';
END;

EXEC sys.sp_executesql @sql;
GO

/* ESG unified import catalog (adds unified entry) */
DECLARE @catSegments nvarchar(max) = N'';
DECLARE @sql nvarchar(max);

IF OBJECT_ID(N'rpt.v_gov_import_catalog_plus', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog_plus', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog_plus', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog_plus', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog_plus', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''G'' AS pillar, [schema], view_name, category, notes FROM rpt.v_gov_import_catalog_plus';
END
ELSE IF OBJECT_ID(N'rpt.v_gov_import_catalog', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_gov_import_catalog', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''G'' AS pillar, [schema], view_name, category, notes FROM rpt.v_gov_import_catalog';
END;

IF OBJECT_ID(N'rpt.v_soc_import_catalog', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_soc_import_catalog', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_soc_import_catalog', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_soc_import_catalog', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_soc_import_catalog', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''S'', [schema], view_name, category, notes FROM rpt.v_soc_import_catalog';
END;

IF OBJECT_ID(N'rpt.v_social_import_catalog', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_social_import_catalog', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_social_import_catalog', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_social_import_catalog', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_social_import_catalog', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''S'', [schema], view_name, category, notes FROM rpt.v_social_import_catalog';
END;

IF OBJECT_ID(N'rpt.v_energy_import_catalog', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_energy_import_catalog', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_energy_import_catalog', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_energy_import_catalog', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_energy_import_catalog', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''E'', [schema], view_name, category, notes FROM rpt.v_energy_import_catalog';
END;

IF OBJECT_ID(N'rpt.v_env_import_catalog', N'V') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_env_import_catalog', N'schema') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_env_import_catalog', N'view_name') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_env_import_catalog', N'category') IS NOT NULL
   AND COL_LENGTH(N'rpt.v_env_import_catalog', N'notes') IS NOT NULL
BEGIN
    SET @catSegments = @catSegments + N' UNION ALL SELECT N''E'', [schema], view_name, category, notes FROM rpt.v_env_import_catalog';
END;

IF LEN(@catSegments) > 0
BEGIN
    SET @catSegments = STUFF(@catSegments, 1, LEN(N' UNION ALL '), N'');
    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_esg_import_catalog
        AS
        SELECT pillar, [schema], view_name, category, notes
        FROM (
            ' + @catSegments + N'
            UNION ALL
            SELECT N''ESG'' AS pillar, N''rpt'' AS [schema], N''v_esg_pbix_dataset'' AS view_name,
                   N''esg-long'' AS category, N''Unified ESG long-format dataset'' AS notes
        ) AS src;';
END
ELSE
BEGIN
    SET @sql = N'
        CREATE OR ALTER VIEW rpt.v_esg_import_catalog
        AS
        SELECT
            N''ESG'' AS pillar,
            N''rpt'' AS [schema],
            N''v_esg_pbix_dataset'' AS view_name,
            N''esg-long'' AS category,
            N''Unified ESG long-format dataset'' AS notes;';
END;

EXEC sys.sp_executesql @sql;
GO
