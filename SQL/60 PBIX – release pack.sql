IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* DB extended properties for PBIX release */
DECLARE @pbix_ts nvarchar(30) = CONVERT(nvarchar(30), SYSDATETIME(), 126);
BEGIN TRY EXEC sys.sp_updateextendedproperty @name=N'pbix_release_ready', @value=1; END TRY
BEGIN CATCH BEGIN TRY EXEC sys.sp_addextendedproperty @name=N'pbix_release_ready', @value=1; END TRY BEGIN CATCH END CATCH END CATCH;

BEGIN TRY EXEC sys.sp_updateextendedproperty @name=N'pbix_release_timestamp', @value=@pbix_ts; END TRY
BEGIN CATCH BEGIN TRY EXEC sys.sp_addextendedproperty @name=N'pbix_release_timestamp', @value=@pbix_ts; END TRY BEGIN CATCH END CATCH END CATCH;
GO

/* PBIX synonyms manifest */
CREATE OR ALTER VIEW rpt.v_pbix_import_manifest_synonyms
AS
SELECT
    s.name        AS [schema],
    sy.name       AS object_name,
    N'synonym'    AS object_type,
    CASE sy.name
        WHEN N'pbix_esg_dataset'            THEN N'ESG unified long dataset'
        WHEN N'pbix_dim_year'               THEN N'Year dimension'
        WHEN N'pbix_dim_pillar'             THEN N'Pillar dimension'
        WHEN N'pbix_dim_stream'             THEN N'Stream dimension'
        WHEN N'pbix_cards_latest_and_last5' THEN N'Latest & last-5 cards (ESG)'
        WHEN N'pbix_manifest'               THEN N'PBIX manifest (views & dims)'
        ELSE N'PBIX synonym'
    END AS notes
FROM sys.synonyms sy
JOIN sys.schemas s ON s.schema_id = sy.schema_id
WHERE s.name = N'rpt'
  AND sy.name LIKE N'pbix_%';
GO

/* Consolidated PBIX manifest (synonyms + view catalog) */
CREATE OR ALTER VIEW rpt.v_pbix_manifest_all
AS
    SELECT NULL AS pillar, [schema], object_name AS view_or_synonym, object_type, notes
    FROM rpt.v_pbix_import_manifest_synonyms
    UNION ALL
    SELECT pillar, [schema], view_name, N'view' AS object_type, notes
    FROM rpt.v_pbix_import_manifest;
GO

/* Latest-year presence view */
CREATE OR ALTER VIEW rpt.v_esg_latest_presence
AS
WITH latest AS (SELECT MAX([year]) AS latest_year FROM rpt.v_esg_pbix_dataset),
grid AS (
    SELECT p.pillar, s.stream
    FROM rpt.v_dim_pillar p
    CROSS JOIN rpt.v_dim_stream s
)
SELECT
    g.pillar,
    g.stream,
    l.latest_year,
    CASE WHEN EXISTS (
        SELECT 1
        FROM rpt.v_esg_pbix_dataset d
        WHERE d.pillar = g.pillar
          AND d.stream = g.stream
          AND d.[year] = l.latest_year
    )
    THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS is_present_latest
FROM grid g
CROSS JOIN latest l;
GO

/* Fan-out grant helper */
CREATE OR ALTER PROCEDURE rpt.sp_pbix_grant_many
    @principals nvarchar(max),
    @dry_run    bit = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @parsed nvarchar(max) = REPLACE(REPLACE(@principals, ';', ','), ' ', '');

    IF OBJECT_ID('tempdb..#pbix_principals') IS NOT NULL DROP TABLE #pbix_principals;
    SELECT DISTINCT TRIM(value) AS principal
    INTO #pbix_principals
    FROM STRING_SPLIT(@parsed, ',')
    WHERE TRIM(value) <> '';

    DECLARE @p sysname;
    DECLARE grant_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT principal FROM #pbix_principals;
    OPEN grant_cursor;
    FETCH NEXT FROM grant_cursor INTO @p;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @dry_run = 0
            EXEC rpt.sp_pbix_grant_user @principal = @p;
        FETCH NEXT FROM grant_cursor INTO @p;
    END
    CLOSE grant_cursor;
    DEALLOCATE grant_cursor;

    SELECT r.name AS role_name, u.name AS member_name
    FROM sys.database_role_members rm
    JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id AND r.name = N'bi_ro'
    JOIN sys.database_principals u ON u.principal_id = rm.member_principal_id
    WHERE u.name IN (SELECT principal FROM #pbix_principals)
    ORDER BY u.name;

    SELECT sy.name AS synonym_name, sy.base_object_name
    FROM sys.synonyms sy
    JOIN sys.schemas s ON s.schema_id = sy.schema_id
    WHERE s.name = N'rpt' AND sy.name LIKE N'pbix_%'
    ORDER BY sy.name;
END;
GO
