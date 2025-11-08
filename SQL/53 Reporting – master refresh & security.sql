-- Ensure schema rpt exists (safe even if already present)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Security: read-only role for PBIX */
IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'bi_ro'
      AND type = 'R'
)
BEGIN
    EXEC(N'CREATE ROLE [bi_ro] AUTHORIZATION [dbo]');
END;
GO

-- Grant SELECT on schema::rpt if the role does not already have it
IF NOT EXISTS (
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
BEGIN
    GRANT SELECT ON SCHEMA::rpt TO [bi_ro];
END;
GO

/* Master refresh wrapper (guarded fan-out; dry-run by default) */
CREATE OR ALTER PROCEDURE rpt.sp_refresh_all_reporting
    @areas   nvarchar(100) = NULL,  -- e.g. 'E,S,G' (NULL => all)
    @dry_run bit           = 1      -- default: list plan only
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @filter nvarchar(100) = UPPER(ISNULL(@areas, ''));
    DECLARE @includeE bit = CASE WHEN @areas IS NULL OR CHARINDEX('E', @filter) > 0 THEN 1 ELSE 0 END;
    DECLARE @includeS bit = CASE WHEN @areas IS NULL OR CHARINDEX('S', @filter) > 0 THEN 1 ELSE 0 END;
    DECLARE @includeG bit = CASE WHEN @areas IS NULL OR CHARINDEX('G', @filter) > 0 THEN 1 ELSE 0 END;

    DECLARE @plan TABLE (
        ordinal int IDENTITY(1,1) PRIMARY KEY,
        proc_name sysname NOT NULL,
        will_execute bit NOT NULL,
        status nvarchar(200) NULL,
        duration_ms int NULL
    );

    IF OBJECT_ID(N'rpt.sp_refresh_energy_reporting', 'P') IS NOT NULL AND @includeE = 1
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'rpt.sp_refresh_energy_reporting', 1, N'pending');
    END;

    IF OBJECT_ID(N'rpt.sp_refresh_social_reporting', 'P') IS NOT NULL AND @includeS = 1
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'rpt.sp_refresh_social_reporting', 1, N'pending');
    END;
    ELSE IF OBJECT_ID(N'mart.sp_refresh_social_reporting', 'P') IS NOT NULL AND @includeS = 1
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'mart.sp_refresh_social_reporting', 1, N'pending');
    END;

    IF OBJECT_ID(N'rpt.sp_refresh_governance_reporting', 'P') IS NOT NULL AND @includeG = 1
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'rpt.sp_refresh_governance_reporting', 1, N'pending');
    END;

    IF OBJECT_ID(N'mart.sp_refresh_reporting_all', 'P') IS NOT NULL AND @areas IS NULL
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'mart.sp_refresh_reporting_all', 1, N'pending');
    END;

    IF NOT EXISTS (SELECT 1 FROM @plan)
    BEGIN
        INSERT INTO @plan (proc_name, will_execute, status)
        VALUES (N'no matching refreshers', 0, N'no operations planned');
    END;

    IF @dry_run = 1
    BEGIN
        UPDATE @plan
        SET status = CASE
                         WHEN will_execute = 1 THEN N'dry_run (would execute)'
                         ELSE status
                     END;
    END
    ELSE
    BEGIN
        DECLARE @ordinal int;
        DECLARE @proc sysname;
        DECLARE @sql nvarchar(512);
        DECLARE @db sysname;
        DECLARE @schema sysname;
        DECLARE @name sysname;
        DECLARE @started datetime2(3);

        DECLARE run CURSOR LOCAL FAST_FORWARD
        FOR
            SELECT ordinal, proc_name
            FROM @plan
            WHERE will_execute = 1
            ORDER BY ordinal;

        OPEN run;
        FETCH NEXT FROM run INTO @ordinal, @proc;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @db = PARSENAME(@proc, 3);
            SET @schema = PARSENAME(@proc, 2);
            SET @name = PARSENAME(@proc, 1);

            IF @name IS NULL
            BEGIN
                UPDATE @plan
                SET status = N'error: invalid procedure name'
                WHERE ordinal = @ordinal;
            END
            ELSE
            BEGIN
                SET @sql =
                    N'EXEC ' +
                    ISNULL(QUOTENAME(@db) + N'.', N'') +
                    QUOTENAME(COALESCE(@schema, N'dbo')) + N'.' +
                    QUOTENAME(@name);

                BEGIN TRY
                    SET @started = SYSDATETIME();
                    EXEC (@sql);
                    UPDATE @plan
                    SET status = N'executed',
                        duration_ms = DATEDIFF(ms, @started, SYSDATETIME())
                    WHERE ordinal = @ordinal;
                END TRY
                BEGIN CATCH
                    UPDATE @plan
                    SET status = N'error: ' + ERROR_MESSAGE()
                    WHERE ordinal = @ordinal;
                END CATCH;
            END;

            FETCH NEXT FROM run INTO @ordinal, @proc;
        END;

        CLOSE run;
        DEALLOCATE run;
    END;

    SELECT ordinal, proc_name, will_execute, status, duration_ms
    FROM @plan
    ORDER BY ordinal;
END;
GO

/* Unified import catalog (pulls available pillar catalogs) */
CREATE OR ALTER PROCEDURE rpt.sp_import_catalog_all
    @pillar_filter nvarchar(50) = NULL  -- e.g. 'E', 'S', 'G' or combinations like 'E,S'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @filter nvarchar(50) = UPPER(ISNULL(@pillar_filter, ''));
    DECLARE @includeE bit = CASE WHEN @pillar_filter IS NULL OR CHARINDEX('E', @filter) > 0 THEN 1 ELSE 0 END;
    DECLARE @includeS bit = CASE WHEN @pillar_filter IS NULL OR CHARINDEX('S', @filter) > 0 THEN 1 ELSE 0 END;
    DECLARE @includeG bit = CASE WHEN @pillar_filter IS NULL OR CHARINDEX('G', @filter) > 0 THEN 1 ELSE 0 END;

    DECLARE @cat TABLE (
        pillar nvarchar(5),
        schema_name sysname,
        view_name sysname,
        category nvarchar(200),
        notes nvarchar(4000)
    );

    DECLARE @obj int;
    DECLARE @sql nvarchar(4000);

    IF @includeG = 1
    BEGIN
        SET @obj = OBJECT_ID(N'rpt.v_gov_import_catalog_plus', 'V');
        IF @obj IS NOT NULL
           AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
        BEGIN
            SET @sql = N'SELECT N''G'', [schema], view_name, category, notes FROM rpt.v_gov_import_catalog_plus;';
            INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
            EXEC sys.sp_executesql @sql;
        END
        ELSE
        BEGIN
            SET @obj = OBJECT_ID(N'rpt.v_gov_import_catalog', 'V');
            IF @obj IS NOT NULL
               AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
            BEGIN
                SET @sql = N'SELECT N''G'', [schema], view_name, category, notes FROM rpt.v_gov_import_catalog;';
                INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
                EXEC sys.sp_executesql @sql;
            END
        END
    END;

    IF @includeS = 1
    BEGIN
        SET @obj = OBJECT_ID(N'rpt.v_soc_import_catalog', 'V');
        IF @obj IS NOT NULL
           AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
        BEGIN
            SET @sql = N'SELECT N''S'', [schema], view_name, category, notes FROM rpt.v_soc_import_catalog;';
            INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
            EXEC sys.sp_executesql @sql;
        END
        ELSE
        BEGIN
            SET @obj = OBJECT_ID(N'rpt.v_social_import_catalog', 'V');
            IF @obj IS NOT NULL
               AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
            BEGIN
                SET @sql = N'SELECT N''S'', [schema], view_name, category, notes FROM rpt.v_social_import_catalog;';
                INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
                EXEC sys.sp_executesql @sql;
            END
        END
    END;

    IF @includeE = 1
    BEGIN
        SET @obj = OBJECT_ID(N'rpt.v_env_import_catalog', 'V');
        IF @obj IS NOT NULL
           AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
        BEGIN
            SET @sql = N'SELECT N''E'', [schema], view_name, category, notes FROM rpt.v_env_import_catalog;';
            INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
            EXEC sys.sp_executesql @sql;
        END
        ELSE
        BEGIN
            SET @obj = OBJECT_ID(N'rpt.v_energy_import_catalog', 'V');
            IF @obj IS NOT NULL
               AND (SELECT COUNT(*) FROM sys.columns WHERE object_id = @obj AND name IN (N'schema', N'view_name', N'category', N'notes')) = 4
            BEGIN
                SET @sql = N'SELECT N''E'', [schema], view_name, category, notes FROM rpt.v_energy_import_catalog;';
                INSERT INTO @cat (pillar, schema_name, view_name, category, notes)
                EXEC sys.sp_executesql @sql;
            END
        END
    END;

    SELECT pillar,
           schema_name AS [schema],
           view_name,
           category,
           notes
    FROM @cat
    ORDER BY pillar, schema_name, view_name;
END;
GO
