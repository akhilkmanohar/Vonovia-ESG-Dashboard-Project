IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'rpt')
BEGIN
    EXEC(N'CREATE SCHEMA rpt AUTHORIZATION dbo');
END;
GO
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

/* Synonym refresh (explicit list for stability) */
IF OBJECT_ID(N'rpt.pbix_esg_dataset', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_esg_dataset;
IF OBJECT_ID(N'rpt.v_esg_pbix_dataset', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_esg_dataset FOR rpt.v_esg_pbix_dataset;

IF OBJECT_ID(N'rpt.pbix_dim_year', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_dim_year;
IF OBJECT_ID(N'rpt.v_dim_year', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_dim_year FOR rpt.v_dim_year;

IF OBJECT_ID(N'rpt.pbix_dim_pillar', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_dim_pillar;
IF OBJECT_ID(N'rpt.v_dim_pillar', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_dim_pillar FOR rpt.v_dim_pillar;

IF OBJECT_ID(N'rpt.pbix_dim_stream', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_dim_stream;
IF OBJECT_ID(N'rpt.v_dim_stream', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_dim_stream FOR rpt.v_dim_stream;

IF OBJECT_ID(N'rpt.pbix_cards_latest_and_last5', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_cards_latest_and_last5;
IF OBJECT_ID(N'rpt.v_esg_cards_latest_and_last5', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_cards_latest_and_last5 FOR rpt.v_esg_cards_latest_and_last5;

IF OBJECT_ID(N'rpt.pbix_manifest', N'SN') IS NOT NULL DROP SYNONYM rpt.pbix_manifest;
IF OBJECT_ID(N'rpt.v_pbix_import_manifest', N'V') IS NOT NULL
    CREATE SYNONYM rpt.pbix_manifest FOR rpt.v_pbix_import_manifest;
GO

/* Ensure bi_ro role */
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

/* PBIX reader grant helper */
CREATE OR ALTER PROCEDURE rpt.sp_pbix_grant_user
    @principal sysname
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @principal)
    BEGIN
        DECLARE @principal_br nvarchar(512) =
            N'[' + REPLACE(@principal, N']', N']]') + N']';
        BEGIN TRY
            EXEC(N'CREATE USER ' + @principal_br + N' FOR LOGIN ' + @principal_br + N';');
        END TRY
        BEGIN CATCH
            BEGIN TRY
                EXEC(N'CREATE USER ' + @principal_br + N' WITHOUT LOGIN;');
            END TRY
            BEGIN CATCH
            END CATCH
        END CATCH
    END

    IF NOT EXISTS (
        SELECT 1
        FROM sys.database_role_members rm
        JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id
        JOIN sys.database_principals u ON u.principal_id = rm.member_principal_id
        WHERE r.name = N'bi_ro'
          AND u.name = @principal
    )
    BEGIN
        DECLARE @role_cmd nvarchar(512) =
            N'ALTER ROLE [bi_ro] ADD MEMBER [' + REPLACE(@principal, N']', N']]') + N'];';
        EXEC(@role_cmd);
    END

    SELECT r.name AS role_name, u.name AS member_name
    FROM sys.database_role_members rm
    JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id
    JOIN sys.database_principals u ON u.principal_id = rm.member_principal_id
    WHERE r.name = N'bi_ro' AND u.name = @principal;

    SELECT s.name AS schema_name,
           sy.name AS synonym_name,
           sy.base_object_name
    FROM sys.synonyms sy
    JOIN sys.schemas s ON s.schema_id = sy.schema_id
    WHERE s.name = N'rpt' AND sy.name LIKE N'pbix_%';
END;
GO
