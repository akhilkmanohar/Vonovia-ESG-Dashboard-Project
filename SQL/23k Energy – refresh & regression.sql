IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER PROCEDURE mart.sp_refresh_energy_mix
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @views TABLE(schema_name sysname, view_name sysname);
    INSERT INTO @views(schema_name, view_name) VALUES
        (N'core', N'v_energy_mix_tagged'),
        (N'core', N'v_energy_mix_filtered'),
        (N'core', N'v_energy_mix_final'),
        (N'mart', N'v_energy_mix_by_year'),
        (N'mart', N'v_energy_renewable_share'),
        (N'mart', N'v_energy_mix_other_ratio'),
        (N'mart', N'v_energy_mix_guardrails');

    DECLARE @refreshed int = 0;

    DECLARE @s sysname, @v sysname, @fq nvarchar(400);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT schema_name, view_name FROM @views;
    OPEN cur;
    FETCH NEXT FROM cur INTO @s, @v;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF OBJECT_ID(QUOTENAME(@s)+N'.'+QUOTENAME(@v), N'V') IS NOT NULL
        BEGIN
            SET @fq = QUOTENAME(@s)+N'.'+QUOTENAME(@v);
            EXEC sys.sp_refreshview @viewname = @fq;
            SET @refreshed += 1;
        END
        FETCH NEXT FROM cur INTO @s, @v;
    END
    CLOSE cur; DEALLOCATE cur;

    SELECT 'refresh_summary' AS section, @refreshed AS refreshed_views;
    PRINT CONCAT('sp_refresh_energy_mix refreshed views: ', @refreshed);
END;
GO
EXEC mart.sp_refresh_energy_mix;
