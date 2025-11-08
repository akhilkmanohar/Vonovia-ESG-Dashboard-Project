SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart');

IF OBJECT_ID('mart.sp_refresh_reporting_all','P') IS NULL
    EXEC('CREATE PROCEDURE mart.sp_refresh_reporting_all AS SELECT 1;');
GO
ALTER PROCEDURE mart.sp_refresh_reporting_all
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @messages TABLE(
        step nvarchar(200) NOT NULL,
        ok   bit           NOT NULL,
        err  nvarchar(max) NULL,
        ran_at datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );

    -- Run existing master refresh first (environment, GHG, energy, water, waste, env reporting, etc.)
    BEGIN TRY
        EXEC mart.sp_refresh_all;
        INSERT INTO @messages(step, ok) VALUES (N'mart.sp_refresh_all', 1);
    END TRY
    BEGIN CATCH
        INSERT INTO @messages(step, ok, err)
        VALUES (N'mart.sp_refresh_all', 0, CONCAT(ERROR_NUMBER(), N' | ', ERROR_MESSAGE()));
    END CATCH;

    -- Then run Social reporting refresh (from module 44d)
    BEGIN TRY
        EXEC rpt.sp_refresh_social_reporting;
        INSERT INTO @messages(step, ok) VALUES (N'rpt.sp_refresh_social_reporting', 1);
    END TRY
    BEGIN CATCH
        INSERT INTO @messages(step, ok, err)
        VALUES (N'rpt.sp_refresh_social_reporting', 0, CONCAT(ERROR_NUMBER(), N' | ', ERROR_MESSAGE()));
    END CATCH;

    -- Quick diag snapshot (row counts) so the caller can confirm coverage
    ;WITH counts AS (
        SELECT 'rpt.v_social_workforce_wide' AS view_name, COUNT_BIG(1) AS rows FROM rpt.v_social_workforce_wide
        UNION ALL SELECT 'rpt.v_social_hs_counts_wide', COUNT_BIG(1) FROM rpt.v_social_hs_counts_wide
        UNION ALL SELECT 'rpt.v_social_hs_rates_wide',  COUNT_BIG(1) FROM rpt.v_social_hs_rates_wide
        UNION ALL SELECT 'rpt.v_social_training_wide',  COUNT_BIG(1) FROM rpt.v_social_training_wide
        UNION ALL SELECT 'rpt.v_social_cards_latest5',  COUNT_BIG(1) FROM rpt.v_social_cards_latest5
    )
    SELECT 'messages' AS section, * FROM @messages
    UNION ALL
    SELECT 'counts' AS section, view_name AS step, CAST(1 AS bit) AS ok, CAST(NULL AS nvarchar(max)) AS err, SYSUTCDATETIME()
    FROM counts;
END
GO
