/* =========================================================================================
   Module 22 – Environment: integrate reporting refresh
   - Update mart.sp_refresh_all to also call rpt.sp_refresh_env_reporting
   - Smoke-run the orchestrator and peek recent fact rows
   ========================================================================================= */

SET NOCOUNT ON;

-- Ensure orchestrator exists
IF OBJECT_ID('mart.sp_refresh_all','P') IS NULL
    EXEC ('CREATE PROC mart.sp_refresh_all AS BEGIN SET NOCOUNT ON; END');
GO

ALTER PROC mart.sp_refresh_all
AS
BEGIN
    SET NOCOUNT ON;

    -- ===== GHG =====
    IF OBJECT_ID('core.sp_refresh_ghg_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_ghg_yearly;

    IF OBJECT_ID('core.sp_refresh_ghg_yearly_country','P') IS NOT NULL
        EXEC core.sp_refresh_ghg_yearly_country;

    -- ===== Energy =====
    IF OBJECT_ID('core.sp_refresh_energy_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_energy_yearly;

    -- ===== Water =====
    IF OBJECT_ID('core.sp_refresh_water_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_water_yearly;

    -- ===== Waste =====
    IF OBJECT_ID('core.sp_refresh_waste_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_waste_yearly;

    -- ===== Reporting (new) =====
    IF OBJECT_ID('rpt.sp_refresh_env_reporting','P') IS NOT NULL
        EXEC rpt.sp_refresh_env_reporting;
END
GO

-- Smoke test run
EXEC mart.sp_refresh_all;

-- Peek for run summary
SELECT TOP (12)
    metric_code, year, value
FROM rpt.fact_env_totals
ORDER BY metric_code, year DESC;
