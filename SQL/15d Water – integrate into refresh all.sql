/* =========================================================================================
   Module 15d – Environment: Water Consumption
   Integrate Water into mart.sp_refresh_all and provide a tiny peek.
   - Safely (re)create mart.sp_refresh_all to include existing GHG + Energy + Water refresh calls.
   - Idempotent: if a proc doesn’t exist, skip it.
   ========================================================================================= */

SET NOCOUNT ON;

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
END
GO

-- Smoke test run
EXEC mart.sp_refresh_all;

-- Peek (for run summary)
SELECT TOP (10) year, water_m3
FROM mart.v_water_total_by_year
ORDER BY year DESC;
