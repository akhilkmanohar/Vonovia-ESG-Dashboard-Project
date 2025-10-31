/* =========================================================================================
   Module 16d – Environment: Waste
   Integrate Waste into mart.sp_refresh_all and provide a tiny peek.
   - Safely (re)create mart.sp_refresh_all to include existing GHG + Energy + Water + Waste.
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

    -- ===== Waste (new) =====
    IF OBJECT_ID('core.sp_refresh_waste_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_waste_yearly;
END
GO

-- Smoke test run
EXEC mart.sp_refresh_all;

-- Peek (for run summary)
SELECT TOP (10) year, waste_t
FROM mart.v_waste_total_by_year
ORDER BY year DESC;
