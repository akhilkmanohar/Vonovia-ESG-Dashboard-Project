/* =========================================================================================
   Module 14f – Environment: Energy Consumption
   Integrate into mart.sp_refresh_all and provide a tiny peek.
   - Safely (re)create mart.sp_refresh_all to include existing GHG refresh calls + Energy.
   - Keeps idempotent behavior; if procs don’t exist, they’re skipped.
   ========================================================================================= */

SET NOCOUNT ON;

IF OBJECT_ID('mart.sp_refresh_all','P') IS NULL
    EXEC ('CREATE PROC mart.sp_refresh_all AS BEGIN SET NOCOUNT ON; END');
GO

ALTER PROC mart.sp_refresh_all
AS
BEGIN
    SET NOCOUNT ON;

    -- ===== GHG (existing) =====
    IF OBJECT_ID('core.sp_refresh_ghg_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_ghg_yearly;

    IF OBJECT_ID('core.sp_refresh_ghg_yearly_country','P') IS NOT NULL
        EXEC core.sp_refresh_ghg_yearly_country;

    -- ===== Energy (new) =====
    IF OBJECT_ID('core.sp_refresh_energy_yearly','P') IS NOT NULL
        EXEC core.sp_refresh_energy_yearly;

    -- Future modules can be appended here…
END
GO

-- Run once as a smoke test
EXEC mart.sp_refresh_all;

-- Tiny peek (helps run summary)
SELECT TOP (10) year, energy_mwh
FROM mart.v_energy_total_by_year
ORDER BY year DESC;
