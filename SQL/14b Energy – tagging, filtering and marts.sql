/* =========================================================================================
   Module 14b – Environment: Energy Consumption
   Tag rows → filtered facts → marts
   - Tags: rate/percentage, intensity (per m²), index, totals, notes
   - Filtered view keeps tangible energy amounts (MWh-like facts)
   - Marts: totals by year; by sheet & year
   Dependencies:
     - core.v_energy_yearly   (from Module 14)
     - mart schema exists (from GHG module)
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Tagging view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_yearly_tagged','V') IS NOT NULL
    DROP VIEW core.v_energy_yearly_tagged;
GO
CREATE VIEW core.v_energy_yearly_tagged
AS
WITH src AS (
    SELECT
        e.sheet_name,
        e.category,
        e.subcategory,
        e.year,
        e.row_label,
        e.value,
        e.derived_unit,
        UPPER(COALESCE(e.row_label, N'')) AS lbl_u
    FROM core.v_energy_yearly e
)
SELECT
    s.*,
    -- Presence of percent char or “share/Anteil/quote/rate”
    CASE WHEN CHARINDEX('%', s.lbl_u) > 0
           OR s.lbl_u LIKE '%SHARE%' OR s.lbl_u LIKE N'%ANTEIL%'
           OR s.lbl_u LIKE '%RATE%'  OR s.lbl_u LIKE N'%QUOTE%'
         THEN 1 ELSE 0 END                                         AS is_rate_like,

    -- Intensity / normalised metrics (per m2, kWh/m², etc.)
    CASE WHEN s.lbl_u LIKE '%/M2%' OR s.lbl_u LIKE '% PER M2%' OR s.lbl_u LIKE N'%M²%'
           OR s.lbl_u LIKE '%KWH/M2%' OR s.lbl_u LIKE N'%KWH/M²%' OR s.lbl_u LIKE '%INTENS%'
         THEN 1 ELSE 0 END                                         AS is_intensity_like,

    -- Index-style metrics
    CASE WHEN s.lbl_u LIKE '%INDEX%' THEN 1 ELSE 0 END             AS is_index_like,

    -- Explicit totals (kept but flagged)
    CASE WHEN s.lbl_u LIKE '%TOTAL%' OR s.lbl_u LIKE N'%SUMME%' THEN 1 ELSE 0 END AS is_total_like,

    -- Notes / comments rows
    CASE WHEN s.lbl_u LIKE '%NOTE%' OR s.lbl_u LIKE '%COMMENT%' OR s.lbl_u LIKE N'%KOMMENTAR%'
         THEN 1 ELSE 0 END                                         AS is_note_like
FROM src s;
GO

---------------------------------------------------------------------------------------------
-- 2) Filtered facts (exclude rates, intensities, obvious non-amounts)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_yearly_filtered','V') IS NOT NULL
    DROP VIEW core.v_energy_yearly_filtered;
GO
CREATE VIEW core.v_energy_yearly_filtered
AS
SELECT
    t.sheet_name,
    t.category,
    t.subcategory,
    t.year,
    t.row_label,
    -- Keep only sensible positive/zero values; drop absurd magnitudes as a safety valve
    CASE WHEN t.value BETWEEN 0 AND 1000000000 THEN t.value END AS value,
    t.derived_unit
FROM core.v_energy_yearly_tagged t
WHERE t.derived_unit = N'MWh'
  AND t.is_rate_like = 0
  AND t.is_intensity_like = 0
  AND t.is_index_like = 0;
GO

---------------------------------------------------------------------------------------------
-- 3) Energy marts
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_energy_total_by_year','V') IS NOT NULL
    DROP VIEW mart.v_energy_total_by_year;
GO
CREATE VIEW mart.v_energy_total_by_year
AS
SELECT
    f.year,
    SUM(f.value) AS energy_mwh
FROM core.v_energy_yearly_filtered f
GROUP BY f.year;
GO

IF OBJECT_ID('mart.v_energy_by_sheet_year','V') IS NOT NULL
    DROP VIEW mart.v_energy_by_sheet_year;
GO
CREATE VIEW mart.v_energy_by_sheet_year
AS
SELECT
    f.sheet_name,
    f.year,
    SUM(f.value) AS energy_mwh
FROM core.v_energy_yearly_filtered f
GROUP BY f.sheet_name, f.year;
GO

-- Tiny peek to support run summary
SELECT TOP (25) year, energy_mwh
FROM mart.v_energy_total_by_year
ORDER BY year;
