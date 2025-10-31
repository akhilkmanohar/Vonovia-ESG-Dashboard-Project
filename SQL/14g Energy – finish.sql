/* =========================================================================================
   Module 14g – Environment: Energy Consumption (Finish)
   - Exclude totals from final facts
   - Point marts at materialized final table
   - Add YoY and 3-year trend marts
   - Re-run refresh proc; show a tiny peek
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Exclude totals in the FINAL view
--    (keeps rates/intensity/index excluded as before, now also remove is_total_like=1)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_yearly_final','V') IS NOT NULL
    DROP VIEW core.v_energy_yearly_final;
GO
CREATE VIEW core.v_energy_yearly_final
AS
WITH src AS (
    SELECT
        t.sheet_name,
        t.category,
        t.subcategory,
        t.year,
        t.row_label,
        t.value,
        t.derived_unit,
        t.is_rate_like,
        t.is_intensity_like,
        t.is_index_like,
        t.is_total_like,
        UPPER(COALESCE(t.row_label,N'')) AS lbl_u
    FROM core.v_energy_yearly_tagged t
),
filtered AS (
    SELECT *
    FROM src
    WHERE derived_unit = N'MWh'
      AND is_rate_like      = 0
      AND is_intensity_like = 0
      AND is_index_like     = 0
      AND is_total_like     = 0     -- NEW: drop totals/summe rows
      AND value BETWEEN 0 AND 100000000
)
SELECT
    f.sheet_name,
    MIN(f.category)    AS category,
    MIN(f.subcategory) AS subcategory,
    f.year,
    f.row_label,
    SUM(f.value)       AS value,
    CAST(N'MWh' AS nvarchar(50)) AS derived_unit
FROM filtered f
GROUP BY
    f.sheet_name, f.year, f.row_label;
GO

---------------------------------------------------------------------------------------------
-- 2) Refresh materialized table from FINAL facts
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_energy_yearly','P') IS NULL
    EXEC ('CREATE PROC core.sp_refresh_energy_yearly AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC core.sp_refresh_energy_yearly
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ch TABLE(action nvarchar(10));

    ;WITH src AS (SELECT * FROM core.v_energy_yearly_final)
    MERGE core.energy_yearly AS tgt
    USING src
       ON tgt.sheet_name = src.sheet_name
      AND tgt.year       = src.year
      AND ISNULL(tgt.row_label,'') = ISNULL(src.row_label,'')
    WHEN MATCHED AND (
           ISNULL(tgt.value, 0)        <> ISNULL(src.value, 0)
        OR ISNULL(tgt.derived_unit,'') <> ISNULL(src.derived_unit,'')
        OR ISNULL(tgt.category,'')     <> ISNULL(src.category,'')
        OR ISNULL(tgt.subcategory,'')  <> ISNULL(src.subcategory,'')
    )
    THEN UPDATE SET
        tgt.value        = src.value,
        tgt.derived_unit = src.derived_unit,
        tgt.category     = src.category,
        tgt.subcategory  = src.subcategory,
        tgt.load_dts     = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (sheet_name, category, subcategory, year, row_label, value, derived_unit)
         VALUES (src.sheet_name, src.category, src.subcategory, src.year, src.row_label, src.value, src.derived_unit)
    WHEN NOT MATCHED BY SOURCE
    THEN DELETE
    OUTPUT $action INTO @ch;

    SELECT
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END),0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END),0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END),0)
    FROM @ch;
END
GO

EXEC core.sp_refresh_energy_yearly;

---------------------------------------------------------------------------------------------
-- 3) Re-point marts at the materialized FINAL table + add YoY / 3-year rolling trend
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_energy_total_by_year','V') IS NOT NULL
    DROP VIEW mart.v_energy_total_by_year;
GO
CREATE VIEW mart.v_energy_total_by_year
AS
SELECT
    e.year,
    SUM(e.value) AS energy_mwh
FROM core.energy_yearly e
GROUP BY e.year;
GO

IF OBJECT_ID('mart.v_energy_by_sheet_year','V') IS NOT NULL
    DROP VIEW mart.v_energy_by_sheet_year;
GO
CREATE VIEW mart.v_energy_by_sheet_year
AS
SELECT
    e.sheet_name,
    e.year,
    SUM(e.value) AS energy_mwh
FROM core.energy_yearly e
GROUP BY e.sheet_name, e.year;
GO

IF OBJECT_ID('mart.v_energy_total_yoy','V') IS NOT NULL
    DROP VIEW mart.v_energy_total_yoy;
GO
CREATE VIEW mart.v_energy_total_yoy
AS
WITH base AS (
    SELECT year, energy_mwh,
           LAG(energy_mwh) OVER (ORDER BY year) AS prev_mwh
    FROM mart.v_energy_total_by_year
)
SELECT
    year,
    energy_mwh,
    prev_mwh,
    CASE WHEN prev_mwh IS NULL OR prev_mwh=0 THEN NULL
         ELSE (energy_mwh - prev_mwh) END AS abs_change_mwh,
    CASE WHEN prev_mwh IS NULL OR prev_mwh=0 THEN NULL
         ELSE (energy_mwh - prev_mwh) / prev_mwh END AS yoy_pct
FROM base;
GO

IF OBJECT_ID('mart.v_energy_total_trend_3yr','V') IS NOT NULL
    DROP VIEW mart.v_energy_total_trend_3yr;
GO
CREATE VIEW mart.v_energy_total_trend_3yr
AS
SELECT
    year,
    energy_mwh,
    AVG(energy_mwh) OVER (ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mwh_3yr_avg
FROM mart.v_energy_total_by_year;
GO

-- Peek
SELECT TOP (12) *
FROM mart.v_energy_total_yoy
ORDER BY year DESC;
