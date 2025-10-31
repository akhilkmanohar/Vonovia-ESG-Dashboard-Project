/* =========================================================================================
   Module 18 – Environment: Summary by Year (Energy MWh, Water m³, Waste t)
   - Builds consolidated marts from existing materialized facts/marts:
       * Energy: mart.v_energy_total_by_year (MWh)           [already in Module 14]
       * Water : core.water_yearly  -> totals (m³)           [Module 15]
       * Waste : core.waste_yearly  -> totals (t)            [Module 16]
   - Adds YoY deltas inline for each metric
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Ensure base total views exist for Water/Waste (Energy already has mart.v_energy_total_by_year)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_water_total_by_year','V') IS NULL
BEGIN
    EXEC('CREATE VIEW mart.v_water_total_by_year AS
          SELECT year, SUM(value) AS water_m3
          FROM core.water_yearly
          GROUP BY year;');
END;

IF OBJECT_ID('mart.v_waste_total_by_year','V') IS NULL
BEGIN
    EXEC('CREATE VIEW mart.v_waste_total_by_year AS
          SELECT year, SUM(value) AS waste_t
          FROM core.waste_yearly
          GROUP BY year;');
END;

---------------------------------------------------------------------------------------------
-- 2) Consolidated view: totals side-by-side
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_totals_by_year','V') IS NOT NULL
    DROP VIEW mart.v_env_totals_by_year;
GO
CREATE VIEW mart.v_env_totals_by_year
AS
WITH e AS (
    SELECT year, energy_mwh = SUM(energy_mwh)
    FROM mart.v_energy_total_by_year
    GROUP BY year
),
w AS (
    SELECT year, water_m3
    FROM mart.v_water_total_by_year
),
x AS (
    SELECT year, waste_t
    FROM mart.v_waste_total_by_year
),
all_years AS (
    SELECT year FROM e
    UNION
    SELECT year FROM w
    UNION
    SELECT year FROM x
)
SELECT
    a.year,
    e.energy_mwh,
    w.water_m3,
    x.waste_t
FROM all_years a
LEFT JOIN e ON e.year = a.year
LEFT JOIN w ON w.year = a.year
LEFT JOIN x ON x.year = a.year;
GO

---------------------------------------------------------------------------------------------
-- 3) YoY overlay per metric (inline)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_totals_yoy','V') IS NOT NULL
    DROP VIEW mart.v_env_totals_yoy;
GO
CREATE VIEW mart.v_env_totals_yoy
AS
WITH base AS (
    SELECT * FROM mart.v_env_totals_by_year
)
SELECT
    year,
    energy_mwh,
    LAG(energy_mwh) OVER(ORDER BY year) AS prev_energy_mwh,
    CASE WHEN LAG(energy_mwh) OVER(ORDER BY year) IS NULL OR LAG(energy_mwh) OVER(ORDER BY year)=0
         THEN NULL ELSE (energy_mwh - LAG(energy_mwh) OVER(ORDER BY year)) / LAG(energy_mwh) OVER(ORDER BY year) END AS energy_yoy,

    water_m3,
    LAG(water_m3) OVER(ORDER BY year) AS prev_water_m3,
    CASE WHEN LAG(water_m3) OVER(ORDER BY year) IS NULL OR LAG(water_m3) OVER(ORDER BY year)=0
         THEN NULL ELSE (water_m3 - LAG(water_m3) OVER(ORDER BY year)) / LAG(water_m3) OVER(ORDER BY year) END AS water_yoy,

    waste_t,
    LAG(waste_t) OVER(ORDER BY year) AS prev_waste_t,
    CASE WHEN LAG(waste_t) OVER(ORDER BY year) IS NULL OR LAG(waste_t) OVER(ORDER BY year)=0
         THEN NULL ELSE (waste_t - LAG(waste_t) OVER(ORDER BY year)) / LAG(waste_t) OVER(ORDER BY year) END AS waste_yoy
FROM base;
GO

-- Peek for run summary
SELECT TOP (10) * FROM mart.v_env_totals_yoy ORDER BY year DESC;
