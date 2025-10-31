/* =========================================================================================
   Module 19 – Environment unified “long” mart
   Stacks Energy / Water / Waste into one tidy table for viz:
     - mart.v_env_unified_yearly     (year, metric_group, metric_code, unit, value)
     - mart.v_env_unified_yoy        (+ YoY %, abs change per metric)
     - mart.v_env_unified_trend_3yr  (+ 3yr rolling average per metric)
   Inputs:
     - mart.v_energy_total_by_year   (energy_mwh)
     - mart.v_water_total_by_year    (water_m3)
     - mart.v_waste_total_by_year    (waste_t)
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Unified yearly (long)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_unified_yearly','V') IS NOT NULL
    DROP VIEW mart.v_env_unified_yearly;
GO
CREATE VIEW mart.v_env_unified_yearly
AS
SELECT
    e.year,
    CAST('Environment' AS nvarchar(100)) AS metric_group,
    CAST('energy_mwh'  AS nvarchar(100)) AS metric_code,
    CAST('Energy (MWh)' AS nvarchar(200)) AS metric_label,
    CAST('MWh' AS nvarchar(20)) AS unit,
    CAST(e.energy_mwh AS decimal(38,6)) AS value
FROM mart.v_energy_total_by_year e
UNION ALL
SELECT
    w.year,
    'Environment',
    'water_m3',
    'Water (m³)',
    N'm³',
    CAST(w.water_m3 AS decimal(38,6))
FROM mart.v_water_total_by_year w
UNION ALL
SELECT
    x.year,
    'Environment',
    'waste_t',
    'Waste (t)',
    't',
    CAST(x.waste_t AS decimal(38,6))
FROM mart.v_waste_total_by_year x;
GO

---------------------------------------------------------------------------------------------
-- 2) YoY overlay (partition per metric_code)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_unified_yoy','V') IS NOT NULL
    DROP VIEW mart.v_env_unified_yoy;
GO
CREATE VIEW mart.v_env_unified_yoy
AS
WITH base AS (
    SELECT * FROM mart.v_env_unified_yearly
)
SELECT
    year,
    metric_group,
    metric_code,
    metric_label,
    unit,
    value,
    LAG(value) OVER (PARTITION BY metric_code ORDER BY year) AS prev_value,
    CASE WHEN LAG(value) OVER (PARTITION BY metric_code ORDER BY year) IS NULL
         THEN NULL ELSE value - LAG(value) OVER (PARTITION BY metric_code ORDER BY year) END AS abs_change,
    CASE WHEN LAG(value) OVER (PARTITION BY metric_code ORDER BY year) IN (NULL, 0)
         THEN NULL ELSE (value - LAG(value) OVER (PARTITION BY metric_code ORDER BY year))
                        / NULLIF(LAG(value) OVER (PARTITION BY metric_code ORDER BY year), 0) END AS yoy_pct
FROM base;
GO

---------------------------------------------------------------------------------------------
-- 3) 3-year rolling average (per metric_code)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_unified_trend_3yr','V') IS NOT NULL
    DROP VIEW mart.v_env_unified_trend_3yr;
GO
CREATE VIEW mart.v_env_unified_trend_3yr
AS
WITH base AS (
    SELECT * FROM mart.v_env_unified_yearly
)
SELECT
    year,
    metric_group,
    metric_code,
    metric_label,
    unit,
    value,
    AVG(value) OVER (
        PARTITION BY metric_code
        ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS value_3yr_avg
FROM base;
GO

-- Peek (recent rows per metric)
SELECT TOP (15) * FROM mart.v_env_unified_yoy       ORDER BY metric_code, year DESC;
SELECT TOP (15) * FROM mart.v_env_unified_trend_3yr ORDER BY metric_code, year DESC;
