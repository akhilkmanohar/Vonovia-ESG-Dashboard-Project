/* =========================================================================================
   Module 17 – Environment: Water + Waste YoY & 3-year Trend
   - Creates YoY and 3-year rolling-average marts for Water and Waste
   - Reads from materialized tables: core.water_yearly, core.waste_yearly
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- WATER: YoY
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_water_total_yoy','V') IS NOT NULL
    DROP VIEW mart.v_water_total_yoy;
GO
CREATE VIEW mart.v_water_total_yoy
AS
WITH base AS (
    SELECT year, SUM(value) AS water_m3
    FROM core.water_yearly
    GROUP BY year
)
SELECT
    year,
    water_m3,
    LAG(water_m3) OVER (ORDER BY year) AS prev_m3,
    CASE WHEN LAG(water_m3) OVER (ORDER BY year) IS NULL THEN NULL
         ELSE water_m3 - LAG(water_m3) OVER (ORDER BY year) END AS abs_change_m3,
    CASE WHEN LAG(water_m3) OVER (ORDER BY year) IS NULL OR LAG(water_m3) OVER (ORDER BY year)=0 THEN NULL
         ELSE (water_m3 - LAG(water_m3) OVER (ORDER BY year)) / LAG(water_m3) OVER (ORDER BY year) END AS yoy_pct
FROM base;
GO

---------------------------------------------------------------------------------------------
-- WATER: 3-year rolling average
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_water_total_trend_3yr','V') IS NOT NULL
    DROP VIEW mart.v_water_total_trend_3yr;
GO
CREATE VIEW mart.v_water_total_trend_3yr
AS
WITH base AS (
    SELECT year, SUM(value) AS water_m3
    FROM core.water_yearly
    GROUP BY year
)
SELECT
    year,
    water_m3,
    AVG(water_m3) OVER (ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m3_3yr_avg
FROM base;
GO

---------------------------------------------------------------------------------------------
-- WASTE: YoY
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_waste_total_yoy','V') IS NOT NULL
    DROP VIEW mart.v_waste_total_yoy;
GO
CREATE VIEW mart.v_waste_total_yoy
AS
WITH base AS (
    SELECT year, SUM(value) AS waste_t
    FROM core.waste_yearly
    GROUP BY year
)
SELECT
    year,
    waste_t,
    LAG(waste_t) OVER (ORDER BY year) AS prev_t,
    CASE WHEN LAG(waste_t) OVER (ORDER BY year) IS NULL THEN NULL
         ELSE waste_t - LAG(waste_t) OVER (ORDER BY year) END AS abs_change_t,
    CASE WHEN LAG(waste_t) OVER (ORDER BY year) IS NULL OR LAG(waste_t) OVER (ORDER BY year)=0 THEN NULL
         ELSE (waste_t - LAG(waste_t) OVER (ORDER BY year)) / LAG(waste_t) OVER (ORDER BY year) END AS yoy_pct
FROM base;
GO

---------------------------------------------------------------------------------------------
-- WASTE: 3-year rolling average
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_waste_total_trend_3yr','V') IS NOT NULL
    DROP VIEW mart.v_waste_total_trend_3yr;
GO
CREATE VIEW mart.v_waste_total_trend_3yr
AS
WITH base AS (
    SELECT year, SUM(value) AS waste_t
    FROM core.waste_yearly
    GROUP BY year
)
SELECT
    year,
    waste_t,
    AVG(waste_t) OVER (ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS t_3yr_avg
FROM base;
GO

-- Peek (recent few rows)
SELECT TOP (10) 'water_yoy' AS what, * FROM mart.v_water_total_yoy ORDER BY year DESC;
SELECT TOP (10) 'waste_yoy' AS what, * FROM mart.v_waste_total_yoy ORDER BY year DESC;
