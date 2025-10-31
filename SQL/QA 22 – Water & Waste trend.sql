/* =========================================================================================
   QA 22 – Water & Waste YoY / Trend (single-sheet)
   Sections:
     - water_totals    : totals by year (m³)
     - water_yoy       : YoY % and absolute (Top 15 recent-first)
     - water_trend     : 3-yr avg (Top 15 recent-first)
     - waste_totals    : totals by year (t)
     - waste_yoy       : YoY % and absolute (Top 15 recent-first)
     - waste_trend     : 3-yr avg (Top 15 recent-first)
   ========================================================================================= */
SET NOCOUNT ON;

WITH water_totals AS (
    SELECT TOP (50)
           'water_totals' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(water_m3 AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_water_total_by_year
    ORDER BY year DESC
),
water_yoy AS (
    SELECT TOP (15)
           'water_yoy' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(yoy_pct AS nvarchar(100)) AS k2,
           CAST(abs_change_m3 AS nvarchar(100)) AS k3,
           CAST(water_m3 AS nvarchar(100)) AS k4
    FROM mart.v_water_total_yoy
    ORDER BY year DESC
),
water_trend AS (
    SELECT TOP (15)
           'water_trend' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(m3_3yr_avg AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_water_total_trend_3yr
    ORDER BY year DESC
),
waste_totals AS (
    SELECT TOP (50)
           'waste_totals' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(waste_t AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_waste_total_by_year
    ORDER BY year DESC
),
waste_yoy AS (
    SELECT TOP (15)
           'waste_yoy' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(yoy_pct AS nvarchar(100)) AS k2,
           CAST(abs_change_t AS nvarchar(100)) AS k3,
           CAST(waste_t AS nvarchar(100)) AS k4
    FROM mart.v_waste_total_yoy
    ORDER BY year DESC
),
waste_trend AS (
    SELECT TOP (15)
           'waste_trend' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(t_3yr_avg AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_waste_total_trend_3yr
    ORDER BY year DESC
)
SELECT * FROM water_totals
UNION ALL SELECT * FROM water_yoy
UNION ALL SELECT * FROM water_trend
UNION ALL SELECT * FROM waste_totals
UNION ALL SELECT * FROM waste_yoy
UNION ALL SELECT * FROM waste_trend
ORDER BY section, k1, k2;
