/* =========================================================================================
   QA 23 – Environment summary (single-sheet)
   Sections:
     - coverage        : min/max year per metric + total years present
     - totals_recent   : last 10 years of side-by-side totals
     - yoy_recent      : last 10 years YoY by metric
     - latest_snapshot : most recent available year values
   ========================================================================================= */
SET NOCOUNT ON;

WITH coverage AS (
    SELECT 'coverage' AS section,
           'energy_mwh' AS k1,
           CAST(MIN(year) AS nvarchar(10)) AS k2,
           CAST(MAX(year) AS nvarchar(10)) AS k3,
           CAST(COUNT(*)  AS nvarchar(10)) AS k4
    FROM mart.v_energy_total_by_year
    UNION ALL
    SELECT 'coverage','water_m3',
           CAST(MIN(year) AS nvarchar(10)), CAST(MAX(year) AS nvarchar(10)), CAST(COUNT(*) AS nvarchar(10))
    FROM mart.v_water_total_by_year
    UNION ALL
    SELECT 'coverage','waste_t',
           CAST(MIN(year) AS nvarchar(10)), CAST(MAX(year) AS nvarchar(10)), CAST(COUNT(*) AS nvarchar(10))
    FROM mart.v_waste_total_by_year
),
totals_recent AS (
    SELECT TOP (10)
           'totals_recent' AS section,
           CAST(t.year AS nvarchar(10)) AS k1,
           CAST(t.energy_mwh AS nvarchar(100)) AS k2,
           CAST(t.water_m3   AS nvarchar(100)) AS k3,
           CAST(t.waste_t    AS nvarchar(100)) AS k4
    FROM mart.v_env_totals_by_year t
    ORDER BY t.year DESC
),
yoy_recent AS (
    SELECT TOP (10)
           'yoy_recent' AS section,
           CAST(y.year AS nvarchar(10)) AS k1,
           CAST(y.energy_yoy AS nvarchar(100)) AS k2,
           CAST(y.water_yoy  AS nvarchar(100)) AS k3,
           CAST(y.waste_yoy  AS nvarchar(100)) AS k4
    FROM mart.v_env_totals_yoy y
    ORDER BY y.year DESC
),
latest_snapshot AS (
    SELECT TOP (1)
           'latest_snapshot' AS section,
           CAST(y.year AS nvarchar(10)) AS k1,
           CAST(y.energy_mwh AS nvarchar(100)) AS k2,
           CAST(y.water_m3   AS nvarchar(100)) AS k3,
           CAST(y.waste_t    AS nvarchar(100)) AS k4
    FROM mart.v_env_totals_by_year y
    ORDER BY y.year DESC
)
SELECT * FROM coverage
UNION ALL SELECT * FROM totals_recent
UNION ALL SELECT * FROM yoy_recent
UNION ALL SELECT * FROM latest_snapshot
ORDER BY section, k1;
