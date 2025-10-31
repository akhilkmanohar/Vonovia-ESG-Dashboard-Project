/* =========================================================================================
   QA 24 – Environment unified long (single-sheet)
   Sections:
     - coverage_by_metric : min/max year + count
     - yearly_recent      : last 10 years per metric (stacked)
     - yoy_recent         : YoY % (last 10 years) per metric
     - trend_recent       : 3yr avg (last 10 years) per metric
     - latest_snapshot    : most recent year by metric
   ========================================================================================= */
SET NOCOUNT ON;

WITH coverage_by_metric AS (
    SELECT 'coverage_by_metric' AS section,
           metric_code AS k1,
           CAST(MIN(year) AS nvarchar(10)) AS k2,
           CAST(MAX(year) AS nvarchar(10)) AS k3,
           CAST(COUNT(*)  AS nvarchar(10)) AS k4
    FROM mart.v_env_unified_yearly
    GROUP BY metric_code
),
yearly_recent AS (
    SELECT TOP (30)
           'yearly_recent' AS section,
           metric_code AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           CAST(value AS nvarchar(100)) AS k3,
           unit AS k4
    FROM mart.v_env_unified_yearly
    ORDER BY metric_code, year DESC
),
yoy_recent AS (
    SELECT TOP (30)
           'yoy_recent' AS section,
           metric_code AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           CAST(yoy_pct AS nvarchar(100)) AS k3,
           CAST(abs_change AS nvarchar(100)) AS k4
    FROM mart.v_env_unified_yoy
    ORDER BY metric_code, year DESC
),
trend_recent AS (
    SELECT TOP (30)
           'trend_recent' AS section,
           metric_code AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           CAST(value_3yr_avg AS nvarchar(100)) AS k3,
           unit AS k4
    FROM mart.v_env_unified_trend_3yr
    ORDER BY metric_code, year DESC
),
latest_snapshot AS (
    SELECT
        'latest_snapshot' AS section,
        y.metric_code AS k1,
        CAST(y.year AS nvarchar(10)) AS k2,
        CAST(y.value AS nvarchar(100)) AS k3,
        y.unit AS k4
    FROM mart.v_env_unified_yearly y
    WHERE y.year = (SELECT MAX(year) FROM mart.v_env_unified_yearly)
)
SELECT * FROM coverage_by_metric
UNION ALL SELECT * FROM yearly_recent
UNION ALL SELECT * FROM yoy_recent
UNION ALL SELECT * FROM trend_recent
UNION ALL SELECT * FROM latest_snapshot
ORDER BY section, k1, k2;
