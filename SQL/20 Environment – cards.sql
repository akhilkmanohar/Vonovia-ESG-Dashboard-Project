/* =========================================================================================
   Module 20 – Environment: Dashboard cards (latest + last-5 series)
   Inputs:
     - mart.v_env_unified_yearly
     - mart.v_env_unified_yoy
     - mart.v_env_unified_trend_3yr
   Outputs:
     - mart.v_env_cards_latest
     - mart.v_env_cards_ts_last5
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- Latest card rows (one per metric)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_cards_latest','V') IS NOT NULL
    DROP VIEW mart.v_env_cards_latest;
GO
CREATE VIEW mart.v_env_cards_latest
AS
WITH latest_year AS (
    SELECT metric_code, MAX(year) AS year
    FROM mart.v_env_unified_yearly
    GROUP BY metric_code
),
Y AS (
    SELECT u.metric_code, u.metric_label, u.unit, u.year, u.value
    FROM mart.v_env_unified_yearly u
    INNER JOIN latest_year ly
        ON ly.metric_code = u.metric_code AND ly.year = u.year
),
yoy AS (
    SELECT metric_code, year, yoy_pct, abs_change
    FROM mart.v_env_unified_yoy
),
t3 AS (
    SELECT metric_code, year, value_3yr_avg
    FROM mart.v_env_unified_trend_3yr
)
SELECT
    y.metric_code,
    y.metric_label,
    y.unit,
    y.year,
    y.value,
    yy.yoy_pct,
    yy.abs_change AS yoy_abs_change,
    t.value_3yr_avg
FROM y
LEFT JOIN yoy  yy ON yy.metric_code = y.metric_code AND yy.year = y.year
LEFT JOIN t3   t  ON  t.metric_code = y.metric_code AND  t.year = y.year;
GO

---------------------------------------------------------------------------------------------
-- Last 5 years per metric (for sparklines)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_env_cards_ts_last5','V') IS NOT NULL
    DROP VIEW mart.v_env_cards_ts_last5;
GO
CREATE VIEW mart.v_env_cards_ts_last5
AS
WITH maxy AS (
    SELECT metric_code, MAX(year) AS max_year
    FROM mart.v_env_unified_yearly
    GROUP BY metric_code
)
SELECT
    u.metric_code,
    u.metric_label,
    u.unit,
    u.year,
    u.value
FROM mart.v_env_unified_yearly u
INNER JOIN maxy m
    ON m.metric_code = u.metric_code
WHERE u.year BETWEEN m.max_year - 4 AND m.max_year;
GO

-- Peek for run summary
SELECT * FROM mart.v_env_cards_latest;
SELECT TOP (30) * FROM mart.v_env_cards_ts_last5 ORDER BY metric_code, year DESC;
