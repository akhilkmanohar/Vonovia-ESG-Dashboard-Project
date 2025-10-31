/* =========================================================================================
   QA 25 – Environment dashboard cards (single-sheet)
   Sections:
     - cards_latest   : one row per metric with YoY and 3yr avg
     - last5_energy   : last 5 values for energy
     - last5_water    : last 5 values for water
     - last5_waste    : last 5 values for waste
   ========================================================================================= */
SET NOCOUNT ON;

WITH cards_latest AS (
    SELECT 'cards_latest' AS section,
           metric_code AS k1,
           CONCAT(metric_label, ' (', unit, ')') AS k2,
           CONCAT('year=', year) AS k3,
           CAST(value AS nvarchar(100)) AS k4
    FROM mart.v_env_cards_latest
),
yoy_bits AS (
    SELECT 'cards_latest' AS section,
           metric_code AS k1,
           'YoY % / YoY abs' AS k2,
           CAST(yoy_pct AS nvarchar(100)) AS k3,
           CAST(yoy_abs_change AS nvarchar(100)) AS k4
    FROM mart.v_env_cards_latest
),
avg_bits AS (
    SELECT 'cards_latest' AS section,
           metric_code AS k1,
           '3yr_avg' AS k2,
           CAST(value_3yr_avg AS nvarchar(100)) AS k3,
           CAST(NULL AS nvarchar(100)) AS k4
    FROM mart.v_env_cards_latest
),
last5_energy AS (
    SELECT 'last5_energy' AS section,
           CAST(sub.year AS nvarchar(10)) AS k1,
           CAST(sub.value AS nvarchar(100)) AS k2,
           sub.unit AS k3,
           CAST(NULL AS nvarchar(100)) AS k4
    FROM (
        SELECT year, value, unit,
               ROW_NUMBER() OVER (ORDER BY year DESC) AS rn
        FROM mart.v_env_cards_ts_last5
        WHERE metric_code='energy_mwh'
    ) sub
    WHERE sub.rn <= 5
),
last5_water AS (
    SELECT 'last5_water' AS section,
           CAST(sub.year AS nvarchar(10)) AS k1,
           CAST(sub.value AS nvarchar(100)) AS k2,
           sub.unit AS k3,
           CAST(NULL AS nvarchar(100)) AS k4
    FROM (
        SELECT year, value, unit,
               ROW_NUMBER() OVER (ORDER BY year DESC) AS rn
        FROM mart.v_env_cards_ts_last5
        WHERE metric_code='water_m3'
    ) sub
    WHERE sub.rn <= 5
),
last5_waste AS (
    SELECT 'last5_waste' AS section,
           CAST(sub.year AS nvarchar(10)) AS k1,
           CAST(sub.value AS nvarchar(100)) AS k2,
           sub.unit AS k3,
           CAST(NULL AS nvarchar(100)) AS k4
    FROM (
        SELECT year, value, unit,
               ROW_NUMBER() OVER (ORDER BY year DESC) AS rn
        FROM mart.v_env_cards_ts_last5
        WHERE metric_code='waste_t'
    ) sub
    WHERE sub.rn <= 5
)
SELECT * FROM cards_latest
UNION ALL SELECT * FROM yoy_bits
UNION ALL SELECT * FROM avg_bits
UNION ALL SELECT * FROM last5_energy
UNION ALL SELECT * FROM last5_water
UNION ALL SELECT * FROM last5_waste
ORDER BY section, k1, k2, k3;
