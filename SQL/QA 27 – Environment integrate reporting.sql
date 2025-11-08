/* =========================================================================================
   QA 27 – Environment integrate reporting (single-sheet)
   Sections:
     - proc_exists       : confirm reporting proc presence
     - fact_counts       : row counts/min/max year per metric in rpt.fact_env_totals
     - latest_snapshot   : latest year/value per metric from fact
     - cards_vs_fact     : consistency check between cards_latest and fact latest
   ========================================================================================= */
SET NOCOUNT ON;

WITH proc_exists AS (
    SELECT 'proc_exists' AS section,
           'rpt.sp_refresh_env_reporting' AS k1,
           CASE WHEN OBJECT_ID('rpt.sp_refresh_env_reporting','P') IS NOT NULL THEN 'yes' ELSE 'no' END AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
),
fact_counts AS (
    SELECT 'fact_counts' AS section,
           f.metric_code AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(MIN(f.year) AS nvarchar(10)) AS k3,
           CAST(MAX(f.year) AS nvarchar(10)) AS k4
    FROM rpt.fact_env_totals f
    GROUP BY f.metric_code
),
latest_snapshot AS (
    SELECT 'latest_snapshot' AS section,
           f.metric_code AS k1,
           CAST(f.year AS nvarchar(10)) AS k2,
           CAST(f.value AS nvarchar(100)) AS k3,
           d.unit AS k4
    FROM rpt.fact_env_totals f
    JOIN rpt.d_env_metric d ON d.metric_code = f.metric_code
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT metric_code, MAX(year) AS max_year
            FROM rpt.fact_env_totals
            GROUP BY metric_code
        ) mx
        WHERE mx.metric_code = f.metric_code AND mx.max_year = f.year
    )
),
cards_vs_fact AS (
    SELECT 'cards_vs_fact' AS section,
           c.metric_code AS k1,
           CAST(c.value AS nvarchar(100)) AS k2,
           CAST(f.value AS nvarchar(100)) AS k3,
           CAST(c.value - f.value AS nvarchar(100)) AS k4
    FROM mart.v_env_cards_latest c
    JOIN rpt.fact_env_totals f
      ON f.metric_code = c.metric_code
     AND f.year = c.year
)
SELECT * FROM proc_exists
UNION ALL
SELECT * FROM fact_counts
UNION ALL
SELECT * FROM latest_snapshot
UNION ALL
SELECT * FROM cards_vs_fact
ORDER BY section, k1, k2, k3;
