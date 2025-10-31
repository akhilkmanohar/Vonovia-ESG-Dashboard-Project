/* =========================================================================================
   QA 26 – Environment reporting star (single-sheet)
   Sections:
     - dim_metrics      : list of metrics in d_env_metric
     - coverage         : min/max year + count per metric in fact
     - dup_natural_keys : (metric_code, year) duplicates (should be 0 thanks to UNIQUE)
     - latest_snapshot  : most recent year per metric from fact
   ========================================================================================= */
SET NOCOUNT ON;

WITH dim_metrics AS (
    SELECT 'dim_metrics' AS section,
           metric_code AS k1,
           metric_label AS k2,
           unit AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM rpt.d_env_metric
),
coverage AS (
    SELECT 'coverage' AS section,
           metric_code AS k1,
           CAST(MIN(year) AS nvarchar(10)) AS k2,
           CAST(MAX(year) AS nvarchar(10)) AS k3,
           CAST(COUNT(*)  AS nvarchar(10)) AS k4
    FROM rpt.fact_env_totals
    GROUP BY metric_code
),
dup_natural_keys AS (
    SELECT 'dup_natural_keys' AS section,
           metric_code AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           CAST(COUNT(*) AS nvarchar(10)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM rpt.fact_env_totals
    GROUP BY metric_code, year
    HAVING COUNT(*) > 1
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
        ) AS mx
        WHERE mx.metric_code = f.metric_code AND mx.max_year = f.year
    )
)
SELECT * FROM dim_metrics
UNION ALL SELECT * FROM coverage
UNION ALL SELECT * FROM dup_natural_keys
UNION ALL SELECT * FROM latest_snapshot
ORDER BY section, k1, k2;
