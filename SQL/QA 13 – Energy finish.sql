/* =========================================================================================
   QA 13 – Energy finish (single result set)
   Sections:
     - counts_by_year   : materialized final counts
     - totals_by_year   : mart totals (top 25 recent-first)
     - yoy_preview      : YoY levels (% and absolute)
     - no_totals_check  : verify no "TOTAL/SUMME" labels remain in core.energy_yearly
   ========================================================================================= */
SET NOCOUNT ON;

WITH counts_by_year AS (
    SELECT 'counts_by_year' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.energy_yearly
    GROUP BY year
),
totals_by_year AS (
    SELECT TOP (25)
           'totals_by_year' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(energy_mwh AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_energy_total_by_year
    ORDER BY year DESC
),
yoy_preview AS (
    SELECT TOP (25)
           'yoy_preview' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(energy_mwh AS nvarchar(100)) AS k2,
           CAST(yoy_pct AS nvarchar(100)) AS k3,
           CAST(abs_change_mwh AS nvarchar(100)) AS k4
    FROM mart.v_energy_total_yoy
    ORDER BY year DESC
),
no_totals_check AS (
    SELECT 'no_totals_check' AS section,
           CASE WHEN EXISTS (
               SELECT 1 FROM core.energy_yearly
               WHERE UPPER(row_label) LIKE '%TOTAL%' OR UPPER(row_label) LIKE N'%SUMME%'
           ) THEN 'FOUND' ELSE 'OK' END AS k1,
           CAST(NULL AS nvarchar(4000)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
)
SELECT * FROM counts_by_year
UNION ALL
SELECT * FROM totals_by_year
UNION ALL
SELECT * FROM yoy_preview
UNION ALL
SELECT * FROM no_totals_check
ORDER BY section, k1, k2;
