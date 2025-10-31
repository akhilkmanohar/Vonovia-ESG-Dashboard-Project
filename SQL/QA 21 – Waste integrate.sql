/* =========================================================================================
   QA 21 – Waste integrate (single sheet)
   Sections:
     - proc_exists     : confirm waste proc is present
     - counts          : row counts for core.waste_yearly (min/max year)
     - totals_by_year  : mart totals top 20 (t)
   ========================================================================================= */

SET NOCOUNT ON;

WITH proc_exists AS (
    SELECT 'proc_exists' AS section,
           'core.sp_refresh_waste_yearly' AS k1,
           CASE WHEN OBJECT_ID('core.sp_refresh_waste_yearly','P') IS NOT NULL THEN 'yes' ELSE 'no' END AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
),
counts AS (
    SELECT 'counts' AS section,
           'core.waste_yearly' AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(MIN(year) AS nvarchar(10)) AS k3,
           CAST(MAX(year) AS nvarchar(10)) AS k4
    FROM core.waste_yearly
),
totals_by_year AS (
    SELECT TOP (20)
           'totals_by_year' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(waste_t AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_waste_total_by_year
    ORDER BY year DESC
)
SELECT * FROM proc_exists
UNION ALL
SELECT * FROM counts
UNION ALL
SELECT * FROM totals_by_year
ORDER BY section, k1, k2;
