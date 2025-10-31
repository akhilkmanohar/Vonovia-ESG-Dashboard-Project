/* =========================================================================================
   QA 11 – Energy filtered final (single result set)
   Sections:
     - counts_by_year     : post-filter counts
     - dup_natural_keys   : duplicates on (sheet_name, year, row_label) — should be 0
     - recent_sample      : sample from last 5 years
     - top_outliers       : top 50 values after filter (sanity check magnitudes)
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
dup_natural_keys AS (
    SELECT 'dup_natural_keys' AS section,
           sheet_name AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           LEFT(row_label, 280) AS k3,
           CAST(COUNT(*) AS nvarchar(50)) AS k4
    FROM core.energy_yearly
    GROUP BY sheet_name, year, row_label
    HAVING COUNT(*) > 1
),
recent_sample AS (
    SELECT 'recent_sample' AS section,
           e.sheet_name AS k1,
           CAST(e.year AS nvarchar(10)) AS k2,
           LEFT(e.row_label, 280) AS k3,
           CAST(e.value AS nvarchar(100)) AS k4
    FROM core.energy_yearly e
    WHERE e.year >= YEAR(GETUTCDATE()) - 5
),
top_outliers AS (
    SELECT TOP (50)
           'top_outliers' AS section,
           e.sheet_name AS k1,
           CAST(e.year AS nvarchar(10)) AS k2,
           LEFT(e.row_label, 280) AS k3,
           CAST(e.value AS nvarchar(100)) AS k4
    FROM core.energy_yearly e
    ORDER BY e.value DESC
)
SELECT * FROM counts_by_year
UNION ALL
SELECT * FROM dup_natural_keys
UNION ALL
SELECT * FROM recent_sample
UNION ALL
SELECT * FROM top_outliers
ORDER BY section, k1, k2, k3;
