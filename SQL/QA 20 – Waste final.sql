/* =========================================================================================
   QA 20 – Waste final (single result set)
   Sections:
     - counts_by_year     : final row counts per year
     - dup_natural_keys   : duplicate (sheet_name, year, row_label) rows
     - recent_sample      : sample of last 5 years
     - top_outliers       : top 30 values (sanity)
     - unit_rows_check    : confirm no "Unit/Einheit" labels remain
   ========================================================================================= */

SET NOCOUNT ON;

WITH counts_by_year AS (
    SELECT 'counts_by_year' AS section,
           CAST(year AS nvarchar(10)) AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.waste_yearly
    GROUP BY year
),
dup_natural_keys AS (
    SELECT 'dup_natural_keys' AS section,
           sheet_name AS k1,
           CAST(year AS nvarchar(10)) AS k2,
           LEFT(row_label, 280) AS k3,
           CAST(COUNT(*) AS nvarchar(50)) AS k4
    FROM core.waste_yearly
    GROUP BY sheet_name, year, row_label
    HAVING COUNT(*) > 1
),
recent_sample AS (
    SELECT 'recent_sample' AS section,
           e.sheet_name AS k1,
           CAST(e.year AS nvarchar(10)) AS k2,
           LEFT(e.row_label, 280) AS k3,
           CAST(e.value AS nvarchar(100)) AS k4
    FROM core.waste_yearly e
    WHERE e.year >= YEAR(GETUTCDATE()) - 5
),
top_outliers AS (
    SELECT TOP (30)
           'top_outliers' AS section,
           e.sheet_name AS k1,
           CAST(e.year AS nvarchar(10)) AS k2,
           LEFT(e.row_label, 280) AS k3,
           CAST(e.value AS nvarchar(100)) AS k4
    FROM core.waste_yearly e
    ORDER BY e.value DESC
),
unit_rows_check AS (
    SELECT TOP (1)
           'unit_rows_check' AS section,
           MIN(CASE WHEN UPPER(row_label) IN (N'UNIT',N'UNITS',N'EINHEIT',N'EINHEITEN')
                     OR UPPER(row_label) LIKE N'% UNIT'
                     OR UPPER(row_label) LIKE N'UNIT %'
                     OR UPPER(row_label) LIKE N'% EINHEIT%'
                    THEN 'FOUND' ELSE 'OK' END) AS k1,
           CAST(NULL AS nvarchar(4000)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.waste_yearly
)
SELECT * FROM counts_by_year
UNION ALL
SELECT * FROM dup_natural_keys
UNION ALL
SELECT * FROM recent_sample
UNION ALL
SELECT * FROM top_outliers
UNION ALL
SELECT * FROM unit_rows_check
ORDER BY section, k1, k2, k3;
