/* ===========================================================
   QA 18 – Waste (single-sheet export)
   Sections:
     - candidates      : candidate sheets detected
     - year_col_map    : strict column→year mapping per sheet
     - yearly_peek     : sample of parsed yearly facts (t)
     - density         : rows_with_value vs total by sheet/year
   =========================================================== */
SET NOCOUNT ON;

WITH candidates AS (
    SELECT DISTINCT sheet_name, category
    FROM stg.v_raw_all_with_cat
    WHERE (category LIKE '%Environment%' OR category LIKE '%Environmental%')
      AND sheet_name NOT LIKE '%Greenhouse Gas Balance%'
      AND sheet_name NOT LIKE '%GHG%'
      AND sheet_name NOT LIKE '%Energy%'
      AND sheet_name NOT LIKE '%Water%'
      AND (sheet_name LIKE '%Waste%' OR sheet_name LIKE N'%Abfall%' OR
           sheet_name LIKE '%Recycl%' OR sheet_name LIKE N'%Entsorg%' OR
           sheet_name LIKE '%Disposal%' OR sheet_name LIKE '%Garbage%')
),
q_candidates AS (
    SELECT 'candidates' AS section, sheet_name,
           category AS k1,
           CAST(NULL AS nvarchar(4000)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM candidates
),
q_yearcolmap AS (
    SELECT 'year_col_map' AS section, y.sheet_name,
           CAST(y.year AS nvarchar(50)) AS k1,
           y.value_col AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.v_waste_year_col_map y
),
q_yearly AS (
    SELECT 'yearly_peek' AS section, y.sheet_name,
           CAST(y.year AS nvarchar(50)) AS k1,
           LEFT(y.row_label, 300) AS k2,
           CAST(y.value AS nvarchar(100)) AS k3,
           y.derived_unit AS k4
    FROM core.v_waste_yearly y
),
q_density AS (
    SELECT 'density' AS section,
           y.sheet_name,
           CAST(y.year AS nvarchar(50)) AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(SUM(CASE WHEN y.value IS NOT NULL THEN 1 ELSE 0 END) AS nvarchar(50)) AS k3,
           CAST(SUM(CASE WHEN y.value IS NULL THEN 1 ELSE 0 END) AS nvarchar(50)) AS k4
    FROM core.v_waste_yearly y
    GROUP BY y.sheet_name, y.year
)
SELECT * FROM q_candidates
UNION ALL
SELECT * FROM q_yearcolmap
UNION ALL
SELECT * FROM q_yearly
UNION ALL
SELECT * FROM q_density
ORDER BY section, sheet_name, k1, k2;
