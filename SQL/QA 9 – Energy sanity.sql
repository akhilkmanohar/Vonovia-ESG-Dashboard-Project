/* =========================================================================================
   QA 9 – Energy sanity (single result set)
   Sections:
     - year_map_summary : columns→year (post-refinement), count by sheet/year
     - future_years     : any years > current_year+1 (should be zero)
     - yearly_peek      : sample facts
     - totals_by_year   : quick totals from filtered core.energy_yearly
   ========================================================================================= */
SET NOCOUNT ON;

DECLARE @ymax int = YEAR(GETUTCDATE()) + 1;

WITH year_map_summary AS (
    SELECT 'year_map_summary' AS section,
           y.sheet_name AS sheet_name,
           CAST(y.year AS nvarchar(50)) AS k1,
           y.value_col AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.v_energy_year_col_map y
),
future_years AS (
    SELECT 'future_years' AS section,
           e.sheet_name,
           CAST(e.year AS nvarchar(50)) AS k1,
           LEFT(e.row_label, 300) AS k2,
           CAST(e.value AS nvarchar(100)) AS k3,
           e.derived_unit AS k4
    FROM core.v_energy_yearly e
    WHERE e.year > @ymax
),
yearly_peek AS (
    SELECT TOP (80)
           'yearly_peek' AS section,
           e.sheet_name,
           CAST(e.year AS nvarchar(50)) AS k1,
           LEFT(e.row_label, 300) AS k2,
           CAST(e.value AS nvarchar(100)) AS k3,
           e.derived_unit AS k4
    FROM core.v_energy_yearly e
    ORDER BY e.year DESC, e.sheet_name, e.row_label
),
totals_by_year AS (
    SELECT 'totals_by_year' AS section,
           CAST(NULL AS nvarchar(255)) AS sheet_name,
           CAST(e.year AS nvarchar(50)) AS k1,
           CAST(SUM(e.value) AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.energy_yearly e
    GROUP BY e.year
)
SELECT section, sheet_name, k1, k2, k3, k4 FROM year_map_summary
UNION ALL
SELECT section, sheet_name, k1, k2, k3, k4 FROM future_years
UNION ALL
SELECT section, sheet_name, k1, k2, k3, k4 FROM yearly_peek
UNION ALL
SELECT section, sheet_name, k1, k2, k3, k4 FROM totals_by_year
ORDER BY section, sheet_name, k1, k2;
