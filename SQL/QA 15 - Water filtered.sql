/* =========================================================================================
   QA 15 – Water filtered (single-sheet)
   Sections:
     - tag_counts      : counts of flagged rows
     - yearly_peek     : sample from final filtered view
     - totals_by_year  : mart totals by year (m³)
     - by_sheet_year   : distribution across sheets
     - table_counts    : rows currently in core.water_yearly
   ========================================================================================= */
SET NOCOUNT ON;

WITH tag_counts AS (
    SELECT 'tag_counts' AS section,
           'rates'      AS k1, CAST(SUM(CASE WHEN is_rate_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3, CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.v_water_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','intensity',
           CAST(SUM(CASE WHEN is_intensity_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_water_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','index',
           CAST(SUM(CASE WHEN is_index_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_water_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','totals',
           CAST(SUM(CASE WHEN is_total_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_water_yearly_tagged
),
yearly_peek AS (
    SELECT TOP (200)
           'yearly_peek' AS section,
           f.sheet_name AS k1,
           CAST(f.year AS nvarchar(50)) AS k2,
           LEFT(f.row_label, 300) AS k3,
           CAST(f.value AS nvarchar(100)) AS k4
    FROM core.v_water_yearly_final f
    ORDER BY f.year DESC, f.sheet_name, f.row_label
),
totals_by_year AS (
    SELECT TOP (25)
           'totals_by_year' AS section,
           CAST(y.year AS nvarchar(50)) AS k1,
           CAST(y.water_m3 AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_water_total_by_year y
    ORDER BY y.year DESC
),
by_sheet_year AS (
    SELECT 'by_sheet_year' AS section,
           s.sheet_name AS k1,
           CAST(s.year AS nvarchar(50)) AS k2,
           CAST(s.water_m3 AS nvarchar(100)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_water_by_sheet_year s
),
table_counts AS (
    SELECT 'table_counts' AS section,
           'core.water_yearly' AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(MIN(year) AS nvarchar(50)) AS k3,
           CAST(MAX(year) AS nvarchar(50)) AS k4
    FROM core.water_yearly
)
SELECT * FROM tag_counts
UNION ALL
SELECT * FROM yearly_peek
UNION ALL
SELECT * FROM totals_by_year
UNION ALL
SELECT * FROM by_sheet_year
UNION ALL
SELECT * FROM table_counts
ORDER BY section, k1, k2, k3;
