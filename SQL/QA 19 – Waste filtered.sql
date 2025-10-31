/* =========================================================================================
   QA 19 – Waste filtered (single-sheet)
   Sections:
     - tag_counts      : counts of flagged rows
     - yearly_peek     : sample from final filtered view
     - totals_by_year  : mart totals by year (t)
     - by_sheet_year   : distribution across sheets
     - table_counts    : rows currently in core.waste_yearly
     - top_outliers    : largest 30 values (sanity)
   ========================================================================================= */
SET NOCOUNT ON;

WITH tag_counts AS (
    SELECT 'tag_counts' AS section,
           'rates'      AS k1, CAST(SUM(CASE WHEN is_rate_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3, CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.v_waste_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','intensity',
           CAST(SUM(CASE WHEN is_intensity_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_waste_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','index',
           CAST(SUM(CASE WHEN is_index_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_waste_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','totals',
           CAST(SUM(CASE WHEN is_total_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_waste_yearly_tagged
),
yearly_peek AS (
    SELECT TOP (150)
           'yearly_peek' AS section,
           f.sheet_name AS k1,
           CAST(f.year AS nvarchar(50)) AS k2,
           LEFT(f.row_label, 300) AS k3,
           CAST(f.value AS nvarchar(100)) AS k4
    FROM core.v_waste_yearly_final f
    ORDER BY f.year DESC, f.sheet_name, f.row_label
),
totals_by_year AS (
    SELECT TOP (25)
           'totals_by_year' AS section,
           CAST(y.year AS nvarchar(50)) AS k1,
           CAST(y.waste_t AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_waste_total_by_year y
    ORDER BY y.year DESC
),
by_sheet_year AS (
    SELECT 'by_sheet_year' AS section,
           s.sheet_name AS k1,
           CAST(s.year AS nvarchar(50)) AS k2,
           CAST(s.waste_t AS nvarchar(100)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_waste_by_sheet_year s
),
table_counts AS (
    SELECT 'table_counts' AS section,
           'core.waste_yearly' AS k1,
           CAST(COUNT(*) AS nvarchar(50)) AS k2,
           CAST(MIN(year) AS nvarchar(50)) AS k3,
           CAST(MAX(year) AS nvarchar(50)) AS k4
    FROM core.waste_yearly
),
top_outliers AS (
    SELECT TOP (30)
           'top_outliers' AS section,
           w.sheet_name AS k1,
           CAST(w.year AS nvarchar(50)) AS k2,
           LEFT(w.row_label, 300) AS k3,
           CAST(w.value AS nvarchar(100)) AS k4
    FROM core.waste_yearly w
    ORDER BY w.value DESC
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
UNION ALL
SELECT * FROM top_outliers
ORDER BY section, k1, k2, k3;
