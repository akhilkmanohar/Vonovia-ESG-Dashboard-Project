/* =========================================================================================
   QA 8 – Energy marts & tagging
   Single-sheet output with sections:
     - tag_counts      : how many rows match each tag
     - yearly_totals   : mart totals by year (MWh)
     - top_values      : top 50 raw yearly facts (post-filter) for eyeballing outliers
     - by_sheet_year   : distribution across sheets
   ========================================================================================= */
SET NOCOUNT ON;

WITH tag_counts AS (
    SELECT 'tag_counts' AS section,
           'rates'      AS k1, CAST(SUM(CASE WHEN is_rate_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3, CAST(NULL AS nvarchar(4000)) AS k4
    FROM core.v_energy_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','intensity',
           CAST(SUM(CASE WHEN is_intensity_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_energy_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','index',
           CAST(SUM(CASE WHEN is_index_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_energy_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','totals',
           CAST(SUM(CASE WHEN is_total_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_energy_yearly_tagged
    UNION ALL
    SELECT 'tag_counts','notes',
           CAST(SUM(CASE WHEN is_note_like=1 THEN 1 ELSE 0 END) AS nvarchar(50)), NULL, NULL
    FROM core.v_energy_yearly_tagged
),
yearly_totals AS (
    SELECT 'yearly_totals' AS section,
           CAST(y.year AS nvarchar(50)) AS k1,
           CAST(y.energy_mwh AS nvarchar(100)) AS k2,
           CAST(NULL AS nvarchar(4000)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_energy_total_by_year y
),
top_values AS (
    SELECT TOP (50)
           'top_values' AS section,
           f.sheet_name AS k1,
           CAST(f.year AS nvarchar(50)) AS k2,
           LEFT(f.row_label, 300) AS k3,
           CAST(f.value AS nvarchar(100)) AS k4
    FROM core.v_energy_yearly_filtered f
    ORDER BY f.value DESC
),
by_sheet_year AS (
    SELECT 'by_sheet_year' AS section,
           s.sheet_name AS k1,
           CAST(s.year AS nvarchar(50)) AS k2,
           CAST(s.energy_mwh AS nvarchar(100)) AS k3,
           CAST(NULL AS nvarchar(4000)) AS k4
    FROM mart.v_energy_by_sheet_year s
)
SELECT * FROM tag_counts
UNION ALL
SELECT * FROM yearly_totals
UNION ALL
SELECT * FROM top_values
UNION ALL
SELECT * FROM by_sheet_year
ORDER BY section, k1, k2;
