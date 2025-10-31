/* =========================================================================================
   Module 16b – Environment: Waste
   Tag rows → filtered, deduped facts → MERGE → marts
   - Tags: rate/percentage, intensity (per m² / per unit), index, totals, notes
   - Filtered view keeps tangible waste amounts (normalized to tonnes "t"), caps magnitudes
   - Final view dedupes by (sheet_name, year, row_label)
   - Marts: totals by year; by sheet & year
   Prereqs:
     - core.v_waste_yearly exists (values normalized to t) from "16 Waste – matrix and yearly.sql"
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Tagging view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_yearly_tagged','V') IS NOT NULL
    DROP VIEW core.v_waste_yearly_tagged;
GO
CREATE VIEW core.v_waste_yearly_tagged
AS
WITH src AS (
    SELECT
        w.sheet_name,
        w.category,
        w.subcategory,
        w.year,
        w.row_label,
        w.value,
        w.derived_unit,
        UPPER(COALESCE(w.row_label, N'')) AS lbl_u
    FROM core.v_waste_yearly w
)
SELECT
    s.*,
    -- percent-like (labels often contain % or "share/rate/quote/anteil")
    CASE WHEN CHARINDEX('%', s.lbl_u) > 0
           OR s.lbl_u LIKE '%SHARE%' OR s.lbl_u LIKE N'%ANTEIL%'
           OR s.lbl_u LIKE '%RATE%'  OR s.lbl_u LIKE N'%QUOTE%'
         THEN 1 ELSE 0 END                                         AS is_rate_like,

    -- intensity / normalized metrics (per m², per unit, kg/FTE, t/unit, etc.)
    CASE WHEN s.lbl_u LIKE '%/M2%' OR s.lbl_u LIKE '% PER M2%' OR s.lbl_u LIKE N'%M²%'
           OR s.lbl_u LIKE '%/UNIT%' OR s.lbl_u LIKE '% PER UNIT%' OR s.lbl_u LIKE N'%PRO EINHEIT%'
           OR s.lbl_u LIKE '%KG/M2%' OR s.lbl_u LIKE '%T/M2%' OR s.lbl_u LIKE '%KG/FTE%'
           OR s.lbl_u LIKE '%INTENS%'
         THEN 1 ELSE 0 END                                         AS is_intensity_like,

    -- index-style metrics
    CASE WHEN s.lbl_u LIKE '%INDEX%' THEN 1 ELSE 0 END             AS is_index_like,

    -- explicit totals
    CASE WHEN s.lbl_u LIKE '%TOTAL%' OR s.lbl_u LIKE N'%SUMME%' THEN 1 ELSE 0 END AS is_total_like,

    -- notes / comments
    CASE WHEN s.lbl_u LIKE '%NOTE%' OR s.lbl_u LIKE '%COMMENT%' OR s.lbl_u LIKE N'%KOMMENTAR%'
         THEN 1 ELSE 0 END                                         AS is_note_like
FROM src s;
GO

---------------------------------------------------------------------------------------------
-- 2) Filtered facts (exclude rates, intensities, indices, totals; keep plausible magnitudes)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_yearly_filtered','V') IS NOT NULL
    DROP VIEW core.v_waste_yearly_filtered;
GO
CREATE VIEW core.v_waste_yearly_filtered
AS
SELECT
    t.sheet_name,
    t.category,
    t.subcategory,
    t.year,
    t.row_label,
    -- plausible corporate annual waste volumes: 0 .. 1,000,000 t (adjust if needed)
    CASE WHEN t.value BETWEEN 0 AND 1000000 THEN t.value END AS value,
    t.derived_unit
FROM core.v_waste_yearly_tagged t
WHERE t.derived_unit = N't'
  AND t.is_rate_like = 0
  AND t.is_intensity_like = 0
  AND t.is_index_like = 0
  AND t.is_total_like = 0;
GO

---------------------------------------------------------------------------------------------
-- 3) Final deduped view (one row per natural key)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_yearly_final','V') IS NOT NULL
    DROP VIEW core.v_waste_yearly_final;
GO
CREATE VIEW core.v_waste_yearly_final
AS
SELECT
    f.sheet_name,
    MIN(f.category)    AS category,
    MIN(f.subcategory) AS subcategory,
    f.year,
    f.row_label,
    SUM(f.value)       AS value,
    CAST(N't' AS nvarchar(50)) AS derived_unit
FROM core.v_waste_yearly_filtered f
GROUP BY f.sheet_name, f.year, f.row_label;
GO

---------------------------------------------------------------------------------------------
-- 4) Materialize into core.waste_yearly via MERGE
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.waste_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.waste_yearly
    (
        waste_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
        sheet_name   nvarchar(255) NOT NULL,
        category     nvarchar(255) NULL,
        subcategory  nvarchar(255) NULL,
        year         int NOT NULL,
        row_label    nvarchar(1000) NULL,
        value        decimal(38,6) NULL,
        derived_unit nvarchar(50) NOT NULL,
        load_dts     datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_waste_yearly_year  ON core.waste_yearly(year);
    CREATE INDEX IX_waste_yearly_sheet ON core.waste_yearly(sheet_name, year);
END
;

;WITH src AS (SELECT * FROM core.v_waste_yearly_final)
MERGE core.waste_yearly AS tgt
USING src
   ON tgt.sheet_name = src.sheet_name
  AND tgt.year       = src.year
  AND ISNULL(tgt.row_label,'') = ISNULL(src.row_label,'')
WHEN MATCHED AND (
       ISNULL(tgt.value, 0)       <> ISNULL(src.value, 0)
    OR ISNULL(tgt.derived_unit,'') <> ISNULL(src.derived_unit,'')
    OR ISNULL(tgt.category,'')    <> ISNULL(src.category,'')
    OR ISNULL(tgt.subcategory,'') <> ISNULL(src.subcategory,'')
)
THEN UPDATE SET
    tgt.value        = src.value,
    tgt.derived_unit = src.derived_unit,
    tgt.category     = src.category,
    tgt.subcategory  = src.subcategory,
    tgt.load_dts     = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET
THEN INSERT (sheet_name, category, subcategory, year, row_label, value, derived_unit)
     VALUES (src.sheet_name, src.category, src.subcategory, src.year, src.row_label, src.value, src.derived_unit)
WHEN NOT MATCHED BY SOURCE
THEN DELETE
;

---------------------------------------------------------------------------------------------
-- 5) Simple marts (based on the materialized table)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('mart.v_waste_total_by_year','V') IS NOT NULL
    DROP VIEW mart.v_waste_total_by_year;
GO
CREATE VIEW mart.v_waste_total_by_year
AS
SELECT
    year,
    SUM(value) AS waste_t
FROM core.waste_yearly
GROUP BY year;
GO

IF OBJECT_ID('mart.v_waste_by_sheet_year','V') IS NOT NULL
    DROP VIEW mart.v_waste_by_sheet_year;
GO
CREATE VIEW mart.v_waste_by_sheet_year
AS
SELECT
    sheet_name,
    year,
    SUM(value) AS waste_t
FROM core.waste_yearly
GROUP BY sheet_name, year;
GO

-- Peek (supports run summary)
SELECT TOP (20) year, sheet_name, LEFT(row_label, 120) AS row_label, value, derived_unit
FROM core.waste_yearly
ORDER BY year DESC, sheet_name, row_label;
