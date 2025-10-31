/* =========================================================================================
   Module 16c – Environment: Waste (Finalize)
   - Exclude "Unit / Einheit" rows from final facts
   - Stored procedure: core.sp_refresh_waste_yearly  (MERGE from final view)
   - Index checks for core.waste_yearly
   - Executes the proc and peeks a few rows for the run summary
   Prereqs:
     - Views from 16/16b already created (core.v_waste_yearly*, marts, etc.)
     - Table core.waste_yearly created by 16b; if not, create it here.
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) (Re)create filtered + final views with "unit-like" exclusion
---------------------------------------------------------------------------------------------

-- Tagging: add is_unit_like for labels such as "Unit", "Units", "Einheit(en)"
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
    CASE WHEN CHARINDEX('%', s.lbl_u) > 0
           OR s.lbl_u LIKE '%SHARE%' OR s.lbl_u LIKE N'%ANTEIL%'
           OR s.lbl_u LIKE '%RATE%'  OR s.lbl_u LIKE N'%QUOTE%'
         THEN 1 ELSE 0 END                                         AS is_rate_like,
    CASE WHEN s.lbl_u LIKE '%/M2%' OR s.lbl_u LIKE '% PER M2%' OR s.lbl_u LIKE N'%M²%'
           OR s.lbl_u LIKE '%/UNIT%' OR s.lbl_u LIKE '% PER UNIT%' OR s.lbl_u LIKE N'%PRO EINHEIT%'
           OR s.lbl_u LIKE '%KG/M2%' OR s.lbl_u LIKE '%T/M2%' OR s.lbl_u LIKE '%KG/FTE%'
           OR s.lbl_u LIKE '%INTENS%'
         THEN 1 ELSE 0 END                                         AS is_intensity_like,
    CASE WHEN s.lbl_u LIKE '%INDEX%' THEN 1 ELSE 0 END             AS is_index_like,
    CASE WHEN s.lbl_u LIKE '%TOTAL%' OR s.lbl_u LIKE N'%SUMME%' THEN 1 ELSE 0 END AS is_total_like,
    CASE WHEN s.lbl_u LIKE '%NOTE%' OR s.lbl_u LIKE '%COMMENT%' OR s.lbl_u LIKE N'%KOMMENTAR%'
         THEN 1 ELSE 0 END                                         AS is_note_like,
    -- NEW: unit-like meta rows
    CASE WHEN s.lbl_u IN (N'UNIT', N'UNITS', N'EINHEIT', N'EINHEITEN')
           OR s.lbl_u LIKE N'% UNIT' OR s.lbl_u LIKE N'UNIT %' OR s.lbl_u LIKE N'% EINHEIT%'
         THEN 1 ELSE 0 END                                         AS is_unit_like
FROM src s;
GO

-- Filter: keep tangible tonnes; drop rate/intensity/index/totals/units; cap magnitudes
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
    CASE WHEN t.value BETWEEN 0 AND 1000000 THEN t.value END AS value,  -- 0..1,000,000 t
    t.derived_unit
FROM core.v_waste_yearly_tagged t
WHERE t.derived_unit = N't'
  AND t.is_rate_like      = 0
  AND t.is_intensity_like = 0
  AND t.is_index_like     = 0
  AND t.is_total_like     = 0
  AND t.is_unit_like      = 0;
GO

-- Final: natural-key collapse
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
-- 2) Ensure table + indexes
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
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('core.waste_yearly') AND name='IX_waste_yearly_year')
    CREATE INDEX IX_waste_yearly_year ON core.waste_yearly(year);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('core.waste_yearly') AND name='IX_waste_yearly_sheet')
    CREATE INDEX IX_waste_yearly_sheet ON core.waste_yearly(sheet_name, year);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('core.waste_yearly') AND name='IX_waste_yearly_key_incl')
    CREATE INDEX IX_waste_yearly_key_incl ON core.waste_yearly(sheet_name, year, row_label) INCLUDE(value, derived_unit);

---------------------------------------------------------------------------------------------
-- 3) Proc: refresh via MERGE from final view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_waste_yearly','P') IS NULL
    EXEC ('CREATE PROC core.sp_refresh_waste_yearly AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC core.sp_refresh_waste_yearly
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ch TABLE(action nvarchar(10));

    ;WITH src AS (SELECT * FROM core.v_waste_yearly_final)
    MERGE core.waste_yearly AS tgt
    USING src
       ON tgt.sheet_name = src.sheet_name
      AND tgt.year       = src.year
      AND ISNULL(tgt.row_label,'') = ISNULL(src.row_label,'')
    WHEN MATCHED AND (
           ISNULL(tgt.value, 0)        <> ISNULL(src.value, 0)
        OR ISNULL(tgt.derived_unit,'') <> ISNULL(src.derived_unit,'')
        OR ISNULL(tgt.category,'')     <> ISNULL(src.category,'')
        OR ISNULL(tgt.subcategory,'')  <> ISNULL(src.subcategory,'')
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
    OUTPUT $action INTO @ch;

    SELECT
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END), 0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END), 0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END), 0)
    FROM @ch;
END
GO

---------------------------------------------------------------------------------------------
-- 4) Execute once + peek
---------------------------------------------------------------------------------------------
EXEC core.sp_refresh_waste_yearly;

SELECT TOP (15)
    year, sheet_name, LEFT(row_label, 80) AS row_label, value, derived_unit
FROM core.waste_yearly
ORDER BY year DESC, sheet_name, row_label;
