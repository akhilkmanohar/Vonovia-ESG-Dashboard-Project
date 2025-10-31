/* =========================================================================================
   Module 14e – Environment: Energy Consumption
   FINAL FILTERED LAYER + PROC UPDATE
   - New view core.v_energy_yearly_final: filters to tangible energy amounts (MWh), excludes
     rate/intensity/index rows, caps magnitudes, and DEDUPs by (sheet_name, year, row_label).
   - Update core.sp_refresh_energy_yearly to MERGE from the final view (instead of raw).
   - Leaves earlier marts intact; they will align once the table mirrors filtered facts.
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Final filtered + deduped yearly view (MWh only)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_yearly_final','V') IS NOT NULL
    DROP VIEW core.v_energy_yearly_final;
GO
CREATE VIEW core.v_energy_yearly_final
AS
WITH src AS (
    SELECT
        t.sheet_name,
        t.category,
        t.subcategory,
        t.year,
        t.row_label,
        t.value,
        t.derived_unit,
        t.is_rate_like,
        t.is_intensity_like,
        t.is_index_like,
        UPPER(COALESCE(t.row_label,N'')) AS lbl_u
    FROM core.v_energy_yearly_tagged t
),
filtered AS (
    SELECT *
    FROM src
    WHERE derived_unit = N'MWh'
      AND is_rate_like     = 0
      AND is_intensity_like= 0
      AND is_index_like    = 0
      -- safety bounds for plausible annual energy values (0 .. 100 million MWh)
      AND value BETWEEN 0 AND 100000000
)
SELECT
    f.sheet_name,
    MIN(f.category)    AS category,
    MIN(f.subcategory) AS subcategory,
    f.year,
    f.row_label,
    SUM(f.value)       AS value,
    CAST(N'MWh' AS nvarchar(50)) AS derived_unit
FROM filtered f
GROUP BY
    f.sheet_name, f.year, f.row_label;
GO

---------------------------------------------------------------------------------------------
-- 2) Update refresh proc to use the FINAL view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_energy_yearly','P') IS NULL
    EXEC ('CREATE PROC core.sp_refresh_energy_yearly AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC core.sp_refresh_energy_yearly
AS
BEGIN
    SET NOCOUNT ON;

    -- Remove existing duplicate natural keys to avoid MERGE conflicts
    ;WITH dupe AS (
        SELECT
            energy_yearly_id,
            ROW_NUMBER() OVER (
                PARTITION BY sheet_name, year, row_label
                ORDER BY load_dts DESC, energy_yearly_id DESC
            ) AS rn
        FROM core.energy_yearly
    )
    DELETE FROM dupe WHERE rn > 1;

    DECLARE @ch TABLE(action nvarchar(10));

    MERGE core.energy_yearly AS tgt
    USING (SELECT * FROM core.v_energy_yearly_final) AS src
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
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END),0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END),0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END),0)
    FROM @ch;
END
GO

-- Execute once to materialize filtered data
EXEC core.sp_refresh_energy_yearly;

-- Peek
SELECT TOP (20) year, sheet_name, LEFT(row_label,80) AS row_label, value, derived_unit
FROM core.energy_yearly
ORDER BY year DESC, sheet_name, row_label;
