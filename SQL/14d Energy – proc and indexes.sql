/* =========================================================================================
   Module 14d – Environment: Energy Consumption (Finalize)
   - Stored procedure: core.sp_refresh_energy_yearly
   - Index checks for core.energy_yearly
   - Executes the proc and peeks a few rows for the run summary
   Prereqs:
     - Views from 14/14b/14c already created (core.v_energy_yearly, etc.)
     - Table core.energy_yearly already created by earlier module; if not, create it here.
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- Ensure materialized table and indexes exist
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.energy_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.energy_yearly
    (
        energy_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
        sheet_name   nvarchar(255) NOT NULL,
        category     nvarchar(255) NULL,
        subcategory  nvarchar(255) NULL,
        year         int NOT NULL,
        row_label    nvarchar(1000) NULL,
        value        decimal(38,6) NULL,
        derived_unit nvarchar(50) NOT NULL,
        load_dts     datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
;

-- Index: by year
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.energy_yearly')
      AND name = 'IX_energy_yearly_year'
)
BEGIN
    CREATE INDEX IX_energy_yearly_year ON core.energy_yearly(year);
END;

-- Index: by sheet/year
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.energy_yearly')
      AND name = 'IX_energy_yearly_sheet'
)
BEGIN
    CREATE INDEX IX_energy_yearly_sheet ON core.energy_yearly(sheet_name, year);
END;

-- Covering index for lookups by natural key
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.energy_yearly')
      AND name = 'IX_energy_yearly_key_incl'
)
BEGIN
    CREATE INDEX IX_energy_yearly_key_incl
    ON core.energy_yearly(sheet_name, year, row_label)
    INCLUDE (value, derived_unit);
END;

---------------------------------------------------------------------------------------------
-- Stored procedure to refresh core.energy_yearly via MERGE from the refined view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_energy_yearly','P') IS NULL
    EXEC ('CREATE PROC core.sp_refresh_energy_yearly AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC core.sp_refresh_energy_yearly
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ch TABLE(action nvarchar(10));

    ;WITH src AS (
        SELECT * FROM core.v_energy_yearly
    )
    MERGE core.energy_yearly AS tgt
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
    OUTPUT $action INTO @ch;

    SELECT
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END),0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END),0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END),0)
    FROM @ch;
END
GO

---------------------------------------------------------------------------------------------
-- Execute the refresh now and show a tiny peek for the build summary
---------------------------------------------------------------------------------------------
EXEC core.sp_refresh_energy_yearly;

SELECT TOP (15)
    year, sheet_name, LEFT(row_label, 80) AS row_label, value, derived_unit
FROM core.energy_yearly
ORDER BY year DESC, sheet_name, row_label;
