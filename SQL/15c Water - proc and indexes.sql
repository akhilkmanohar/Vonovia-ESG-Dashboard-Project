/* =========================================================================================
   Module 15c – Environment: Water Consumption (Finalize operationalization)
   - Stored procedure: core.sp_refresh_water_yearly
   - Index checks for core.water_yearly
   - Executes the proc and peeks a few rows for the run summary
   Prereqs:
     - Views from 15/15b already created (core.v_water_yearly_final, marts, etc.)
     - Table core.water_yearly already created by 15b; if not, create it here.
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- Ensure materialized table and indexes exist
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.water_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.water_yearly
    (
        water_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
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

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.water_yearly')
      AND name = 'IX_water_yearly_year'
)
BEGIN
    CREATE INDEX IX_water_yearly_year ON core.water_yearly(year);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.water_yearly')
      AND name = 'IX_water_yearly_sheet'
)
BEGIN
    CREATE INDEX IX_water_yearly_sheet ON core.water_yearly(sheet_name, year);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('core.water_yearly')
      AND name = 'IX_water_yearly_key_incl'
)
BEGIN
    CREATE INDEX IX_water_yearly_key_incl
    ON core.water_yearly(sheet_name, year, row_label)
    INCLUDE (value, derived_unit);
END;

---------------------------------------------------------------------------------------------
-- Stored procedure to refresh core.water_yearly via MERGE from the FINAL view
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_water_yearly','P') IS NULL
    EXEC ('CREATE PROC core.sp_refresh_water_yearly AS BEGIN SET NOCOUNT ON; END');
GO
ALTER PROC core.sp_refresh_water_yearly
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ch TABLE(action nvarchar(10));

    ;WITH src AS (
        SELECT * FROM core.v_water_yearly_final
    )
    MERGE core.water_yearly AS tgt
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
        inserted = COALESCE(SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END),0),
        updated  = COALESCE(SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END),0),
        deleted  = COALESCE(SUM(CASE WHEN action='DELETE' THEN 1 ELSE 0 END),0)
    FROM @ch;
END
GO

---------------------------------------------------------------------------------------------
-- Execute the refresh now and show a tiny peek for the build summary
---------------------------------------------------------------------------------------------
EXEC core.sp_refresh_water_yearly;

SELECT TOP (15)
    year, sheet_name, LEFT(row_label, 80) AS row_label, value, derived_unit
FROM core.water_yearly
ORDER BY year DESC, sheet_name, row_label;
