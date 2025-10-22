

USE [Vonovia_ESG_DB];
GO

IF OBJECT_ID('core.sheet_catalog','U') IS NULL
BEGIN
  CREATE TABLE core.sheet_catalog
  (
      sheet_id     INT IDENTITY(1,1) PRIMARY KEY,
      year_label   CHAR(4)         NOT NULL,  -- '2024','2023','2022'
      sheet_name   NVARCHAR(128)   NOT NULL,  -- as seen in the All_Tables workbook
      category     NVARCHAR(16)    NOT NULL,  -- 'Environment'|'Social'|'Governance'|'Unknown'
      source_file  NVARCHAR(260)   NOT NULL,  -- the Key_Figures* workbook the tag came from, or 'manual'
      created_at   DATETIME2(3)    NOT NULL CONSTRAINT DF_sheet_catalog_created DEFAULT (SYSDATETIME()),
      updated_at   DATETIME2(3)    NULL
  );
  CREATE UNIQUE INDEX UX_sheet_catalog_y_s ON core.sheet_catalog(year_label, sheet_name);
END
GO

IF OBJECT_ID('core.trg_sheet_catalog_touch','TR') IS NOT NULL DROP TRIGGER core.trg_sheet_catalog_touch;
GO
CREATE TRIGGER core.trg_sheet_catalog_touch
ON core.sheet_catalog
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  UPDATE c SET updated_at = SYSDATETIME()
  FROM core.sheet_catalog c JOIN inserted i ON i.sheet_id = c.sheet_id;
END
GO
