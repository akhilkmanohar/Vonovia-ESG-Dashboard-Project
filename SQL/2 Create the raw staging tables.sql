USE [Vonovia_ESG_DB];
GO

/* Generic helper: we’ll use the same shape for all years */
IF OBJECT_ID('stg.raw_fb24_all','U') IS NULL
BEGIN
    CREATE TABLE stg.raw_fb24_all
    (
        row_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        sheet_name   NVARCHAR(128) NOT NULL,
        row_num      INT           NOT NULL,       -- 1-based row order from Excel
        c01          NVARCHAR(4000) NULL,
        c02          NVARCHAR(4000) NULL,
        c03          NVARCHAR(4000) NULL,
        c04          NVARCHAR(4000) NULL,
        c05          NVARCHAR(4000) NULL,
        c06          NVARCHAR(4000) NULL,
        c07          NVARCHAR(4000) NULL,
        c08          NVARCHAR(4000) NULL,
        c09          NVARCHAR(4000) NULL,
        c10          NVARCHAR(4000) NULL,
        c11          NVARCHAR(4000) NULL,
        c12          NVARCHAR(4000) NULL,
        c13          NVARCHAR(4000) NULL,
        c14          NVARCHAR(4000) NULL,
        c15          NVARCHAR(4000) NULL,
        c16          NVARCHAR(4000) NULL,
        c17          NVARCHAR(4000) NULL,
        c18          NVARCHAR(4000) NULL,
        c19          NVARCHAR(4000) NULL,
        c20          NVARCHAR(4000) NULL,
        c21          NVARCHAR(4000) NULL,
        c22          NVARCHAR(4000) NULL,
        c23          NVARCHAR(4000) NULL,
        c24          NVARCHAR(4000) NULL,
        c25          NVARCHAR(4000) NULL,
        c26          NVARCHAR(4000) NULL,
        c27          NVARCHAR(4000) NULL,
        c28          NVARCHAR(4000) NULL,
        source_file  NVARCHAR(260) NOT NULL,
        load_ts      DATETIME2(3)  NOT NULL CONSTRAINT DF_stg_fb24_loadts DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_stg_fb24_sheet_row ON stg.raw_fb24_all(sheet_name, row_num);
END
GO

IF OBJECT_ID('stg.raw_fb23_all','U') IS NULL
BEGIN
    CREATE TABLE stg.raw_fb23_all
    (
        row_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        sheet_name   NVARCHAR(128) NOT NULL,
        row_num      INT           NOT NULL,
        c01 NVARCHAR(4000) NULL, c02 NVARCHAR(4000) NULL, c03 NVARCHAR(4000) NULL, c04 NVARCHAR(4000) NULL,
        c05 NVARCHAR(4000) NULL, c06 NVARCHAR(4000) NULL, c07 NVARCHAR(4000) NULL, c08 NVARCHAR(4000) NULL,
        c09 NVARCHAR(4000) NULL, c10 NVARCHAR(4000) NULL, c11 NVARCHAR(4000) NULL, c12 NVARCHAR(4000) NULL,
        c13 NVARCHAR(4000) NULL, c14 NVARCHAR(4000) NULL, c15 NVARCHAR(4000) NULL, c16 NVARCHAR(4000) NULL,
        c17 NVARCHAR(4000) NULL, c18 NVARCHAR(4000) NULL, c19 NVARCHAR(4000) NULL, c20 NVARCHAR(4000) NULL,
        c21 NVARCHAR(4000) NULL, c22 NVARCHAR(4000) NULL, c23 NVARCHAR(4000) NULL, c24 NVARCHAR(4000) NULL,
        c25 NVARCHAR(4000) NULL, c26 NVARCHAR(4000) NULL, c27 NVARCHAR(4000) NULL, c28 NVARCHAR(4000) NULL,
        source_file  NVARCHAR(260) NOT NULL,
        load_ts      DATETIME2(3)  NOT NULL CONSTRAINT DF_stg_fb23_loadts DEFAULT (SYSUTCDATETIME())
    );
    CREATE INDEX IX_stg_fb23_sheet_row ON stg.raw_fb23_all(sheet_name, row_num);
END
GO

IF OBJECT_ID('stg.raw_sr22_all','U') IS NULL
BEGIN
    CREATE TABLE stg.raw_sr22_all
    (
        row_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        sheet_name   NVARCHAR(128) NOT NULL,
        row_num      INT           NOT NULL,
        c01 NVARCHAR(4000) NULL, c02 NVARCHAR(4000) NULL, c03 NVARCHAR(4000) NULL, c04 NVARCHAR(4000) NULL,
        c05 NVARCHAR(4000) NULL, c06 NVARCHAR(4000) NULL, c07 NVARCHAR(4000) NULL, c08 NVARCHAR(4000) NULL,
        c09 NVARCHAR(4000) NULL, c10 NVARCHAR(4000) NULL, c11 NVARCHAR(4000) NULL, c12 NVARCHAR(4000) NULL,
        c13 NVARCHAR(4000) NULL, c14 NVARCHAR(4000) NULL, c15 NVARCHAR(4000) NULL, c16 NVARCHAR(4000) NULL,
        c17 NVARCHAR(4000) NULL, c18 NVARCHAR(4000) NULL, c19 NVARCHAR(4000) NULL, c20 NVARCHAR(4000) NULL,
        c21 NVARCHAR(4000) NULL, c22 NVARCHAR(4000) NULL, c23 NVARCHAR(4000) NULL, c24 NVARCHAR(4000) NULL,
        c25 NVARCHAR(4000) NULL, c26 NVARCHAR(4000) NULL, c27 NVARCHAR(4000) NULL, c28 NVARCHAR(4000) NULL,
        source_file  NVARCHAR(260) NOT NULL,
        load_ts      DATETIME2(3)  NOT NULL CONSTRAINT DF_stg_sr22_loadts DEFAULT (SYSUTCDATETIME())
    );
    CREATE INDEX IX_stg_sr22_sheet_row ON stg.raw_sr22_all(sheet_name, row_num);
END
GO
