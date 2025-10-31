USE [Vonovia_ESG_DB];
GO
/* File: 13 Admin – refresh pipeline sprocs.sql
   Purpose:
     - Provide one-button stored procedures to (re)build materialized facts and marts
     - Encapsulate the MERGE steps already defined in the views/scripts
   Depends on:
     - core.v_ghg_balance_yearly
     - core.v_ghg_balance_yearly_country
     - core.v_ghg_balance_tagged
     - mart views defined in module 11
*/

------------------------------------------------------------
-- Helper: (Re)materialize yearly totals from view -> table
------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_ghg_yearly','P') IS NULL EXEC('CREATE PROC core.sp_refresh_ghg_yearly AS RETURN;');
GO
ALTER PROC core.sp_refresh_ghg_yearly
AS
BEGIN
  SET NOCOUNT ON;

  -- Ensure table exists (same definition as module 7)
  IF OBJECT_ID('core.ghg_balance_yearly','U') IS NULL
  BEGIN
    CREATE TABLE core.ghg_balance_yearly
    (
        year_label int NOT NULL,
        [year] int NOT NULL,
        row_num int NOT NULL,
        label nvarchar(400) NOT NULL,
        unit nvarchar(100) NULL,
        value_text nvarchar(200) NULL,
        value_num decimal(38,4) NULL,
        CONSTRAINT PK_core_ghg_balance_yearly PRIMARY KEY (year_label, [year], row_num, label)
    );
  END;

  ;WITH src AS (SELECT
                  year_label, [year], row_num, label,
                  derived_unit AS unit, value_text, value_num
                FROM core.v_ghg_balance_yearly)
  MERGE core.ghg_balance_yearly AS t
  USING src AS s
  ON (t.year_label = s.year_label AND t.[year] = s.[year] AND t.row_num = s.row_num AND t.label = s.label)
  WHEN MATCHED AND (ISNULL(t.value_num, -999999) <> ISNULL(s.value_num, -999999)
                 OR ISNULL(t.value_text, N'') <> ISNULL(s.value_text, N'')
                 OR ISNULL(t.unit, N'') <> ISNULL(s.unit, N''))
      THEN UPDATE SET t.unit = s.unit, t.value_text = s.value_text, t.value_num = s.value_num
  WHEN NOT MATCHED BY TARGET
      THEN INSERT (year_label, [year], row_num, label, unit, value_text, value_num)
           VALUES (s.year_label, s.[year], s.row_num, s.label, s.unit, s.value_text, s.value_num)
  WHEN NOT MATCHED BY SOURCE THEN DELETE;
END
GO

------------------------------------------------------------
-- Helper: (Re)materialize yearly-by-country from view -> table
------------------------------------------------------------
IF OBJECT_ID('core.sp_refresh_ghg_yearly_country','P') IS NULL EXEC('CREATE PROC core.sp_refresh_ghg_yearly_country AS RETURN;');
GO
ALTER PROC core.sp_refresh_ghg_yearly_country
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('core.ghg_balance_yearly_country','U') IS NULL
  BEGIN
    CREATE TABLE core.ghg_balance_yearly_country
    (
        year_label int NOT NULL,
        [year] int NOT NULL,
        row_num int NOT NULL,
        label nvarchar(400) NOT NULL,
        unit nvarchar(100) NULL,
        country_name nvarchar(100) NOT NULL,
        iso2 char(2) NULL,
        value_text nvarchar(200) NULL,
        value_num decimal(38,4) NULL,
        CONSTRAINT PK_core_ghg_balance_yearly_country PRIMARY KEY (year_label, [year], row_num, country_name)
    );
  END;

  ;WITH src AS (SELECT * FROM core.v_ghg_balance_yearly_country)
  MERGE core.ghg_balance_yearly_country AS t
  USING src AS s
  ON (t.year_label = s.year_label AND t.[year] = s.[year] AND t.row_num = s.row_num AND t.country_name = s.country_name)
  WHEN MATCHED AND (ISNULL(t.value_num, -999999) <> ISNULL(s.value_num, -999999)
                 OR ISNULL(t.value_text, N'') <> ISNULL(s.value_text, N'')
                 OR ISNULL(t.unit, N'') <> ISNULL(s.unit, N'')
                 OR ISNULL(t.iso2, N'') <> ISNULL(s.iso2, N''))
      THEN UPDATE SET t.unit = s.unit, t.value_text = s.value_text, t.value_num = s.value_num, t.iso2 = s.iso2
  WHEN NOT MATCHED BY TARGET
      THEN INSERT (year_label, [year], row_num, label, unit, country_name, iso2, value_text, value_num)
           VALUES (s.year_label, s.[year], s.row_num, s.label, s.unit, s.country_name, s.iso2, s.value_text, s.value_num)
  WHEN NOT MATCHED BY SOURCE THEN DELETE;
END
GO

------------------------------------------------------------
-- Orchestrator: refresh everything in order
------------------------------------------------------------
IF OBJECT_ID('mart.sp_refresh_all','P') IS NULL EXEC('CREATE PROC mart.sp_refresh_all AS RETURN;');
GO
ALTER PROC mart.sp_refresh_all
AS
BEGIN
  SET NOCOUNT ON;

  -- 1) Facts (materialized)
  EXEC core.sp_refresh_ghg_yearly;
  EXEC core.sp_refresh_ghg_yearly_country;

  -- 2) Dims (module 11 logic)
  MERGE core.d_year AS t
  USING (SELECT DISTINCT [year] FROM core.v_ghg_balance_tagged WHERE [year] IS NOT NULL) s([year])
  ON t.[year]=s.[year]
  WHEN NOT MATCHED BY TARGET THEN INSERT([year]) VALUES(s.[year]);

  MERGE core.d_country AS t
  USING (SELECT DISTINCT iso2, country_name FROM core.ghg_balance_yearly_country WHERE iso2 IS NOT NULL) s(iso2,name)
  ON t.iso2=s.iso2
  WHEN NOT MATCHED BY TARGET THEN INSERT(iso2,name) VALUES(s.iso2,s.name);

  MERGE core.d_scope AS t
  USING (VALUES (1,N'Scope 1'),(2,N'Scope 2'),(3,N'Scope 3')) s(scope_id,scope_name)
  ON t.scope_id=s.scope_id
  WHEN NOT MATCHED BY TARGET THEN INSERT(scope_id,scope_name) VALUES(s.scope_id,s.scope_name);

  MERGE core.d_segment AS t
  USING (VALUES (1,N'Portfolio'),(2,N'Business operations')) s(segment_id,segment_name)
  ON t.segment_id=s.segment_id
  WHEN NOT MATCHED BY TARGET THEN INSERT(segment_id,segment_name) VALUES(s.segment_id,s.segment_name);

  -- 3) No action needed for mart views—they read live.

  SELECT 'OK' AS status, SYSDATETIME() AS refreshed_at;
END
GO
