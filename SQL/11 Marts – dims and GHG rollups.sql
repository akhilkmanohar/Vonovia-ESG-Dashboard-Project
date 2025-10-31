USE [Vonovia_ESG_DB];
GO
/* File: 11 Marts – dims and GHG rollups.sql
   Purpose:
     - Build minimal dimension tables (year, country, scope, segment)
     - Create mart views ready for Power BI:
         mart.v_ghg_total_by_year
         mart.v_ghg_by_country_year
         mart.v_ghg_by_scope_year
     - Add helpful indexes on materialized facts
   Depends on:
     - core.v_ghg_balance_tagged
     - core.ghg_balance_yearly_country
*/

------------------------------------------------------------
-- 0) Ensure mart schema
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

------------------------------------------------------------
-- 1) Dimensions (minimal, idempotent MERGE)
------------------------------------------------------------
IF OBJECT_ID('core.d_year','U') IS NULL
  CREATE TABLE core.d_year([year] int NOT NULL PRIMARY KEY);
MERGE core.d_year AS t
USING (SELECT DISTINCT [year] FROM core.v_ghg_balance_tagged WHERE [year] IS NOT NULL) s([year])
ON t.[year]=s.[year]
WHEN NOT MATCHED BY TARGET THEN INSERT([year]) VALUES(s.[year]);

IF OBJECT_ID('core.d_country','U') IS NULL
  CREATE TABLE core.d_country(iso2 char(2) NOT NULL PRIMARY KEY, name nvarchar(100) NOT NULL);
MERGE core.d_country AS t
USING (SELECT DISTINCT iso2, country_name FROM core.ghg_balance_yearly_country WHERE iso2 IS NOT NULL) s(iso2,name)
ON t.iso2=s.iso2
WHEN NOT MATCHED BY TARGET THEN INSERT(iso2,name) VALUES(s.iso2,s.name);

IF OBJECT_ID('core.d_scope','U') IS NULL
  CREATE TABLE core.d_scope(scope_id tinyint NOT NULL PRIMARY KEY, scope_name nvarchar(20) NOT NULL);
MERGE core.d_scope AS t
USING (VALUES (1,N'Scope 1'),(2,N'Scope 2'),(3,N'Scope 3')) s(scope_id,scope_name)
ON t.scope_id=s.scope_id
WHEN NOT MATCHED BY TARGET THEN INSERT(scope_id,scope_name) VALUES(s.scope_id,s.scope_name);

IF OBJECT_ID('core.d_segment','U') IS NULL
  CREATE TABLE core.d_segment(segment_id int NOT NULL PRIMARY KEY, segment_name nvarchar(40) NOT NULL);
MERGE core.d_segment AS t
USING (VALUES (1,N'Portfolio'),(2,N'Business operations')) s(segment_id,segment_name)
ON t.segment_id=s.segment_id
WHEN NOT MATCHED BY TARGET THEN INSERT(segment_id,segment_name) VALUES(s.segment_id,s.segment_name);
GO

------------------------------------------------------------
-- 2) Helpful indexes on materialized facts
------------------------------------------------------------
IF OBJECT_ID('core.ghg_balance_yearly','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ghg_year_row' AND object_id=OBJECT_ID('core.ghg_balance_yearly'))
    CREATE INDEX IX_ghg_year_row ON core.ghg_balance_yearly([year], row_num);

IF OBJECT_ID('core.ghg_balance_yearly_country','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ghg_year_country' AND object_id=OBJECT_ID('core.ghg_balance_yearly_country'))
    CREATE INDEX IX_ghg_year_country ON core.ghg_balance_yearly_country([year], country_name);
GO

------------------------------------------------------------
-- 3) Mart views for Power BI
------------------------------------------------------------
CREATE OR ALTER VIEW mart.v_ghg_total_by_year
AS
SELECT
  y.[year],
  SUM(y.value_num) AS total_value
FROM core.v_ghg_balance_tagged AS y
WHERE ISNULL(y.is_total,0)=1
GROUP BY y.[year];
GO

CREATE OR ALTER VIEW mart.v_ghg_by_country_year
AS
SELECT
  f.[year],
  f.iso2,
  f.country_name,
  SUM(f.value_num) AS value_sum
FROM core.ghg_balance_yearly_country AS f
GROUP BY f.[year], f.iso2, f.country_name;
GO

CREATE OR ALTER VIEW mart.v_ghg_by_scope_year
AS
SELECT
  t.[year],
  t.scope,
  SUM(t.value_num) AS value_sum
FROM core.v_ghg_balance_tagged AS t
WHERE t.scope IS NOT NULL
GROUP BY t.[year], t.scope;
GO
