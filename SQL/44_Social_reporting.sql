SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='rpt')  EXEC('CREATE SCHEMA rpt;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.v_social_workforce_filtered','V') IS NULL OR
   OBJECT_ID('core.v_social_hs_counts_yearly','V') IS NULL OR
   OBJECT_ID('core.v_social_hs_rates_yearly','V') IS NULL OR
   OBJECT_ID('core.v_social_training_yearly','V') IS NULL
BEGIN
    RAISERROR('Missing upstream Social views (ensure S2/S3/S4 modules ran).', 16, 1);
    RETURN;
END
GO

CREATE OR ALTER VIEW rpt.v_social_workforce_wide
AS
WITH src AS (
  SELECT [year], measure, value_num
  FROM mart.v_workforce_headcount_by_year
)
SELECT
  s.[year],
  persons = SUM(CASE WHEN s.measure='Persons' THEN s.value_num END),
  fte     = SUM(CASE WHEN s.measure='FTE'     THEN s.value_num END)
FROM src s
GROUP BY s.[year];
GO

CREATE OR ALTER VIEW rpt.v_social_hs_counts_wide
AS
WITH src AS (
  SELECT [year], metric, value_num
  FROM mart.v_hs_counts_by_year
)
SELECT
  s.[year],
  incidents  = SUM(CASE WHEN metric='incidents'  THEN value_num END),
  lost_days  = SUM(CASE WHEN metric='lost_days'  THEN value_num END),
  fatalities = SUM(CASE WHEN metric='fatalities' THEN value_num END)
FROM src s
GROUP BY s.[year];
GO

CREATE OR ALTER VIEW rpt.v_social_hs_rates_wide
AS
WITH src AS (
  SELECT [year], rate_metric, value_num
  FROM mart.v_hs_rates_by_year
)
SELECT
  s.[year],
  ltir          = AVG(CASE WHEN rate_metric='ltir'          THEN value_num END),
  trir          = AVG(CASE WHEN rate_metric='trir'          THEN value_num END),
  severity_rate = AVG(CASE WHEN rate_metric='severity_rate' THEN value_num END)
FROM src s
GROUP BY s.[year];
GO

CREATE OR ALTER VIEW rpt.v_social_training_wide
AS
WITH src AS (
  SELECT [year], metric, value_num
  FROM mart.v_training_by_year
)
SELECT
  s.[year],
  hours_total         = SUM(CASE WHEN metric='hours_total'         THEN value_num END),
  participants_total  = SUM(CASE WHEN metric='participants_total'  THEN value_num END),
  hours_per_employee  = AVG(CASE WHEN metric='hours_per_employee'  THEN value_num END)
FROM src s
GROUP BY s.[year];
GO

CREATE OR ALTER VIEW rpt.v_social_cards_latest
AS
WITH yr AS (
    SELECT MAX([year]) AS y
    FROM (
        SELECT [year] FROM rpt.v_social_workforce_wide
        UNION SELECT [year] FROM rpt.v_social_hs_counts_wide
        UNION SELECT [year] FROM rpt.v_social_hs_rates_wide
        UNION SELECT [year] FROM rpt.v_social_training_wide
    ) u
)
SELECT 'workforce' AS card, w.[year], 'persons' AS metric, w.persons AS value_num, CAST(NULL AS decimal(38,6)) AS value_pct
FROM rpt.v_social_workforce_wide w CROSS JOIN yr WHERE w.[year]=yr.y
UNION ALL
SELECT 'workforce', w.[year], 'fte', w.fte, NULL FROM rpt.v_social_workforce_wide w CROSS JOIN yr WHERE w.[year]=yr.y
UNION ALL
SELECT 'hs_counts', c.[year], 'incidents', c.incidents, NULL FROM rpt.v_social_hs_counts_wide c CROSS JOIN yr WHERE c.[year]=yr.y
UNION ALL
SELECT 'hs_counts', c.[year], 'lost_days', c.lost_days, NULL FROM rpt.v_social_hs_counts_wide c CROSS JOIN yr WHERE c.[year]=yr.y
UNION ALL
SELECT 'hs_counts', c.[year], 'fatalities', c.fatalities, NULL FROM rpt.v_social_hs_counts_wide c CROSS JOIN yr WHERE c.[year]=yr.y
UNION ALL
SELECT 'hs_rates', r.[year], 'ltir', r.ltir, NULL FROM rpt.v_social_hs_rates_wide r CROSS JOIN yr WHERE r.[year]=yr.y
UNION ALL
SELECT 'hs_rates', r.[year], 'trir', r.trir, NULL FROM rpt.v_social_hs_rates_wide r CROSS JOIN yr WHERE r.[year]=yr.y
UNION ALL
SELECT 'hs_rates', r.[year], 'severity_rate', r.severity_rate, NULL FROM rpt.v_social_hs_rates_wide r CROSS JOIN yr WHERE r.[year]=yr.y
UNION ALL
SELECT 'training', t.[year], 'hours_total', t.hours_total, NULL FROM rpt.v_social_training_wide t CROSS JOIN yr WHERE t.[year]=yr.y
UNION ALL
SELECT 'training', t.[year], 'participants_total', t.participants_total, NULL FROM rpt.v_social_training_wide t CROSS JOIN yr WHERE t.[year]=yr.y
UNION ALL
SELECT 'training', t.[year], 'hours_per_employee', t.hours_per_employee, NULL FROM rpt.v_social_training_wide t CROSS JOIN yr WHERE t.[year]=yr.y;
GO

CREATE OR ALTER VIEW rpt.v_social_import_catalog
AS
SELECT * FROM (VALUES
  (N'rpt', N'v_social_workforce_wide', N'Workforce headcount (Persons/FTE)'),
  (N'rpt', N'v_social_hs_counts_wide', N'H&S counts (incidents/lost days/fatalities)'),
  (N'rpt', N'v_social_hs_rates_wide',  N'H&S rates (LTIR/TRIR/severity)'),
  (N'rpt', N'v_social_training_wide',  N'Training (hours, participants, hours per employee)'),
  (N'rpt', N'v_social_cards_latest',   N'Cards: latest Social KPIs')
) AS x(object_schema, object_name, purpose);
GO

CREATE OR ALTER PROCEDURE mart.sp_refresh_social_reporting
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @views TABLE(schema_name sysname, view_name sysname);
  INSERT INTO @views VALUES
    (N'rpt',N'v_social_workforce_wide'),
    (N'rpt',N'v_social_hs_counts_wide'),
    (N'rpt',N'v_social_hs_rates_wide'),
    (N'rpt',N'v_social_training_wide'),
    (N'rpt',N'v_social_cards_latest'),
    (N'rpt',N'v_social_import_catalog');

  DECLARE @s sysname, @v sysname, @fq nvarchar(400);
  DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT schema_name, view_name FROM @views;
  OPEN c; FETCH NEXT FROM c INTO @s, @v;
  DECLARE @refreshed int = 0;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    IF OBJECT_ID(QUOTENAME(@s)+'.'+QUOTENAME(@v),'V') IS NOT NULL
    BEGIN
      SET @fq = QUOTENAME(@s)+'.'+QUOTENAME(@v);
      EXEC sys.sp_refreshview @fq;
      SET @refreshed += 1;
    END
    FETCH NEXT FROM c INTO @s, @v;
  END
  CLOSE c; DEALLOCATE c;
  PRINT CONCAT('sp_refresh_social_reporting refreshed views: ', @refreshed);
END;
GO

EXEC mart.sp_refresh_social_reporting;
SELECT TOP (5) * FROM rpt.v_social_workforce_wide ORDER BY [year] DESC;
SELECT TOP (5) * FROM rpt.v_social_hs_counts_wide  ORDER BY [year] DESC;
SELECT TOP (5) * FROM rpt.v_social_hs_rates_wide   ORDER BY [year] DESC;
SELECT TOP (5) * FROM rpt.v_social_training_wide   ORDER BY [year] DESC;
SELECT TOP (10) * FROM rpt.v_social_cards_latest   ORDER BY card, metric;
