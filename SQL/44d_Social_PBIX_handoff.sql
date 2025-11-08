SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt') EXEC('CREATE SCHEMA rpt');
GO

/* ===== Hand-off wrappers (idempotent) ===== */

-- Workforce (PBIX-ready wide passthrough)
IF OBJECT_ID('rpt.v_social_workforce_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_workforce_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_workforce_wide AS
SELECT CAST([year] AS int) AS [year],
       CAST([measure] AS nvarchar(100)) AS [measure],
       CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_workforce_headcount_by_year;
GO

-- H&S counts
IF OBJECT_ID('rpt.v_social_hs_counts_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_hs_counts_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_hs_counts_wide AS
SELECT CAST([year] AS int) AS [year],
       CAST([metric] AS nvarchar(150)) AS [metric],
       CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_hs_counts_by_year;
GO

-- H&S rates
IF OBJECT_ID('rpt.v_social_hs_rates_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_hs_rates_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_hs_rates_wide AS
SELECT CAST([year] AS int) AS [year],
       CAST([rate_metric] AS nvarchar(150)) AS [metric],
       CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_hs_rates_by_year;
GO

-- Training
IF OBJECT_ID('rpt.v_social_training_wide','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_training_wide AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_training_wide AS
SELECT CAST([year] AS int) AS [year],
       CAST([metric] AS nvarchar(150)) AS [metric],
       CAST([value_num] AS decimal(38,6)) AS [value_num]
FROM mart.v_training_by_year;
GO

/* ===== Latest+Last-5 Cards ===== */

IF OBJECT_ID('rpt.v_social_cards_latest5','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_cards_latest5 AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_cards_latest5 AS
WITH y AS (
    SELECT
      (SELECT MAX([year]) FROM mart.v_workforce_headcount_by_year) AS y_work,
      (SELECT MAX([year]) FROM mart.v_hs_counts_by_year)          AS y_counts,
      (SELECT MAX([year]) FROM mart.v_hs_rates_by_year)           AS y_rates,
      (SELECT MAX([year]) FROM mart.v_training_by_year)           AS y_train
), bounds AS (
    SELECT y_work, y_counts, y_rates, y_train,
           ISNULL(y_work,0)-4   AS y_work_min,
           ISNULL(y_counts,0)-4 AS y_counts_min,
           ISNULL(y_rates,0)-4  AS y_rates_min,
           ISNULL(y_train,0)-4  AS y_train_min
    FROM y
)
SELECT 'workforce' AS section, w.[year], w.[measure] AS label, w.[value_num] AS value_num
FROM mart.v_workforce_headcount_by_year w
CROSS JOIN bounds b
WHERE w.[year] BETWEEN b.y_work_min AND b.y_work
UNION ALL
SELECT 'hs_counts', c.[year], c.[metric], c.[value_num]
FROM mart.v_hs_counts_by_year c
CROSS JOIN bounds b
WHERE c.[year] BETWEEN b.y_counts_min AND b.y_counts
UNION ALL
SELECT 'hs_rates', r.[year], r.[rate_metric], r.[value_num]
FROM mart.v_hs_rates_by_year r
CROSS JOIN bounds b
WHERE r.[year] BETWEEN b.y_rates_min AND b.y_rates
UNION ALL
SELECT 'training', t.[year], t.[metric], t.[value_num]
FROM mart.v_training_by_year t
CROSS JOIN bounds b
WHERE t.[year] BETWEEN b.y_train_min AND b.y_train;
GO

/* ===== Import Catalog for PBIX wiring ===== */

IF OBJECT_ID('rpt.v_social_import_catalog','V') IS NULL
    EXEC('CREATE VIEW rpt.v_social_import_catalog AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW rpt.v_social_import_catalog AS
SELECT v.[schema_id], s.name AS schema_name, v.name AS view_name,
       CASE v.name
           WHEN 'v_social_workforce_wide' THEN 'PBIX table: workforce (wide)'
           WHEN 'v_social_hs_counts_wide'  THEN 'PBIX table: H&S counts (wide)'
           WHEN 'v_social_hs_rates_wide'   THEN 'PBIX table: H&S rates (wide)'
           WHEN 'v_social_training_wide'   THEN 'PBIX table: training (wide)'
           WHEN 'v_social_cards_latest5'   THEN 'PBIX cards: latest + last 5'
           ELSE 'rpt view'
       END AS purpose
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE s.name = 'rpt'
  AND v.name IN (
    'v_social_workforce_wide',
    'v_social_hs_counts_wide',
    'v_social_hs_rates_wide',
    'v_social_training_wide',
    'v_social_cards_latest5'
  );
GO

/* ===== Refresh proc (metadata warm + counts) ===== */

IF OBJECT_ID('rpt.sp_refresh_social_reporting','P') IS NULL
    EXEC('CREATE PROCEDURE rpt.sp_refresh_social_reporting AS SELECT 1;');
GO
ALTER PROCEDURE rpt.sp_refresh_social_reporting
AS
BEGIN
  SET NOCOUNT ON;
  SET LOCK_TIMEOUT 10000;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

  -- Metadata warm with single-thread hint
  SELECT TOP 1 * FROM rpt.v_social_workforce_wide ORDER BY [year] DESC OPTION (MAXDOP 1);
  SELECT TOP 1 * FROM rpt.v_social_hs_counts_wide  ORDER BY [year] DESC OPTION (MAXDOP 1);
  SELECT TOP 1 * FROM rpt.v_social_hs_rates_wide   ORDER BY [year] DESC OPTION (MAXDOP 1);
  SELECT TOP 1 * FROM rpt.v_social_training_wide   ORDER BY [year] DESC OPTION (MAXDOP 1);
  SELECT TOP 1 * FROM rpt.v_social_cards_latest5   ORDER BY section, [year] DESC OPTION (MAXDOP 1);

  -- Row counts for quick diagnostics
  SELECT 'rpt.v_social_workforce_wide' AS view_name, COUNT_BIG(1) AS row_count FROM rpt.v_social_workforce_wide
  UNION ALL SELECT 'rpt.v_social_hs_counts_wide', COUNT_BIG(1) FROM rpt.v_social_hs_counts_wide
  UNION ALL SELECT 'rpt.v_social_hs_rates_wide',  COUNT_BIG(1) FROM rpt.v_social_hs_rates_wide
  UNION ALL SELECT 'rpt.v_social_training_wide',  COUNT_BIG(1) FROM rpt.v_social_training_wide
  UNION ALL SELECT 'rpt.v_social_cards_latest5',  COUNT_BIG(1) FROM rpt.v_social_cards_latest5
  OPTION (MAXDOP 1);
END
GO
