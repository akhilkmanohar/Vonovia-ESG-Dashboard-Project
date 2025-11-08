SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.v_social_discovery','V') IS NULL
BEGIN
    RAISERROR('Missing core.v_social_discovery (run Social S1 first).', 16, 1);
    RETURN;
END
GO

CREATE OR ALTER VIEW core.v_social_training_tagged
AS
SELECT
    [year]        = d.year_guess,
    d.label_raw,
    d.label_norm,
    value_num     = TRY_CONVERT(decimal(38,6), d.value_pref),
    category_norm = d.category_norm,

    is_percent_like = CASE
        WHEN d.label_norm LIKE '%[%]%' OR d.label_norm LIKE '% percent%' OR d.label_norm LIKE '%prozent%'
          OR d.label_norm LIKE '%quote%' OR d.label_norm LIKE '%share%'
        THEN 1 ELSE 0 END,

    is_training = CASE
        WHEN d.label_norm LIKE '%training%' OR d.label_norm LIKE '%trainings%' OR d.label_norm LIKE '%trainingsstunden%'
          OR d.label_norm LIKE '%schulung%' OR d.label_norm LIKE '%schulungen%' OR d.label_norm LIKE '%weiterbildung%'
          OR d.label_norm LIKE '%fortbildung%' OR d.label_norm LIKE '%seminar%' OR d.label_norm LIKE '%course%'
        THEN 1 ELSE 0 END,

    metric_type = CASE
        WHEN (d.label_norm LIKE '%hours per employee%' OR d.label_norm LIKE '%average hours%'
              OR d.label_norm LIKE '%hours/employee%' OR d.label_norm LIKE '%stunden/mitarbeiter%'
              OR d.label_norm LIKE '%pro mitarbeiter%' OR d.label_norm LIKE '%per employee%' OR d.label_norm LIKE '%per fte%')
             AND (d.label_norm LIKE '%training%' OR d.label_norm LIKE '%schulung%' OR d.label_norm LIKE '%weiterbildung%' OR d.label_norm LIKE '%fortbildung%')
          THEN 'hours_per_employee'
        WHEN (d.label_norm LIKE '%hour%' OR d.label_norm LIKE '%hours%' OR d.label_norm LIKE '%stunde%' OR d.label_norm LIKE '%stunden%'
              OR d.label_norm LIKE '%trainingsstunden%')
             AND (d.label_norm LIKE '%training%' OR d.label_norm LIKE '%schulung%' OR d.label_norm LIKE '%weiterbildung%' OR d.label_norm LIKE '%fortbildung%')
          THEN 'hours_total'
        WHEN d.label_norm LIKE '%teilnehmer%' OR d.label_norm LIKE '%teilnehmende%' OR d.label_norm LIKE '%participants%' OR d.label_norm LIKE '%people trained%'
          THEN 'participants_total'
        ELSE 'other' END
FROM core.v_social_discovery d;
GO

CREATE OR ALTER VIEW core.v_social_training_filtered
AS
SELECT
    [year],
    label_raw,
    label_norm,
    value_num,
    metric_type,
    metric_group = CASE
        WHEN metric_type IN ('hours_total','participants_total') THEN 'count'
        WHEN metric_type IN ('hours_per_employee') THEN 'rate'
        ELSE 'other' END
FROM core.v_social_training_tagged
WHERE is_training = 1
  AND value_num IS NOT NULL AND value_num > 0
  AND [year] IS NOT NULL
  AND (metric_type <> 'hours_total' OR is_percent_like = 0);
GO

CREATE OR ALTER VIEW core.v_social_training_yearly
AS
WITH counts AS (
    SELECT [year], metric = metric_type, value_num = SUM(value_num)
    FROM core.v_social_training_filtered
    WHERE metric_group = 'count'
    GROUP BY [year], metric_type
), rates AS (
    SELECT [year], metric = metric_type, value_num = AVG(value_num)
    FROM core.v_social_training_filtered
    WHERE metric_group = 'rate'
    GROUP BY [year], metric_type
)
SELECT * FROM counts
UNION ALL
SELECT * FROM rates;
GO

CREATE OR ALTER VIEW mart.v_training_by_year
AS
SELECT [year], metric, value_num
FROM core.v_social_training_yearly;
GO

SELECT TOP (12) * FROM mart.v_training_by_year ORDER BY [year] DESC, metric;
