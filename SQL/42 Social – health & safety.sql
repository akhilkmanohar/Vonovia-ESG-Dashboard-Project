SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.v_social_discovery','V') IS NULL
BEGIN
    RAISERROR('Missing core.v_social_discovery (run Module S1 first).', 16, 1);
    RETURN;
END
GO

CREATE OR ALTER VIEW core.v_social_hs_tagged
AS
SELECT
    [year]        = d.year_guess,
    d.label_raw,
    d.label_norm,
    value_num     = TRY_CONVERT(decimal(38,6), d.value_pref),
    category_norm = d.category_norm,

    is_hs = CASE
        WHEN d.label_norm LIKE '%unfall%' OR d.label_norm LIKE '%unfaelle%' OR d.label_norm LIKE '%injury%'
          OR d.label_norm LIKE '%verletzung%' OR d.label_norm LIKE '%vorfall%' OR d.label_norm LIKE '%incident%'
          OR d.label_norm LIKE '%accident%' OR d.label_norm LIKE '%lost day%' OR d.label_norm LIKE '%lost days%'
          OR d.label_norm LIKE '%ausfalltage%' OR d.label_norm LIKE '%verloren%' OR d.label_norm LIKE '%fehltage%'
          OR d.label_norm LIKE '%fatal%' OR d.label_norm LIKE '%todes%' OR d.label_norm LIKE '%ltir%'
          OR d.label_norm LIKE '%ltrir%' OR d.label_norm LIKE '%trir%' OR d.label_norm LIKE '%severity%'
          OR d.label_norm LIKE '%schweregrad%' OR d.label_norm LIKE '%arbeitsunfall%'
        THEN 1 ELSE 0 END,

    metric_type = CASE
        WHEN d.label_norm LIKE '%fatal%' OR d.label_norm LIKE '%todes%' THEN 'fatalities'
        WHEN d.label_norm LIKE '%lost day%' OR d.label_norm LIKE '%lost days%' OR d.label_norm LIKE '%ausfalltage%'
             OR d.label_norm LIKE '%fehltage%' OR d.label_norm LIKE '%verloren%' THEN 'lost_days'
        WHEN d.label_norm LIKE '%ltir%' OR d.label_norm LIKE '%ltrir%' THEN 'ltir'
        WHEN d.label_norm LIKE '%trir%' THEN 'trir'
        WHEN d.label_norm LIKE '%severity%' OR d.label_norm LIKE '%schweregrad%' THEN 'severity_rate'
        WHEN d.label_norm LIKE '%incident%' OR d.label_norm LIKE '%accident%' OR d.label_norm LIKE '%unfall%'
             OR d.label_norm LIKE '%injury%' OR d.label_norm LIKE '%verletzung%' OR d.label_norm LIKE '%vorfall%'
             THEN 'incidents'
        ELSE 'other' END,

    is_percent_like = CASE
        WHEN d.label_norm LIKE '%[%]%' OR d.label_norm LIKE '% rate%' OR d.label_norm LIKE 'rate %'
          OR d.label_norm LIKE '%quote%' OR d.label_norm LIKE '%anteil%' OR d.label_norm LIKE '%percent%'
          OR d.label_norm LIKE '%prozent%' OR d.label_norm LIKE '% per %' OR d.label_norm LIKE '% pro %'
          OR d.label_norm LIKE '% je %'
        THEN 1 ELSE 0 END
FROM core.v_social_discovery d;
GO

CREATE OR ALTER VIEW core.v_social_hs_filtered
AS
SELECT
    [year],
    label_raw,
    label_norm,
    value_num,
    metric_type,
    metric_group = CASE
        WHEN metric_type IN ('incidents','lost_days','fatalities') THEN 'count'
        WHEN metric_type IN ('ltir','trir','severity_rate') OR is_percent_like = 1 THEN 'rate'
        ELSE 'other' END
FROM core.v_social_hs_tagged
WHERE is_hs = 1
  AND value_num IS NOT NULL AND value_num > 0
  AND [year] IS NOT NULL;
GO

CREATE OR ALTER VIEW core.v_social_hs_counts_yearly
AS
SELECT
    [year],
    metric = metric_type,
    value_num = SUM(value_num)
FROM core.v_social_hs_filtered
WHERE metric_group = 'count'
GROUP BY [year], metric_type;
GO

CREATE OR ALTER VIEW core.v_social_hs_rates_yearly
AS
SELECT
    [year],
    rate_metric = metric_type,
    value_num = AVG(value_num)
FROM core.v_social_hs_filtered
WHERE metric_group = 'rate'
GROUP BY [year], metric_type;
GO

CREATE OR ALTER VIEW mart.v_hs_counts_by_year
AS
SELECT [year], metric, value_num
FROM core.v_social_hs_counts_yearly;
GO

CREATE OR ALTER VIEW mart.v_hs_rates_by_year
AS
SELECT [year], rate_metric, value_num
FROM core.v_social_hs_rates_yearly;
GO

SELECT TOP (10) * FROM mart.v_hs_counts_by_year ORDER BY [year] DESC, metric;
SELECT TOP (10) * FROM mart.v_hs_rates_by_year ORDER BY [year] DESC, rate_metric;
