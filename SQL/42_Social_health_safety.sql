SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.v_social_discovery','V') IS NULL
BEGIN
    RAISERROR('Missing core.v_social_discovery (run Social S1 discovery first).', 16, 1);
    RETURN;
END
GO

/* Predefine token tables to avoid long OR chains */
CREATE OR ALTER VIEW core.v_social_hs_tagged
AS
WITH detection AS (
    SELECT pattern FROM (VALUES
        ('%unfall%'),('%unfaelle%'),('%unfalle%'),('%verletzung%'),('%vorfall%'),('%incident%'),('%accident%'),
        ('%lost day%'),('%lost days%'),('%ausfalltage%'),('%verloren%'),('%fehltage%'),('%fatal%'),('%todes%'),
        ('%ltir%'),('%ltrir%'),('%trir%'),('%severity%'),('%schweregrad%')
    ) AS d(pattern)
), metric_tokens AS (
    SELECT * FROM (VALUES
        (1,'fatalities','%fatal%'),
        (1,'fatalities','%todes%'),
        (2,'lost_days','%lost day%'),
        (2,'lost_days','%lost days%'),
        (2,'lost_days','%ausfalltage%'),
        (2,'lost_days','%verloren%'),
        (2,'lost_days','%fehltage%'),
        (3,'ltir','%ltir%'),
        (3,'ltir','%ltrir%'),
        (4,'trir','%trir%'),
        (5,'severity_rate','%severity%'),
        (5,'severity_rate','%schweregrad%'),
        (6,'incidents','%incident%'),
        (6,'incidents','%accident%'),
        (6,'incidents','%unfall%'),
        (6,'incidents','%unfaelle%'),
        (6,'incidents','%unfalle%'),
        (6,'incidents','%verletzung%'),
        (6,'incidents','%vorfall%')
    ) AS m(priority, metric_type, pattern)
), rate_tokens AS (
    SELECT pattern FROM (VALUES
        ('%[%]%'),('% rate%'),('rate %'),('%quote%'),('%anteil%'),('%percent%'),('%prozent%'),('% per %'),('% pro %'),('% je %')
    ) AS r(pattern)
)
SELECT
    [year]        = d.year_guess,
    d.label_raw,
    d.label_norm,
    value_num     = TRY_CONVERT(decimal(38,6), d.value_pref),
    category_norm = d.category_norm,
    is_hs         = CASE WHEN det.hit = 1 OR mt.metric_type IS NOT NULL THEN 1 ELSE 0 END,
    metric_type   = COALESCE(mt.metric_type, CASE WHEN det.hit = 1 THEN 'other' ELSE 'other' END),
    is_percent_like = CASE WHEN rt.hit = 1 THEN 1 ELSE 0 END
FROM core.v_social_discovery d
OUTER APPLY (
    SELECT TOP (1) 1 AS hit
    FROM detection tok
    WHERE d.label_norm LIKE tok.pattern
) det
OUTER APPLY (
    SELECT TOP (1) metric_type
    FROM metric_tokens tok
    WHERE d.label_norm LIKE tok.pattern
    ORDER BY tok.priority
) mt
OUTER APPLY (
    SELECT TOP (1) 1 AS hit
    FROM rate_tokens tok
    WHERE d.label_norm LIKE tok.pattern
) rt;
GO

CREATE OR ALTER VIEW core.v_social_hs_filtered
AS
SELECT
    [year],
    label_raw,
    label_norm,
    value_num,
    metric_type,
    metric_group = CASE WHEN metric_type IN ('incidents','lost_days','fatalities') THEN 'count'
                        WHEN metric_type IN ('ltir','trir','severity_rate') OR is_percent_like = 1 THEN 'rate'
                        ELSE 'other' END
FROM core.v_social_hs_tagged
WHERE is_hs = 1
  AND value_num IS NOT NULL AND value_num > 0
  AND [year] IS NOT NULL;
GO

CREATE OR ALTER VIEW core.v_social_hs_counts_yearly
AS
SELECT [year], metric = metric_type, value_num = SUM(value_num)
FROM core.v_social_hs_filtered
WHERE metric_group = 'count'
GROUP BY [year], metric_type;
GO

CREATE OR ALTER VIEW core.v_social_hs_rates_yearly
AS
SELECT [year], rate_metric = metric_type, value_num = AVG(value_num)
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
SELECT TOP (10) * FROM mart.v_hs_rates_by_year  ORDER BY [year] DESC, rate_metric;
