SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.v_social_discovery','V') IS NULL
BEGIN
    RAISERROR('Missing core.v_social_discovery (run Module S1 first).', 16, 1);
    RETURN;
END;
GO

CREATE OR ALTER VIEW core.v_social_workforce_tagged
AS
SELECT
    [year]        = d.year_guess,
    d.label_raw,
    d.label_norm,
    value_num     = TRY_CONVERT(decimal(38,6), d.value_pref),
    category_norm = d.category_norm,
    is_rate_like = CASE
        WHEN d.label_norm LIKE '%[%]%' OR d.label_norm LIKE '% percent%' OR d.label_norm LIKE 'percent %'
         OR d.label_norm LIKE '% prozent%' OR d.label_norm LIKE '% rate%' OR d.label_norm LIKE 'rate %'
         OR d.label_norm LIKE '%quote%' OR d.label_norm LIKE '%anteil%' OR d.label_norm LIKE '% per %'
         OR d.label_norm LIKE '% pro %' OR d.label_norm LIKE '% je %'
        THEN 1 ELSE 0 END,
    is_workforce = CASE
        WHEN d.label_norm LIKE '%headcount%' OR d.label_norm LIKE '%workforce%'
          OR d.label_norm LIKE '%employee%'  OR d.label_norm LIKE '%employees%'
          OR d.label_norm LIKE '%mitarbeiter%' OR d.label_norm LIKE '%mitarbeitende%'
          OR d.label_norm LIKE '%beschaeftigte%' OR d.label_norm LIKE '%belegschaft%'
          OR d.label_norm LIKE '%personal%' OR d.label_norm LIKE '%staff%'
          OR d.label_norm LIKE '%fte%' OR d.label_norm LIKE '%vollzeitaequivalent%' OR d.label_norm LIKE '%vollzeit%' 
          OR d.label_norm LIKE '%anzahl%' OR d.label_norm LIKE '%people%'
        THEN 1 ELSE 0 END,
    is_fte = CASE
        WHEN d.label_norm LIKE '%fte%' OR d.label_norm LIKE '%full time equivalent%'
          OR d.label_norm LIKE '%vollzeitaequivalent%' OR d.label_norm LIKE '%vollzeit aequivalent%'
          OR d.label_norm LIKE '%vollzeit%' OR d.label_norm LIKE '%vzae%'
        THEN 1 ELSE 0 END
FROM core.v_social_discovery d;
GO

CREATE OR ALTER VIEW core.v_social_workforce_filtered
AS
SELECT
    [year],
    label_raw,
    label_norm,
    value_num,
    measure = CASE WHEN is_fte = 1 THEN 'FTE' ELSE 'Persons' END
FROM core.v_social_workforce_tagged
WHERE is_workforce = 1
  AND is_rate_like = 0
  AND value_num IS NOT NULL
  AND value_num > 0
  AND [year] IS NOT NULL;
GO

CREATE OR ALTER VIEW core.v_social_workforce_yearly
AS
SELECT
    [year],
    measure,
    SUM(value_num) AS value_num
FROM core.v_social_workforce_filtered
GROUP BY [year], measure;
GO

CREATE OR ALTER VIEW mart.v_workforce_headcount_by_year
AS
SELECT [year], measure, value_num
FROM core.v_social_workforce_yearly;
GO

SELECT TOP (10) *
FROM mart.v_workforce_headcount_by_year
ORDER BY [year] DESC, measure;
