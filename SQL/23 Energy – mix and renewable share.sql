SET NOCOUNT ON;
-- Module 23 – Energy Mix & Renewable Share (v2 – robust tagging)

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart;');
GO

CREATE OR ALTER VIEW core.v_energy_mix_tagged
AS
WITH base AS (
    SELECT
        ey.[year],
        ey.derived_unit,
        TRY_CONVERT(decimal(38,6), ey.value) AS mwh,
        label_raw = NULLIF(CONCAT_WS(N' ',
                        NULLIF(CONVERT(nvarchar(4000), ey.row_label), N''),
                        NULLIF(CONVERT(nvarchar(4000), ey.sheet_name), N''),
                        NULLIF(CONVERT(nvarchar(4000), ey.category), N''),
                        NULLIF(CONVERT(nvarchar(4000), ey.subcategory), N'')
                    ), N'')
    FROM core.energy_yearly AS ey
    WHERE ey.derived_unit = 'MWh'
)
SELECT
    b.[year],
    row_label   = b.label_raw,
    b.derived_unit,
    b.mwh,
    is_rate_like = CASE
        WHEN norm.rl_norm LIKE '%[%]%'
          OR norm.rl_norm LIKE '% share %' OR norm.rl_norm LIKE 'share %' OR norm.rl_norm LIKE '% share'
          OR norm.rl_norm LIKE '%anteil%' OR norm.rl_norm LIKE '%quote%' OR norm.rl_norm LIKE '%quota%'
          OR norm.rl_norm LIKE '%rate%'  OR norm.rl_norm LIKE '%ee-quote%' OR norm.rl_norm LIKE '%ee quote%'
          OR norm.rl_norm LIKE '%renewable share%' OR norm.rl_norm LIKE '%strommix (%)%' OR norm.rl_norm LIKE '%mix %'
        THEN 1 ELSE 0 END,
    energy_group = CASE
        WHEN norm.rl_norm LIKE '%electricity%' OR norm.rl_norm LIKE '%elektrizit%' OR norm.rl_norm LIKE '%strom%'
             OR norm.rl_norm LIKE '%power%' OR norm.rl_norm LIKE '%gruenstrom%' OR norm.rl_norm LIKE '%grunstrom%'
             OR norm.rl_norm LIKE '%oekostrom%' OR norm.rl_norm LIKE '%oeko%strom%'
        THEN 'Electricity'
        WHEN norm.rl_norm LIKE '%heat%' OR norm.rl_norm LIKE '%waerme%' OR norm.rl_norm LIKE '%wärme%'
             OR norm.rl_norm LIKE '%fernwaerme%' OR norm.rl_norm LIKE '%fernwärme%' OR norm.rl_norm LIKE '%nahwaerme%'
             OR norm.rl_norm LIKE '%district heat%' OR norm.rl_norm LIKE '%steam%' OR norm.rl_norm LIKE '%dampf%'
             OR norm.rl_norm LIKE '%heizung%' OR norm.rl_norm LIKE '%heizwärme%'
        THEN 'Heat'
        WHEN norm.rl_norm LIKE '%gas%' OR norm.rl_norm LIKE '%erdgas%' OR norm.rl_norm LIKE '%lpg%'
             OR norm.rl_norm LIKE '%cng%' OR norm.rl_norm LIKE '%lng%'
             OR norm.rl_norm LIKE '%diesel%' OR norm.rl_norm LIKE '%petrol%' OR norm.rl_norm LIKE '%benzin%'
             OR norm.rl_norm LIKE '%heizol%' OR norm.rl_norm LIKE '%heizöl%' OR norm.rl_norm LIKE '%heating oil%'
             OR norm.rl_norm LIKE '%kraftstoff%' OR norm.rl_norm LIKE '%fuel%'
        THEN 'Fuels'
        ELSE 'Other'
    END,
    is_renewable = CASE
        WHEN (
              norm.rl_norm LIKE '%renewable%' OR norm.rl_norm LIKE '%erneuerbar%' OR norm.rl_norm LIKE '%regenerativ%'
           OR norm.rl_norm LIKE '%green electricity%' OR norm.rl_norm LIKE '%green power%'
           OR norm.rl_norm LIKE '%oekostrom%' OR norm.rl_norm LIKE '%oeko%strom%' OR norm.rl_norm LIKE '%gruenstrom%' OR norm.rl_norm LIKE '%grunstrom%'
           OR norm.rl_norm LIKE '%gruen strom%' OR norm.rl_norm LIKE '%guarantee of origin%' OR norm.rl_norm LIKE '%guarantees of origin%'
           OR norm.rl_norm LIKE '%herkunftsnachweis%' OR norm.rl_norm LIKE '%herkunftsnachweise%' OR norm.rl_norm LIKE '%hkn%'
        )
        AND NOT (
              norm.rl_norm LIKE '%[%]%'
           OR norm.rl_norm LIKE '% share %' OR norm.rl_norm LIKE 'share %' OR norm.rl_norm LIKE '% share'
           OR norm.rl_norm LIKE '%anteil%' OR norm.rl_norm LIKE '%quote%' OR norm.rl_norm LIKE '%quota%'
           OR norm.rl_norm LIKE '%rate%'  OR norm.rl_norm LIKE '%ee-quote%' OR norm.rl_norm LIKE '%ee quote%'
        )
        THEN 1 ELSE 0 END
FROM base AS b
CROSS APPLY (
    SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                COALESCE(b.label_raw, N'') COLLATE Latin1_General_100_CI_AI,
                'ä','a'),'ö','o'),'ü','u'),'Ä','A'),'Ö','O'),'Ü','U'),'ß','ss')) AS rl_norm
) AS norm;
GO

CREATE OR ALTER VIEW core.v_energy_mix_filtered
AS
SELECT [year], row_label, derived_unit, mwh, energy_group, is_renewable
FROM core.v_energy_mix_tagged
WHERE is_rate_like = 0
  AND mwh IS NOT NULL;
GO

CREATE OR ALTER VIEW core.v_energy_mix_final
AS
SELECT [year], energy_group, is_renewable, SUM(mwh) AS mwh
FROM core.v_energy_mix_filtered
GROUP BY [year], energy_group, is_renewable;
GO

CREATE OR ALTER VIEW mart.v_energy_mix_by_year
AS
SELECT [year], energy_group, is_renewable, mwh
FROM core.v_energy_mix_final;
GO

CREATE OR ALTER VIEW mart.v_energy_renewable_share
AS
WITH totals AS (
    SELECT
        [year],
        renewable_mwh = SUM(CASE WHEN is_renewable = 1 THEN mwh ELSE 0 END),
        total_mwh     = SUM(mwh)
    FROM core.v_energy_mix_final
    GROUP BY [year]
)
SELECT
    [year],
    renewable_mwh,
    total_mwh,
    renewable_share = CASE WHEN total_mwh > 0
                           THEN CAST(renewable_mwh AS decimal(38,6)) / CAST(total_mwh AS decimal(38,6))
                           ELSE NULL END
FROM totals;
GO

DECLARE @max_year int = (SELECT MAX([year]) FROM core.v_energy_mix_final);

SELECT TOP (30) [year], energy_group, is_renewable, mwh
FROM mart.v_energy_mix_by_year
ORDER BY [year] DESC, energy_group, is_renewable DESC;

SELECT TOP (20) *
FROM mart.v_energy_renewable_share
ORDER BY [year] DESC;
