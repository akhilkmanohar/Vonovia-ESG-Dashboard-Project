SET NOCOUNT ON;
-- Module 23e – Energy Mix: exclude TOTAL/SUM rows from absolute MWh

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
        label_raw = NULLIF(CONCAT_WS(' ',
                         NULLIF(CONVERT(nvarchar(4000), ey.row_label), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.sheet_name), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.category), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.subcategory), '')
                     ), ''),
        rl_norm = LOWER(REPLACE(REPLACE(REPLACE(REPLACE(
                     CONVERT(nvarchar(4000), NULLIF(CONCAT_WS(' ',
                         NULLIF(CONVERT(nvarchar(4000), ey.row_label), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.sheet_name), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.category), ''),
                         NULLIF(CONVERT(nvarchar(4000), ey.subcategory), '')
                     ), '') COLLATE Latin1_General_100_CI_AI),
                     'ä','a'),'ö','o'),'ü','u'),'ß','ss'))
    FROM core.energy_yearly ey
    WHERE ey.derived_unit = 'MWh'
)
SELECT
    b.[year],
    row_label = b.label_raw,
    b.derived_unit,
    b.mwh,
    is_rate_like = CASE
        WHEN b.rl_norm LIKE '%[%]%'
          OR b.rl_norm LIKE '% share %' OR b.rl_norm LIKE 'share %' OR b.rl_norm LIKE '% share'
          OR b.rl_norm LIKE '%anteil%' OR b.rl_norm LIKE '%quote%' OR b.rl_norm LIKE '%quota%'
          OR b.rl_norm LIKE '%rate%'  OR b.rl_norm LIKE '%ee-quote%' OR b.rl_norm LIKE '%ee quote%'
          OR b.rl_norm LIKE '%mix (%)%' OR b.rl_norm LIKE '%mix %' OR b.rl_norm LIKE '%struktur%'
        THEN 1 ELSE 0 END,
    is_total_like = CASE
        WHEN b.rl_norm LIKE '%total%'
          OR b.rl_norm LIKE '%overall%'
          OR b.rl_norm LIKE '%sum%'
          OR b.rl_norm LIKE '%summe%'
          OR b.rl_norm LIKE '%gesamt%'
          OR b.rl_norm LIKE '%gesamtverbrauch%'
          OR b.rl_norm LIKE '%totalverbrauch%'
          OR b.rl_norm LIKE '%gesamtenergie%'
          OR b.rl_norm LIKE '%energy total%'
          OR b.rl_norm LIKE '%gesamt %energie%'
          OR b.rl_norm LIKE '%energie gesamt%'
          OR b.rl_norm LIKE '%strom gesamt%'
          OR b.rl_norm LIKE '%waerme gesamt%'
          OR b.rl_norm LIKE '%wärme gesamt%'
        THEN 1 ELSE 0 END,
    energy_group = CASE
        WHEN b.rl_norm LIKE '%electricity%'
          OR b.rl_norm LIKE '%elektrizit%'
          OR b.rl_norm LIKE '%strom%'
          OR b.rl_norm LIKE '%netzstrom%'
          OR b.rl_norm LIKE '%strombezug%'
          OR b.rl_norm LIKE '%stromverbrauch%'
          OR b.rl_norm LIKE '%power%'
          OR b.rl_norm LIKE '%pv%'
          OR b.rl_norm LIKE '%photovoltaik%'
          OR b.rl_norm LIKE '%solarstrom%'
          OR b.rl_norm LIKE '%solar power%'
          OR b.rl_norm LIKE '%wind%'
          OR b.rl_norm LIKE '%windstrom%'
          OR b.rl_norm LIKE '%wasserkraft%'
          OR b.rl_norm LIKE '%hydro%'
          OR b.rl_norm LIKE '%oekostrom%'
          OR b.rl_norm LIKE '%oeko%strom%'
        THEN 'Electricity'
        WHEN b.rl_norm LIKE '%heat%'
          OR b.rl_norm LIKE '%waerme%'
          OR b.rl_norm LIKE '%wärme%'
          OR b.rl_norm LIKE '%fernwaerme%'
          OR b.rl_norm LIKE '%fernwärme%'
          OR b.rl_norm LIKE '%nahwaerme%'
          OR b.rl_norm LIKE '%nahwärme%'
          OR b.rl_norm LIKE '%district heat%'
          OR b.rl_norm LIKE '%steam%'
          OR b.rl_norm LIKE '%dampf%'
          OR b.rl_norm LIKE '%heizung%'
          OR b.rl_norm LIKE '%heizwaerme%'
          OR b.rl_norm LIKE '%heizwärme%'
          OR b.rl_norm LIKE '%solarthermie%'
          OR b.rl_norm LIKE '%solar thermal%'
          OR b.rl_norm LIKE '%wärmepumpe%'
          OR b.rl_norm LIKE '%waermepumpe%'
          OR b.rl_norm LIKE '%heat pump%'
        THEN 'Heat'
        WHEN b.rl_norm LIKE '%gas%'
          OR b.rl_norm LIKE '%erdgas%'
          OR b.rl_norm LIKE '%biogas%'
          OR b.rl_norm LIKE '%biomethan%'
          OR b.rl_norm LIKE '%lpg%'
          OR b.rl_norm LIKE '%cng%'
          OR b.rl_norm LIKE '%lng%'
          OR b.rl_norm LIKE '%diesel%'
          OR b.rl_norm LIKE '%petrol%'
          OR b.rl_norm LIKE '%benzin%'
          OR b.rl_norm LIKE '%hvo%'
          OR b.rl_norm LIKE '%biodiesel%'
          OR b.rl_norm LIKE '%ethanol%'
          OR b.rl_norm LIKE '%heizol%'
          OR b.rl_norm LIKE '%heizöl%'
          OR b.rl_norm LIKE '%heating oil%'
          OR b.rl_norm LIKE '%kraftstoff%'
          OR b.rl_norm LIKE '%fuhrpark%'
          OR b.rl_norm LIKE '%fleet%'
          OR b.rl_norm LIKE '%biomass%'
          OR b.rl_norm LIKE '%biomasse%'
          OR b.rl_norm LIKE '%holz%'
          OR b.rl_norm LIKE '%pellet%'
        THEN 'Fuels'
        ELSE 'Other'
    END,
    is_renewable = CASE
        WHEN (
              b.rl_norm LIKE '%renewable%'
           OR b.rl_norm LIKE '%erneuerbar%'
           OR b.rl_norm LIKE '%regenerativ%'
           OR b.rl_norm LIKE '%green electricity%'
           OR b.rl_norm LIKE '%green power%'
           OR b.rl_norm LIKE '%oekostrom%'
           OR b.rl_norm LIKE '%oeko%strom%'
           OR b.rl_norm LIKE '%gruenstrom%'
           OR b.rl_norm LIKE '%grunstrom%'
           OR b.rl_norm LIKE '%grün%strom%'
           OR b.rl_norm LIKE '%guarantee of origin%'
           OR b.rl_norm LIKE '%guarantees of origin%'
           OR b.rl_norm LIKE '%herkunftsnachweis%'
           OR b.rl_norm LIKE '%hkn%'
           OR b.rl_norm LIKE '%pv%'
           OR b.rl_norm LIKE '%photovoltaik%'
           OR b.rl_norm LIKE '%solarstrom%'
           OR b.rl_norm LIKE '%solar power%'
           OR b.rl_norm LIKE '%wind%'
           OR b.rl_norm LIKE '%windstrom%'
           OR b.rl_norm LIKE '%wasserkraft%'
           OR b.rl_norm LIKE '%hydro%'
           OR b.rl_norm LIKE '%biogas%'
           OR b.rl_norm LIKE '%biomethan%'
           OR b.rl_norm LIKE '%biomass%'
           OR b.rl_norm LIKE '%biomasse%'
           OR b.rl_norm LIKE '%holz%'
           OR b.rl_norm LIKE '%pellet%'
           OR b.rl_norm LIKE '%solarthermie%'
           OR b.rl_norm LIKE '%solar thermal%'
           OR b.rl_norm LIKE '%geotherm%'
        )
        AND NOT (
              b.rl_norm LIKE '%[%]%'
           OR b.rl_norm LIKE '% share %'
           OR b.rl_norm LIKE 'share %'
           OR b.rl_norm LIKE '% share'
           OR b.rl_norm LIKE '%anteil%'
           OR b.rl_norm LIKE '%quote%'
           OR b.rl_norm LIKE '%quota%'
           OR b.rl_norm LIKE '%rate%'
        )
        THEN 1 ELSE 0 END
FROM base b;
GO

CREATE OR ALTER VIEW core.v_energy_mix_filtered
AS
SELECT [year], row_label, derived_unit, mwh, energy_group, is_renewable
FROM core.v_energy_mix_tagged
WHERE is_rate_like = 0
  AND is_total_like = 0
  AND mwh > 0;
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
