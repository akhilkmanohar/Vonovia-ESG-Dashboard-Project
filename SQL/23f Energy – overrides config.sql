SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

-- Module 23f – Override-aware tagging (dynamic-safe)

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
GO

IF OBJECT_ID('core.energy_mix_overrides','U') IS NULL
BEGIN
    CREATE TABLE core.energy_mix_overrides(
        override_id   int IDENTITY(1,1) PRIMARY KEY,
        pattern_norm  nvarchar(400) NOT NULL,
        energy_group  varchar(20) NULL CHECK (energy_group IN ('Electricity','Heat','Fuels','Other')),
        is_renewable  bit NULL,
        is_rate_like  bit NULL,
        is_total_like bit NULL,
        is_active     bit NOT NULL DEFAULT(1),
        priority      int NOT NULL DEFAULT(0),
        notes         nvarchar(400) NULL
    );
    CREATE INDEX IX_energy_mix_overrides_active
      ON core.energy_mix_overrides(is_active, priority)
      INCLUDE(pattern_norm, energy_group, is_renewable, is_rate_like, is_total_like);
END
GO

IF OBJECT_ID('core.v_energy_mix_tagged','V') IS NOT NULL DROP VIEW core.v_energy_mix_tagged;
GO

CREATE VIEW core.v_energy_mix_tagged
AS
WITH base AS (
    SELECT
        ey.[year],
        ey.derived_unit,
        TRY_CONVERT(decimal(38,6), ey.value) AS mwh,
        label_raw = NULLIF(LTRIM(RTRIM(CONCAT_WS(' ',
                        NULLIF(CONVERT(nvarchar(4000), ey.row_label), ''),
                        NULLIF(CONVERT(nvarchar(4000), ey.sheet_name), ''),
                        NULLIF(CONVERT(nvarchar(4000), ey.category), ''),
                        NULLIF(CONVERT(nvarchar(4000), ey.subcategory), '')))), '')
    FROM core.energy_yearly AS ey
    WHERE ey.derived_unit = 'MWh'
), norm AS (
    SELECT b.[year], b.derived_unit, b.mwh, b.label_raw,
           rl_norm = LOWER(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(CONVERT(nvarchar(4000), b.label_raw), ''), 'ä','a'),'ö','o'),'ü','u'),'ß','ss'))
    FROM base b
), calc AS (
    SELECT n.*,
        is_rate_like_base = CASE
            WHEN n.rl_norm LIKE '%[%]%'
              OR n.rl_norm LIKE '% share %' OR n.rl_norm LIKE 'share %' OR n.rl_norm LIKE '% share'
              OR n.rl_norm LIKE '%anteil%' OR n.rl_norm LIKE '%quote%' OR n.rl_norm LIKE '%quota%'
              OR n.rl_norm LIKE '%rate%'  OR n.rl_norm LIKE '%ee-quote%' OR n.rl_norm LIKE '%ee quote%'
              OR n.rl_norm LIKE '%mix (%)%' OR n.rl_norm LIKE '%mix %' OR n.rl_norm LIKE '%struktur%'
            THEN 1 ELSE 0 END,
        is_total_like_base = CASE
            WHEN n.rl_norm LIKE '%total%' OR n.rl_norm LIKE '%overall%' OR n.rl_norm LIKE '%sum%'
              OR n.rl_norm LIKE '%summe%' OR n.rl_norm LIKE '%gesamt%' OR n.rl_norm LIKE '%gesamtverbrauch%'
              OR n.rl_norm LIKE '%totalverbrauch%' OR n.rl_norm LIKE '%gesamtenergie%'
              OR n.rl_norm LIKE '%energie gesamt%' OR n.rl_norm LIKE '%strom gesamt%'
              OR n.rl_norm LIKE '%waerme gesamt%' OR n.rl_norm LIKE '%wärme gesamt%'
            THEN 1 ELSE 0 END,
        energy_group_base = CASE
            WHEN n.rl_norm LIKE '%electricity%' OR n.rl_norm LIKE '%elektrizit%' OR n.rl_norm LIKE '%strom%'
              OR n.rl_norm LIKE '%netzstrom%' OR n.rl_norm LIKE '%strombezug%' OR n.rl_norm LIKE '%stromverbrauch%'
              OR n.rl_norm LIKE '%power%' OR n.rl_norm LIKE '%pv%' OR n.rl_norm LIKE '%photovoltaik%' OR n.rl_norm LIKE '%solarstrom%'
              OR n.rl_norm LIKE '%solar power%' OR n.rl_norm LIKE '%wind%' OR n.rl_norm LIKE '%windstrom%'
              OR n.rl_norm LIKE '%wasserkraft%' OR n.rl_norm LIKE '%hydro%' OR n.rl_norm LIKE '%oekostrom%' OR n.rl_norm LIKE '%oeko%strom%'
            THEN 'Electricity'
            WHEN n.rl_norm LIKE '%heat%' OR n.rl_norm LIKE '%waerme%' OR n.rl_norm LIKE '%wärme%'
              OR n.rl_norm LIKE '%fernwaerme%' OR n.rl_norm LIKE '%fernwärme%' OR n.rl_norm LIKE '%nahwaerme%' OR n.rl_norm LIKE '%nahwärme%'
              OR n.rl_norm LIKE '%district heat%' OR n.rl_norm LIKE '%steam%' OR n.rl_norm LIKE '%dampf%'
              OR n.rl_norm LIKE '%heizung%' OR n.rl_norm LIKE '%heizwaerme%' OR n.rl_norm LIKE '%heizwärme%'
              OR n.rl_norm LIKE '%solarthermie%' OR n.rl_norm LIKE '%solar thermal%' OR n.rl_norm LIKE '%wärmepumpe%' OR n.rl_norm LIKE '%waermepumpe%' OR n.rl_norm LIKE '%heat pump%'
            THEN 'Heat'
            WHEN n.rl_norm LIKE '%gas%' OR n.rl_norm LIKE '%erdgas%' OR n.rl_norm LIKE '%biogas%' OR n.rl_norm LIKE '%biomethan%' OR n.rl_norm LIKE '%lpg%' OR n.rl_norm LIKE '%cng%' OR n.rl_norm LIKE '%lng%'
              OR n.rl_norm LIKE '%diesel%' OR n.rl_norm LIKE '%petrol%' OR n.rl_norm LIKE '%benzin%' OR n.rl_norm LIKE '%hvo%' OR n.rl_norm LIKE '%biodiesel%' OR n.rl_norm LIKE '%ethanol%'
              OR n.rl_norm LIKE '%heizol%' OR n.rl_norm LIKE '%heizöl%' OR n.rl_norm LIKE '%heating oil%'
              OR n.rl_norm LIKE '%kraftstoff%' OR n.rl_norm LIKE '%fuhrpark%' OR n.rl_norm LIKE '%fleet%'
              OR n.rl_norm LIKE '%biomass%' OR n.rl_norm LIKE '%biomasse%' OR n.rl_norm LIKE '%holz%' OR n.rl_norm LIKE '%pellet%'
            THEN 'Fuels'
            ELSE 'Other'
        END,
        is_renewable_base = CASE
            WHEN (
                  n.rl_norm LIKE '%renewable%' OR n.rl_norm LIKE '%erneuerbar%' OR n.rl_norm LIKE '%regenerativ%'
               OR n.rl_norm LIKE '%green electricity%' OR n.rl_norm LIKE '%green power%'
               OR n.rl_norm LIKE '%oekostrom%' OR n.rl_norm LIKE '%oeko%strom%' OR n.rl_norm LIKE '%gruenstrom%' OR n.rl_norm LIKE '%grunstrom%' OR n.rl_norm LIKE '%grün%strom%'
               OR n.rl_norm LIKE '%guarantee of origin%' OR n.rl_norm LIKE '%herkunftsnachweis%' OR n.rl_norm LIKE '%hkn%'
               OR n.rl_norm LIKE '%pv%' OR n.rl_norm LIKE '%photovoltaik%' OR n.rl_norm LIKE '%solarstrom%' OR n.rl_norm LIKE '%solar power%'
               OR n.rl_norm LIKE '%wind%' OR n.rl_norm LIKE '%windstrom%' OR n.rl_norm LIKE '%wasserkraft%' OR n.rl_norm LIKE '%hydro%'
               OR n.rl_norm LIKE '%biogas%' OR n.rl_norm LIKE '%biomethan%' OR n.rl_norm LIKE '%biomass%' OR n.rl_norm LIKE '%biomasse%' OR n.rl_norm LIKE '%holz%' OR n.rl_norm LIKE '%pellet%'
               OR n.rl_norm LIKE '%solarthermie%' OR n.rl_norm LIKE '%solar thermal%' OR n.rl_norm LIKE '%geotherm%'
            )
            AND NOT (
                  n.rl_norm LIKE '%[%]%'
               OR n.rl_norm LIKE '% share %' OR n.rl_norm LIKE 'share %' OR n.rl_norm LIKE '% share'
               OR n.rl_norm LIKE '%anteil%' OR n.rl_norm LIKE '%quote%' OR n.rl_norm LIKE '%quota%' OR n.rl_norm LIKE '%rate%'
            )
            THEN 1 ELSE 0 END
    FROM norm n
)
SELECT
    c.[year],
    row_label = c.label_raw,
    c.derived_unit,
    c.mwh,
    ov.override_id,
    ov.pattern_norm AS override_pattern,
    is_rate_like  = CASE WHEN COALESCE(ov.is_rate_like,  c.is_rate_like_base)  = 1 THEN 1 ELSE 0 END,
    is_total_like = CASE WHEN COALESCE(ov.is_total_like, c.is_total_like_base) = 1 THEN 1 ELSE 0 END,
    energy_group  = COALESCE(ov.energy_group, c.energy_group_base),
    is_renewable  = CASE WHEN COALESCE(ov.is_renewable, c.is_renewable_base)  = 1 THEN 1 ELSE 0 END
FROM calc c
OUTER APPLY (
    SELECT TOP (1) *
    FROM core.energy_mix_overrides o
    WHERE o.is_active = 1
      AND c.rl_norm LIKE '%' + o.pattern_norm + '%'
    ORDER BY o.priority DESC, o.override_id DESC
) ov;
GO

CREATE OR ALTER VIEW core.v_energy_mix_filtered AS
SELECT [year], row_label, derived_unit, mwh, energy_group, is_renewable
FROM core.v_energy_mix_tagged
WHERE is_rate_like = 0 AND is_total_like = 0 AND mwh > 0;
GO

CREATE OR ALTER VIEW core.v_energy_mix_final AS
SELECT [year], energy_group, is_renewable, SUM(mwh) AS mwh
FROM core.v_energy_mix_filtered
GROUP BY [year], energy_group, is_renewable;
GO

CREATE OR ALTER VIEW mart.v_energy_mix_by_year AS
SELECT [year], energy_group, is_renewable, mwh
FROM core.v_energy_mix_final;
GO

CREATE OR ALTER VIEW mart.v_energy_renewable_share AS
WITH totals AS (
    SELECT [year],
           renewable_mwh = SUM(CASE WHEN is_renewable = 1 THEN mwh ELSE 0 END),
           total_mwh     = SUM(mwh)
    FROM core.v_energy_mix_final
    GROUP BY [year]
)
SELECT [year], renewable_mwh, total_mwh,
       renewable_share = CASE WHEN total_mwh>0 THEN CAST(renewable_mwh AS decimal(38,6))/CAST(total_mwh AS decimal(38,6)) END
FROM totals;
GO

SELECT TOP (10) [year], energy_group, is_renewable, mwh
FROM mart.v_energy_mix_by_year
ORDER BY [year] DESC, energy_group, is_renewable DESC;

SELECT TOP (10) [year], row_label, override_id, override_pattern
FROM core.v_energy_mix_tagged
WHERE override_id IS NOT NULL
ORDER BY [year] DESC, row_label;
