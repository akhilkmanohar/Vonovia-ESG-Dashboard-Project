SET NOCOUNT ON;
-- Module 23b – Energy Mix reporting views (rpt.*) + mini cards
-- Depends on: mart.v_energy_mix_by_year, mart.v_energy_renewable_share (built by Module 23)

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')  EXEC('CREATE SCHEMA rpt;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart') EXEC('CREATE SCHEMA mart;');
GO

/* 1) Fact-like reporting view for direct PBIX binding */
CREATE OR ALTER VIEW rpt.fact_energy_mix
AS
SELECT
    m.[year],
    m.energy_group,
    renewable_flag = CAST(m.is_renewable AS bit),
    m.mwh
FROM mart.v_energy_mix_by_year AS m;
GO

/* 2) Renewable share by year (pass-through for consistency with other rpt views) */
CREATE OR ALTER VIEW rpt.v_energy_renewable_share
AS
SELECT
    r.[year],
    r.renewable_mwh,
    r.total_mwh,
    r.renewable_share
FROM mart.v_energy_renewable_share AS r;
GO

/* 3) Cards: latest mix (absolute MWh) and last-5-year renewable share trend */
CREATE OR ALTER VIEW rpt.v_energy_cards_mix_and_share
AS
WITH maxy AS (SELECT MAX([year]) AS y FROM mart.v_energy_mix_by_year),
mix_latest AS (
    SELECT
        card_type = 'mix_latest',
        [year]    = m.[year],
        energy_group = m.energy_group,
        renewable_label = CASE WHEN m.is_renewable=1 THEN 'Renewable' ELSE 'Non-renewable' END,
        mwh = m.mwh,
        renewable_share = CAST(NULL AS decimal(38,6))
    FROM mart.v_energy_mix_by_year m
    CROSS JOIN maxy
    WHERE m.[year] = maxy.y
),
share_last5 AS (
    SELECT
        card_type = 'renewable_share_last5',
        [year]    = t.[year],
        energy_group = CAST(NULL AS varchar(40)),
        renewable_label = CAST(NULL AS varchar(40)),
        mwh = CAST(NULL AS decimal(38,6)),
        renewable_share = t.renewable_share
    FROM mart.v_energy_renewable_share t
    CROSS JOIN maxy
    WHERE t.[year] BETWEEN maxy.y - 4 AND maxy.y
)
SELECT * FROM mix_latest
UNION ALL
SELECT * FROM share_last5;
GO

/* 4) Quick peeks for verification */
DECLARE @max_year int = (SELECT MAX([year]) FROM rpt.fact_energy_mix);
SELECT TOP (30) [year], energy_group, renewable_flag, mwh
FROM rpt.fact_energy_mix
ORDER BY [year] DESC, energy_group, renewable_flag DESC;

SELECT TOP (10) *
FROM rpt.v_energy_renewable_share
ORDER BY [year] DESC;

SELECT *
FROM rpt.v_energy_cards_mix_and_share
ORDER BY card_type, [year] DESC, energy_group;
