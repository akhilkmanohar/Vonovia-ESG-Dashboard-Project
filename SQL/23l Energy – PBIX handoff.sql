IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='rpt')  EXEC('CREATE SCHEMA rpt;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core;');
GO
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE OR ALTER VIEW rpt.v_energy_cards_mix_and_share
AS
WITH latest AS (
    SELECT MAX([year]) AS y FROM mart.v_energy_mix_by_year
),
mix AS (
    SELECT m.[year], m.energy_group, SUM(m.mwh) AS group_mwh
    FROM mart.v_energy_mix_by_year m
    JOIN latest ly ON ly.y = m.[year]
    GROUP BY m.[year], m.energy_group
),
ttl AS (
    SELECT m.[year], SUM(m.group_mwh) AS total_mwh FROM mix m GROUP BY m.[year]
),
mix_cards AS (
    SELECT
        card = 'latest_mix',
        m.[year],
        m.energy_group,
        value_num = CAST(m.group_mwh AS decimal(38,6)),
        value_pct = CASE WHEN t.total_mwh>0 THEN CAST(m.group_mwh AS decimal(38,6))/CAST(t.total_mwh AS decimal(38,6)) END,
        note = CAST(NULL AS nvarchar(200))
    FROM mix m
    JOIN ttl t ON t.[year]=m.[year]
),
share_cards AS (
    SELECT
        card = 'last5_share',
        r.[year],
        energy_group = CAST(NULL AS varchar(20)),
        value_num = CAST(r.renewable_mwh AS decimal(38,6)),
        value_pct = r.renewable_share,
        note = CAST(NULL AS nvarchar(200))
    FROM mart.v_energy_renewable_share r
    CROSS JOIN latest ly
    WHERE r.[year] BETWEEN ly.y-4 AND ly.y
)
SELECT * FROM mix_cards
UNION ALL
SELECT * FROM share_cards;
GO

CREATE OR ALTER VIEW rpt.v_energy_import_catalog
AS
SELECT *
FROM (VALUES
  (N'mart', N'v_energy_mix_by_year',       N'Year × group × renewable MWh'),
  (N'mart', N'v_energy_renewable_share',   N'Yearly renewable share with totals'),
  (N'rpt',  N'v_energy_mix_wide',          N'Year-level wide pivot (renewable/nonrenewable columns)'),
  (N'rpt',  N'v_energy_mix_share_by_group',N'Per-group renewable share'),
  (N'rpt',  N'v_energy_cards_mix_and_share',N'Cards: latest mix + last-5y renewable share')
) AS x(object_schema, object_name, purpose);
GO

SELECT TOP (10) * FROM rpt.v_energy_cards_mix_and_share ORDER BY card, [year] DESC, energy_group;
SELECT TOP (10) * FROM rpt.v_energy_import_catalog;
