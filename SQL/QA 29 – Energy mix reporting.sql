SET NOCOUNT ON;
-- QA 29 – Energy mix reporting layer checks

DECLARE @max_year INT = (SELECT MAX([year]) FROM rpt.fact_energy_mix);

-- SECTION: schema_check (views exist & row counts)
SELECT 'schema_check' AS section,
       v = 'rpt.fact_energy_mix', rows = (SELECT COUNT(*) FROM rpt.fact_energy_mix)
UNION ALL
SELECT 'schema_check', 'rpt.v_energy_renewable_share', (SELECT COUNT(*) FROM rpt.v_energy_renewable_share)
UNION ALL
SELECT 'schema_check', 'rpt.v_energy_cards_mix_and_share', (SELECT COUNT(*) FROM rpt.v_energy_cards_mix_and_share);

-- SECTION: mix_recent (ensure groups present; last 10y)
SELECT 'mix_recent' AS section, f.[year], f.energy_group,
       SUM(CASE WHEN f.renewable_flag=1 THEN f.mwh ELSE 0 END) AS renewable_mwh,
       SUM(CASE WHEN f.renewable_flag=0 THEN f.mwh ELSE 0 END) AS nonrenewable_mwh,
       SUM(f.mwh) AS total_mwh
FROM rpt.fact_energy_mix f
WHERE f.[year] >= ISNULL(@max_year,0) - 9
GROUP BY f.[year], f.energy_group
ORDER BY f.[year] DESC, f.energy_group;

-- SECTION: renewable_share_recent (last 15y; share must be <=1)
SELECT 'renewable_share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM rpt.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC;

-- SECTION: sanity_flags (negatives/zeros and other ratio)
;WITH final AS (
    SELECT [year], energy_group, renewable_flag, mwh FROM rpt.fact_energy_mix
),
other_ratio AS (
    SELECT f.[year],
           other_mwh = SUM(CASE WHEN f.energy_group='Other' THEN f.mwh ELSE 0 END),
           total_mwh = SUM(f.mwh)
    FROM final f
    GROUP BY f.[year]
)
SELECT 'sanity_flags' AS section,
       issue = CASE
                 WHEN f.mwh < 0 THEN 'NEGATIVE_MWH'
                 WHEN f.mwh = 0 THEN 'ZERO_MWH'
                 ELSE 'OK'
               END,
       f.[year], f.energy_group, f.renewable_flag, f.mwh
FROM final f
WHERE f.mwh <= 0
UNION ALL
SELECT 'sanity_flags', 'OTHER_RATIO', o.[year], NULL, NULL,
       CAST(CASE WHEN o.total_mwh>0 THEN o.other_mwh*1.0/o.total_mwh END AS decimal(38,6))
FROM other_ratio o
ORDER BY [year] DESC;
