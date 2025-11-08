SET NOCOUNT ON;
-- QA 30 – Energy mix pivots & group shares

DECLARE @max_year INT = (SELECT MAX([year]) FROM rpt.v_energy_mix_wide);

-- SECTION: schema_check
SELECT 'schema_check' AS section,
       v = 'rpt.v_energy_mix_wide', rows = (SELECT COUNT(*) FROM rpt.v_energy_mix_wide)
UNION ALL
SELECT 'schema_check', 'rpt.v_energy_mix_share_by_group', (SELECT COUNT(*) FROM rpt.v_energy_mix_share_by_group);

-- SECTION: wide_recent (last 10 years)
SELECT 'wide_recent' AS section, *
FROM rpt.v_energy_mix_wide
WHERE [year] >= ISNULL(@max_year,0) - 9
ORDER BY [year] DESC;

-- SECTION: group_share_recent (last 15 years, share bounded in [0,1])
SELECT 'group_share_recent' AS section, s.[year], s.energy_group, s.renewable_mwh, s.total_mwh, s.renewable_share_group
FROM rpt.v_energy_mix_share_by_group s
WHERE s.[year] >= ISNULL(@max_year,0) - 14
ORDER BY s.[year] DESC, s.energy_group;

-- SECTION: sanity_flags (shares outside [0,1], missing totals)
SELECT 'sanity_flags' AS section, issue,
       [year], energy_group, renewable_mwh, total_mwh, renewable_share_group
FROM (
    SELECT
        CASE
          WHEN total_mwh IS NULL THEN 'NULL_TOTAL'
          WHEN total_mwh < 0     THEN 'NEGATIVE_TOTAL'
          WHEN renewable_share_group < 0 OR renewable_share_group > 1 THEN 'SHARE_OUT_OF_RANGE'
          ELSE NULL
        END AS issue,
        *
    FROM rpt.v_energy_mix_share_by_group
) x
WHERE issue IS NOT NULL
ORDER BY [year] DESC, energy_group;
