SET NOCOUNT ON;

DECLARE @max_year INT = (SELECT MAX([year]) FROM mart.v_energy_mix_by_year);

SELECT 'exists_check' AS section, v = 'mart.v_energy_mix_by_year',      CASE WHEN OBJECT_ID('mart.v_energy_mix_by_year','V') IS NOT NULL THEN 1 ELSE 0 END AS exists_flag
UNION ALL SELECT 'exists_check','mart.v_energy_renewable_share',         CASE WHEN OBJECT_ID('mart.v_energy_renewable_share','V') IS NOT NULL THEN 1 ELSE 0 END
UNION ALL SELECT 'exists_check','rpt.v_energy_mix_wide',                 CASE WHEN OBJECT_ID('rpt.v_energy_mix_wide','V') IS NOT NULL THEN 1 ELSE 0 END
UNION ALL SELECT 'exists_check','rpt.v_energy_mix_share_by_group',       CASE WHEN OBJECT_ID('rpt.v_energy_mix_share_by_group','V') IS NOT NULL THEN 1 ELSE 0 END
UNION ALL SELECT 'exists_check','rpt.v_energy_cards_mix_and_share',      CASE WHEN OBJECT_ID('rpt.v_energy_cards_mix_and_share','V') IS NOT NULL THEN 1 ELSE 0 END
UNION ALL SELECT 'exists_check','rpt.v_energy_import_catalog',           CASE WHEN OBJECT_ID('rpt.v_energy_import_catalog','V') IS NOT NULL THEN 1 ELSE 0 END;

SELECT 'counts_recent' AS section, b.[year], COUNT(*) AS rows_count, SUM(b.mwh) AS total_mwh
FROM mart.v_energy_mix_by_year b
WHERE b.[year] >= ISNULL(@max_year,0) - 9
GROUP BY b.[year]
ORDER BY b.[year] DESC;

SELECT 'share_recent' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share r
WHERE r.[year] >= ISNULL(@max_year,0) - 9
ORDER BY r.[year] DESC;

SELECT TOP (20) 'cards_preview' AS section, c.*
FROM rpt.v_energy_cards_mix_and_share c
ORDER BY c.card, c.[year] DESC, c.energy_group;

SELECT 'import_catalog' AS section, *
FROM rpt.v_energy_import_catalog;
