SET NOCOUNT ON;
-- QA 28 – Energy mix (v2)

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

-- SECTION: candidates (by energy_group x renewable)
SELECT 'candidates' AS section, energy_group, is_renewable, COUNT(*) AS row_count
FROM core.v_energy_mix_tagged
GROUP BY energy_group, is_renewable
ORDER BY energy_group, is_renewable DESC;

-- SECTION: mix_recent (last 10 years)
SELECT 'mix_recent' AS section, f.[year], f.energy_group, f.is_renewable, f.mwh
FROM core.v_energy_mix_final AS f
WHERE f.[year] >= ISNULL(@max_year, 0) - 9
ORDER BY f.[year] DESC, f.energy_group, f.is_renewable DESC;

-- SECTION: renewable_share (last 15 years)
SELECT 'renewable_share' AS section, r.[year], r.renewable_mwh, r.total_mwh, r.renewable_share
FROM mart.v_energy_renewable_share AS r
WHERE r.[year] >= ISNULL(@max_year, 0) - 14
ORDER BY r.[year] DESC;

-- SECTION: density (rows by year for MWh)
SELECT 'density' AS section, e.[year], COUNT(*) AS row_count
FROM core.energy_yearly AS e
WHERE e.derived_unit = 'MWh'
GROUP BY e.[year]
ORDER BY e.[year] DESC;

-- SECTION: top_outliers (largest MWh rows)
SELECT TOP (25) 'top_outliers' AS section, f.[year], f.row_label, f.energy_group, f.is_renewable, f.mwh
FROM core.v_energy_mix_filtered AS f
ORDER BY f.mwh DESC;

-- SECTION: other_share (how much "Other" remains by year)
SELECT 'other_share' AS section, f.[year],
       SUM(CASE WHEN f.energy_group='Other' THEN f.mwh ELSE 0 END) AS other_mwh,
       SUM(f.mwh) AS total_mwh,
       CASE WHEN SUM(f.mwh)>0 THEN CAST(SUM(CASE WHEN f.energy_group='Other' THEN f.mwh ELSE 0 END) AS decimal(38,6))/CAST(SUM(f.mwh) AS decimal(38,6)) END AS other_ratio
FROM core.v_energy_mix_final f
GROUP BY f.[year]
ORDER BY f.[year] DESC;
