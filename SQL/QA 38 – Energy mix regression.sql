SET NOCOUNT ON;

DECLARE @threshold decimal(5,4) = 0.12;
DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_energy_mix_final);

SELECT 'negatives_check_filtered' AS section, [year], row_label, energy_group, is_renewable, mwh
FROM core.v_energy_mix_filtered
WHERE mwh < 0
UNION ALL
SELECT 'negatives_check_final', [year], CAST(NULL AS nvarchar(4000)), energy_group, is_renewable, mwh
FROM core.v_energy_mix_final
WHERE mwh < 0;

SELECT 'shares_bounds' AS section, [year], renewable_mwh, total_mwh, renewable_share,
       CASE WHEN renewable_share < 0 OR renewable_share > 1 OR renewable_mwh > total_mwh THEN 'BREACH' ELSE 'OK' END AS status
FROM mart.v_energy_renewable_share
ORDER BY [year] DESC;

;WITH by_group AS (
  SELECT [year], SUM(mwh) AS sum_mwh
  FROM mart.v_energy_mix_by_year
  GROUP BY [year]
)
SELECT 'totals_consistency' AS section, y.[year], y.total_mwh, g.sum_mwh,
       (y.total_mwh - g.sum_mwh) AS diff_mwh
FROM mart.v_energy_renewable_share y
LEFT JOIN by_group g ON g.[year] = y.[year]
ORDER BY y.[year] DESC;

IF OBJECT_ID('mart.v_energy_mix_other_ratio','V') IS NOT NULL
BEGIN
    SELECT 'other_threshold_recent' AS section,
           r.[year], r.other_mwh, r.total_mwh, r.other_ratio, @threshold AS threshold,
           CASE WHEN r.other_ratio IS NOT NULL AND r.other_ratio > @threshold THEN 'BREACH' ELSE 'OK' END AS status
    FROM mart.v_energy_mix_other_ratio r
    WHERE r.[year] >= ISNULL(@max_year,0) - 9
    ORDER BY r.[year] DESC;
END
ELSE
BEGIN
    SELECT 'other_threshold_recent' AS section, CAST(NULL AS int) AS [year],
           CAST(NULL AS decimal(38,6)) AS other_mwh, CAST(NULL AS decimal(38,6)) AS total_mwh,
           CAST(NULL AS decimal(38,6)) AS other_ratio, @threshold AS threshold, 'N/A' AS status;
END

SELECT TOP (20) 'latest_snapshot' AS section, b.[year], b.energy_group, b.is_renewable, b.mwh
FROM mart.v_energy_mix_by_year b
WHERE b.[year] = @max_year
ORDER BY b.energy_group, b.is_renewable DESC;

SELECT 'density' AS section, f.[year], COUNT(*) AS rows_mwh
FROM core.v_energy_mix_filtered f
GROUP BY f.[year]
ORDER BY f.[year] DESC;
