SET NOCOUNT ON;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_social_hs_filtered);

SELECT TOP (50) 'candidates' AS section, metric_group, metric_type, COUNT(*) AS rows_cnt, SUM(value_num) AS sum_value
FROM core.v_social_hs_filtered
GROUP BY metric_group, metric_type
ORDER BY sum_value DESC;

SELECT 'counts_last15' AS section, c.[year], c.metric, c.value_num
FROM core.v_social_hs_counts_yearly c
WHERE c.[year] >= ISNULL(@max_year,0) - 14
ORDER BY c.[year] DESC, c.metric;

SELECT 'rates_last15' AS section, r.[year], r.rate_metric, r.value_num
FROM core.v_social_hs_rates_yearly r
WHERE r.[year] >= ISNULL(@max_year,0) - 14
ORDER BY r.[year] DESC, r.rate_metric;

SELECT 'density' AS section, f.[year], f.metric_group, COUNT(*) AS rows_cnt
FROM core.v_social_hs_filtered f
GROUP BY f.[year], f.metric_group
ORDER BY f.[year] DESC, f.metric_group;

SELECT TOP (50) 'outliers_counts' AS section, [year], metric_type AS metric, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_hs_filtered
WHERE metric_group='count'
ORDER BY value_num DESC;

SELECT 'rates_box' AS section, rate_metric, MIN(value_num) AS min_val, AVG(value_num) AS avg_val, MAX(value_num) AS max_val
FROM core.v_social_hs_rates_yearly
GROUP BY rate_metric
ORDER BY rate_metric;

SELECT TOP (200) 'sample_recent' AS section, [year], metric_type, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_hs_filtered
WHERE [year] >= ISNULL(@max_year,0) - 4
ORDER BY [year] DESC, value_num DESC;
