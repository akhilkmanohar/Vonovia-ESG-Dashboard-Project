SET NOCOUNT ON;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_social_training_filtered);

SELECT TOP (50) 'candidates' AS section, metric_group, metric_type, COUNT(*) AS rows_cnt, SUM(value_num) AS sum_value
FROM core.v_social_training_filtered
GROUP BY metric_group, metric_type
ORDER BY sum_value DESC;

SELECT 'last15' AS section, y.[year], y.metric, y.value_num
FROM core.v_social_training_yearly y
WHERE y.[year] >= ISNULL(@max_year,0) - 14
ORDER BY y.[year] DESC, y.metric;

SELECT 'density' AS section, f.[year], f.metric_group, COUNT(*) AS rows_cnt
FROM core.v_social_training_filtered f
GROUP BY f.[year], f.metric_group
ORDER BY f.[year] DESC, f.metric_group;

SELECT TOP (50) 'outliers_counts' AS section, [year], metric_type AS metric, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_training_filtered
WHERE metric_group = 'count'
ORDER BY value_num DESC;

SELECT 'rates_box' AS section, 'hours_per_employee' AS metric,
       MIN(CAST(value_num AS float)) AS min_val,
       AVG(CAST(value_num AS float)) AS avg_val,
       MAX(CAST(value_num AS float)) AS max_val
FROM core.v_social_training_filtered
WHERE metric_type = 'hours_per_employee';

SELECT TOP (150) 'sample_recent' AS section, [year], metric_type, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_training_filtered
WHERE [year] >= ISNULL(@max_year,0) - 4
ORDER BY [year] DESC, value_num DESC;

