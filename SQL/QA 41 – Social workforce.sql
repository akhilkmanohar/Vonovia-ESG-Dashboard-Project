SET NOCOUNT ON;

IF OBJECT_ID('core.v_social_workforce_filtered','V') IS NULL
BEGIN
    RAISERROR('Missing core.v_social_workforce_filtered (run Module S2 script).', 16, 1);
    RETURN;
END;

DECLARE @max_year INT = (SELECT MAX([year]) FROM core.v_social_workforce_filtered);

SELECT TOP (50) 'candidates' AS section,
       CASE WHEN label_norm LIKE '%fte%' THEN 'FTE' ELSE 'Persons' END AS guessed_measure,
       label_norm,
       COUNT(*) AS rows_cnt,
       SUM(value_num) AS sum_value
FROM core.v_social_workforce_filtered
GROUP BY CASE WHEN label_norm LIKE '%fte%' THEN 'FTE' ELSE 'Persons' END, label_norm
ORDER BY sum_value DESC;

SELECT 'yearly_last15' AS section, y.[year], y.measure, y.value_num
FROM core.v_social_workforce_yearly y
WHERE y.[year] >= ISNULL(@max_year,0) - 14
ORDER BY y.[year] DESC, y.measure;

SELECT 'density' AS section, f.[year], f.measure, COUNT(*) AS rows_cnt, SUM(f.value_num) AS sum_value
FROM core.v_social_workforce_filtered f
GROUP BY f.[year], f.measure
ORDER BY f.[year] DESC, f.measure;

SELECT TOP (50) 'outliers' AS section, [year], measure, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_workforce_filtered
ORDER BY value_num DESC;

SELECT TOP (150) 'sample_recent' AS section, [year], measure, value_num, LEFT(label_raw, 180) AS label_raw
FROM core.v_social_workforce_filtered
WHERE [year] >= ISNULL(@max_year,0) - 4
ORDER BY [year] DESC, value_num DESC;
