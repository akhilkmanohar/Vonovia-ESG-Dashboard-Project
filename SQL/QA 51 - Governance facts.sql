SET NOCOUNT ON;

SELECT 'candidates' AS section,
       measure_group,
       SUM(CASE WHEN metric_type = 'count'  THEN 1 ELSE 0 END) AS n_count_rows,
       SUM(CASE WHEN metric_type = 'rate'   THEN 1 ELSE 0 END) AS n_rate_rows,
       SUM(CASE WHEN metric_type = 'amount' THEN 1 ELSE 0 END) AS n_amount_rows
FROM core.v_gov_filtered
GROUP BY measure_group
ORDER BY measure_group;

SELECT 'counts_by_year' AS section, [year], measure_group, value_num
FROM mart.v_gov_counts_by_year
WHERE [year] >= ISNULL((SELECT MAX([year]) FROM mart.v_gov_counts_by_year), 0) - 15
ORDER BY [year], measure_group;

SELECT 'rates_by_year' AS section, [year], rate_metric, value_num
FROM mart.v_gov_rates_by_year
WHERE [year] >= ISNULL((SELECT MAX([year]) FROM mart.v_gov_rates_by_year), 0) - 15
ORDER BY [year], rate_metric;

SELECT 'amounts_by_year' AS section, [year], measure_group, amount_eur
FROM mart.v_gov_amounts_by_year
WHERE [year] >= ISNULL((SELECT MAX([year]) FROM mart.v_gov_amounts_by_year), 0) - 15
ORDER BY [year], measure_group;

SELECT 'density' AS section, [year], COUNT_BIG(*) AS rows
FROM core.v_gov_discovery
GROUP BY [year]
ORDER BY [year];

WITH f AS (
    SELECT
        [year],
        measure_group,
        metric_type,
        value_num,
        ABS(TRY_CONVERT(decimal(38,6), value_num)) AS abs_v
    FROM core.v_gov_filtered
    WHERE metric_type = 'count'
)
SELECT TOP (50)
    'top_outliers' AS section,
    [year],
    measure_group,
    value_num
FROM f
ORDER BY abs_v DESC;

SELECT 'latest_peek' AS section, stream, metric, value_num, [year]
FROM rpt.v_gov_cards_latest
ORDER BY stream, value_num DESC;
