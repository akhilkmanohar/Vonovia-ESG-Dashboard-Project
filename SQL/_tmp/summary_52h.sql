;WITH y AS (
    SELECT MAX(y) AS latest_year FROM (
        SELECT MAX([year]) AS y FROM mart.v_gov_counts_by_year
        UNION ALL SELECT MAX([year]) FROM mart.v_gov_rates_by_year
        UNION ALL SELECT MAX([year]) FROM mart.v_gov_amounts_by_year
    ) s
)
SELECT 'counts_total' AS metric, COUNT_BIG(*) AS val FROM mart.v_gov_counts_by_year
UNION ALL SELECT 'rates_total',   COUNT_BIG(*) FROM mart.v_gov_rates_by_year
UNION ALL SELECT 'amounts_total', COUNT_BIG(*) FROM mart.v_gov_amounts_by_year
UNION ALL
SELECT 'counts_latest', COUNT_BIG(*) FROM mart.v_gov_counts_by_year  CROSS JOIN y WHERE [year]=y.latest_year
UNION ALL
SELECT 'rates_latest',  COUNT_BIG(*) FROM mart.v_gov_rates_by_year   CROSS JOIN y WHERE [year]=y.latest_year
UNION ALL
SELECT 'amounts_latest',COUNT_BIG(*) FROM mart.v_gov_amounts_by_year CROSS JOIN y WHERE [year]=y.latest_year;
