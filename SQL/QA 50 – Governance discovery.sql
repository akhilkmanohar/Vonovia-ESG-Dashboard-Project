SET NOCOUNT ON;

SELECT 'density' AS section, [year], COUNT_BIG(*) AS rows
FROM core.v_gov_discovery
GROUP BY [year]
ORDER BY [year];

SELECT TOP 50 'top_tokens' AS section, token, hits, years_covered, latest_year
FROM core.v_gov_discovery_tokens
ORDER BY hits DESC, token;

SELECT TOP 200 'recent_samples' AS section, [year], source, sheet, row_label, value_num, derived_unit
FROM core.v_gov_discovery
WHERE [year] >= ISNULL((SELECT MAX([year]) FROM core.v_gov_discovery), 0) - 5
ORDER BY [year] DESC, value_num DESC;

SELECT 'high_values' AS section, *
FROM core.v_gov_recent_high_values;

SELECT 'smoke_exists' AS section, 'core.v_gov_discovery' AS view_name, COUNT_BIG(*) AS rows FROM core.v_gov_discovery
UNION ALL SELECT 'smoke_exists','core.v_gov_discovery_tokens', COUNT_BIG(*) FROM core.v_gov_discovery_tokens
UNION ALL SELECT 'smoke_exists','core.v_gov_recent_high_values', COUNT_BIG(*) FROM core.v_gov_recent_high_values;

SELECT TOP 10 'peek_latest' AS section, [year], row_label, value_num, derived_unit
FROM core.v_gov_discovery
ORDER BY [year] DESC, value_num DESC;
