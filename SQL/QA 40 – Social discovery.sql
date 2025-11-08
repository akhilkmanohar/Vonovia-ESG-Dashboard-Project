SET NOCOUNT ON;

IF OBJECT_ID('stg.v_raw_all_with_cat','V') IS NULL
BEGIN
    RAISERROR('Required source view stg.v_raw_all_with_cat not found.', 16, 1);
    RETURN;
END;

-- SECTION: columns_meta (string vs numeric available in source)
SELECT 'columns_meta' AS section, c.name, t.name AS type_name
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('stg.v_raw_all_with_cat','V')
ORDER BY CASE WHEN t.name IN ('nvarchar','varchar','nchar','char','text','ntext','sysname') THEN 0 ELSE 1 END, c.column_id;

-- SECTION: density_by_year
SELECT 'density_by_year' AS section, year_guess, COUNT(*) AS rows_cnt, SUM(value_pref) AS sum_value_pref
FROM core.v_social_discovery
GROUP BY year_guess
ORDER BY year_guess DESC;

-- SECTION: sample_rows (recent 200)
SELECT TOP (200) 'sample_rows' AS section, year_guess, category_norm, LEFT(label_raw, 180) AS label_raw, value_pref
FROM core.v_social_discovery
ORDER BY year_guess DESC;

-- SECTION: signal_tokens (workforce/H&S/training candidates)
SELECT TOP (100) 'signal_tokens' AS section, token, years_covered, hits, sum_value
FROM core.v_social_discovery_tokens
WHERE token IN (
    'headcount','mitarbeiter','mitarbeitende','beschaeftigte','personal','belegschaft','fte','anzahl',
    'hire','hires','einstellungen','austritte','exits','fluktuation','turnover',
    'unfall','unfaelle','verletzung','incident','accident','ltir','trir','lost','days',
    'training','schulung','stunden','hours','teilnehmer','participants'
)
ORDER BY sum_value DESC, hits DESC;

-- SECTION: top_labels (most frequent labels)
SELECT TOP (100) 'top_labels' AS section, label_norm, COUNT(*) AS cnt
FROM core.v_social_discovery
GROUP BY label_norm
ORDER BY cnt DESC;

-- SECTION: recent_candidates (last 5y rows with non-null value)
DECLARE @max_year INT = (SELECT MAX(year_guess) FROM core.v_social_discovery);
SELECT TOP (300) 'recent_candidates' AS section, year_guess, LEFT(label_raw, 180) AS label_raw, value_pref
FROM core.v_social_discovery
WHERE value_pref IS NOT NULL AND year_guess >= ISNULL(@max_year,0) - 4
ORDER BY year_guess DESC, value_pref DESC;
