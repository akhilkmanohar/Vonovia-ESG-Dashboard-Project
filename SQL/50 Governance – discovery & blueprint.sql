SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core');
GO

IF OBJECT_ID('core.v_gov_discovery','V') IS NULL EXEC('CREATE VIEW core.v_gov_discovery AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW core.v_gov_discovery
AS
WITH base AS (
    SELECT
        TRY_CONVERT(int, s.year_label) AS year,
        LOWER(CONVERT(nvarchar(200), s.category COLLATE Latin1_General_100_CI_AI)) AS category_norm,
        s.source_file,
        s.sheet_name,
        s.row_num,
        s.c01, s.c02, s.c03, s.c04, s.c05, s.c06, s.c07, s.c08,
        s.c09, s.c10, s.c11, s.c12, s.c13, s.c14, s.c15, s.c16,
        s.c17, s.c18, s.c19, s.c20, s.c21, s.c22, s.c23, s.c24,
        s.c25, s.c26, s.c27, s.c28
    FROM stg.v_raw_all_with_cat s
    WHERE s.category IS NOT NULL
      AND LOWER(CONVERT(nvarchar(200), s.category COLLATE Latin1_General_100_CI_AI)) LIKE '%governance%'
),
text_values AS (
    SELECT
        b.year,
        b.category_norm,
        b.source_file,
        b.sheet_name,
        b.row_num,
        v.ord,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(4000), v.val))), '') AS val_clean,
        TRY_CONVERT(decimal(38,6), v.val) AS val_num
    FROM base b
    CROSS APPLY (VALUES
        (1, b.c01),(2, b.c02),(3, b.c03),(4, b.c04),(5, b.c05),(6, b.c06),(7, b.c07),(8, b.c08),
        (9, b.c09),(10, b.c10),(11, b.c11),(12, b.c12),(13, b.c13),(14, b.c14),(15, b.c15),(16, b.c16),
        (17, b.c17),(18, b.c18),(19, b.c19),(20, b.c20),(21, b.c21),(22, b.c22),(23, b.c23),(24, b.c24),
        (25, b.c25),(26, b.c26),(27, b.c27),(28, b.c28)
    ) AS v(ord, val)
),
labels AS (
    SELECT
        tv.year,
        tv.category_norm,
        tv.source_file,
        tv.sheet_name,
        tv.row_num,
        label_text = STUFF((
            SELECT ' | ' + tv2.val_clean
            FROM text_values tv2
            WHERE tv2.year = tv.year
              AND tv2.row_num = tv.row_num
              AND tv2.source_file = tv.source_file
              AND tv2.sheet_name = tv.sheet_name
              AND tv2.val_clean IS NOT NULL
              AND tv2.val_num IS NULL
            ORDER BY tv2.ord
            FOR XML PATH(''), TYPE
        ).value('.','nvarchar(max)'), 1, 3, '')
    FROM text_values tv
    GROUP BY tv.year, tv.category_norm, tv.source_file, tv.sheet_name, tv.row_num
),
first_numeric AS (
    SELECT DISTINCT
        tv.year,
        tv.category_norm,
        tv.source_file,
        tv.sheet_name,
        tv.row_num,
        value_num = FIRST_VALUE(tv.val_num) OVER (
            PARTITION BY tv.year, tv.category_norm, tv.source_file, tv.sheet_name, tv.row_num
            ORDER BY tv.ord
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    FROM text_values tv
    WHERE tv.val_num IS NOT NULL
),
unit_candidates AS (
    SELECT DISTINCT
        tv.year,
        tv.category_norm,
        tv.source_file,
        tv.sheet_name,
        tv.row_num,
        unit_text = FIRST_VALUE(tv.val_clean) OVER (
            PARTITION BY tv.year, tv.category_norm, tv.source_file, tv.sheet_name, tv.row_num
            ORDER BY tv.ord
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    FROM text_values tv
    WHERE tv.val_clean IS NOT NULL
      AND tv.val_num IS NULL
      AND (
            tv.val_clean IN ('%', 'percent', 'percentage', 'eur', 'euro', 'count', 'ratio', 'index', 'score')
            OR tv.val_clean LIKE '% per %'
            OR tv.val_clean LIKE '% rate%'
            OR tv.val_clean LIKE '% share%'
          )
)
SELECT
    b.year,
    b.category_norm AS category,
    b.source_file AS source,
    b.sheet_name AS sheet,
    lbl.label_text AS row_label,
    num.value_num,
    unit.unit_text AS derived_unit,
    LOWER(CONCAT_WS(' ',
        NULLIF(LTRIM(RTRIM(lbl.label_text)),''),
        NULLIF(LTRIM(RTRIM(b.sheet_name)),''),
        NULLIF(LTRIM(RTRIM(b.source_file)),'')
    )) COLLATE Latin1_General_100_CI_AI AS label_norm
FROM base b
LEFT JOIN labels lbl
    ON lbl.year = b.year
   AND lbl.category_norm = b.category_norm
   AND lbl.source_file = b.source_file
   AND lbl.sheet_name = b.sheet_name
   AND lbl.row_num = b.row_num
LEFT JOIN first_numeric num
    ON num.year = b.year
   AND num.category_norm = b.category_norm
   AND num.source_file = b.source_file
   AND num.sheet_name = b.sheet_name
   AND num.row_num = b.row_num
LEFT JOIN unit_candidates unit
    ON unit.year = b.year
   AND unit.category_norm = b.category_norm
   AND unit.source_file = b.source_file
   AND unit.sheet_name = b.sheet_name
   AND unit.row_num = b.row_num;
GO

IF OBJECT_ID('core.v_gov_discovery_tokens','V') IS NULL EXEC('CREATE VIEW core.v_gov_discovery_tokens AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW core.v_gov_discovery_tokens
AS
WITH src AS (
    SELECT [year], label_norm
    FROM core.v_gov_discovery
    WHERE label_norm IS NOT NULL
),
clean AS (
    SELECT s.[year], r21.txt AS txt
    FROM src s
    CROSS APPLY (SELECT REPLACE(s.label_norm, '.', ' ') AS txt) r1
    CROSS APPLY (SELECT REPLACE(r1.txt, ',', ' ') AS txt) r2
    CROSS APPLY (SELECT REPLACE(r2.txt, ';', ' ') AS txt) r3
    CROSS APPLY (SELECT REPLACE(r3.txt, ':', ' ') AS txt) r4
    CROSS APPLY (SELECT REPLACE(r4.txt, '/', ' ') AS txt) r5
    CROSS APPLY (SELECT REPLACE(r5.txt, '\\', ' ') AS txt) r6
    CROSS APPLY (SELECT REPLACE(r6.txt, '(', ' ') AS txt) r7
    CROSS APPLY (SELECT REPLACE(r7.txt, ')', ' ') AS txt) r8
    CROSS APPLY (SELECT REPLACE(r8.txt, '[', ' ') AS txt) r9
    CROSS APPLY (SELECT REPLACE(r9.txt, ']', ' ') AS txt) r10
    CROSS APPLY (SELECT REPLACE(r10.txt, '{', ' ') AS txt) r11
    CROSS APPLY (SELECT REPLACE(r11.txt, '}', ' ') AS txt) r12
    CROSS APPLY (SELECT REPLACE(r12.txt, NCHAR(34), ' ') AS txt) r13
    CROSS APPLY (SELECT REPLACE(r13.txt, NCHAR(39), ' ') AS txt) r14
    CROSS APPLY (SELECT REPLACE(r14.txt, '%', ' ') AS txt) r15
    CROSS APPLY (SELECT REPLACE(r15.txt, '+', ' ') AS txt) r16
    CROSS APPLY (SELECT REPLACE(r16.txt, '-', ' ') AS txt) r17
    CROSS APPLY (SELECT REPLACE(r17.txt, '_', ' ') AS txt) r18
    CROSS APPLY (SELECT REPLACE(r18.txt, '#', ' ') AS txt) r19
    CROSS APPLY (SELECT REPLACE(r19.txt, '@', ' ') AS txt) r20
    CROSS APPLY (SELECT REPLACE(r20.txt, '!', ' ') AS txt) r21
),
tokens AS (
    SELECT c.[year], LTRIM(RTRIM(value)) AS token
    FROM clean c
    CROSS APPLY STRING_SPLIT(c.txt, ' ')
)
SELECT
    LOWER(token)           AS token,
    COUNT_BIG(*)           AS hits,
    COUNT(DISTINCT [year]) AS years_covered,
    MAX([year])            AS latest_year
FROM tokens
WHERE token IS NOT NULL
  AND LEN(token) >= 3
  AND token NOT LIKE '%[0-9]%'
  AND token NOT IN (N'und',N'and',N'der',N'die',N'das',N'the',N'von',N'für',N'per',N'pro',N'mit',N'bei',N'zum',N'zur',N'aus',N'auf',N'in',N'an')
GROUP BY LOWER(token)
HAVING COUNT_BIG(*) >= 2;
GO

IF OBJECT_ID('core.v_gov_recent_high_values','V') IS NULL EXEC('CREATE VIEW core.v_gov_recent_high_values AS SELECT 1 AS stub');
GO
CREATE OR ALTER VIEW core.v_gov_recent_high_values
AS
WITH snapshot AS (
    SELECT MAX([year]) AS y_max
    FROM core.v_gov_discovery
    WHERE value_num IS NOT NULL
)
SELECT TOP (200)
    g.[year],
    g.source,
    g.sheet,
    g.row_label,
    g.value_num,
    g.derived_unit
FROM core.v_gov_discovery g
CROSS JOIN snapshot s
WHERE g.value_num IS NOT NULL
  AND (s.y_max IS NULL OR g.[year] >= s.y_max - 5)
ORDER BY g.value_num DESC;
GO
