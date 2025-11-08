SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core') EXEC('CREATE SCHEMA core;');

IF OBJECT_ID('stg.v_raw_all_with_cat','V') IS NULL
BEGIN
    RAISERROR('Required source view stg.v_raw_all_with_cat not found.', 16, 1);
    RETURN;
END;

DECLARE @src sysname = N'stg.v_raw_all_with_cat';
DECLARE @obj_id int = OBJECT_ID(@src, 'V');

DECLARE @label_list nvarchar(max);
SELECT @label_list = STUFF((
    SELECT ', ' + 'NULLIF(CONVERT(nvarchar(4000), ' + QUOTENAME(c.name) + '), '''')'
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = @obj_id
      AND t.name IN ('nvarchar','varchar','nchar','char','text','ntext','sysname')
    ORDER BY c.column_id
    FOR XML PATH(''), TYPE
).value('.','nvarchar(max)'), 1, 2, '');

DECLARE @label_expr nvarchar(max) = N'''#nolabel''';
IF @label_list IS NOT NULL AND @label_list <> ''
    SET @label_expr = N'NULLIF(LTRIM(RTRIM(CONCAT_WS('' '', ' + @label_list + '))), '''')';

DECLARE @category_expr nvarchar(max);
SELECT TOP (1) @category_expr = N'LOWER(CONVERT(nvarchar(4000), ' + QUOTENAME(c.name) + N' COLLATE Latin1_General_100_CI_AI))'
FROM sys.columns c
WHERE c.object_id = @obj_id
  AND (
        c.name LIKE '%category%' OR c.name LIKE '%kategorie%' OR c.name LIKE '%cat%'
        OR c.name LIKE '%pillar%' OR c.name LIKE '%dimension%' OR c.name LIKE '%bereich%'
        OR c.name LIKE '%theme%'
      )
ORDER BY CASE
            WHEN c.name LIKE '%category%' THEN 1
            WHEN c.name LIKE '%kategorie%' THEN 2
            WHEN c.name LIKE '%cat%' THEN 3
            WHEN c.name LIKE '%pillar%' THEN 4
            WHEN c.name LIKE '%dimension%' THEN 5
            WHEN c.name LIKE '%bereich%' THEN 6
            WHEN c.name LIKE '%theme%' THEN 7
            ELSE 99
         END, c.column_id;
IF @category_expr IS NULL SET @category_expr = N'NULL';

DECLARE @year_expr nvarchar(max);
SELECT TOP (1) @year_expr = N'TRY_CONVERT(int, ' + QUOTENAME(c.name) + N')'
FROM sys.columns c
WHERE c.object_id = @obj_id AND (c.name LIKE '%year%' OR c.name LIKE '%jahr%')
ORDER BY CASE
            WHEN c.name = 'year' THEN 1
            WHEN c.name LIKE '%year%' THEN 2
            WHEN c.name LIKE '%jahr%' THEN 3
            ELSE 9
         END, c.column_id;
IF @year_expr IS NULL SET @year_expr = N'NULL';

DECLARE @pref TABLE(n nvarchar(50));
INSERT INTO @pref(n) VALUES
    (N'value'),(N'amount'),(N'anzahl'),(N'count'),(N'fte'),(N'hours'),(N'stunden'),
    (N'personen'),(N'cases'),(N'faelle'),(N'rate'),(N'quote');

DECLARE @numeric TABLE(name sysname, ord int);
INSERT INTO @numeric(name, ord)
SELECT
    c.name,
    ROW_NUMBER() OVER (
        ORDER BY CASE WHEN EXISTS (SELECT 1 FROM @pref p WHERE LOWER(c.name) LIKE '%' + p.n + '%') THEN 0 ELSE 1 END,
                 c.column_id
    )
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = @obj_id
  AND t.name IN ('int','bigint','smallint','tinyint','decimal','numeric','float','real','money','smallmoney');

DECLARE @num_list nvarchar(max);
DECLARE @num_count int = (SELECT COUNT(*) FROM @numeric);
DECLARE @num_expr nvarchar(max) = N'NULL';
IF @num_count > 0
BEGIN
    SELECT @num_list = STUFF((
        SELECT ', ' + 'TRY_CONVERT(decimal(38,6), ' + QUOTENAME(name) + ')'
        FROM @numeric
        ORDER BY ord
        FOR XML PATH(''), TYPE
    ).value('.','nvarchar(max)'), 1, 2, '');

    IF @num_count = 1
        SET @num_expr = @num_list;
    ELSE
        SET @num_expr = N'COALESCE(' + @num_list + N')';
END

DECLARE @sql nvarchar(max) = N'
CREATE OR ALTER VIEW core.v_social_discovery AS
SELECT
    year_guess    = ' + @year_expr + N',
    category_norm = ' + @category_expr + N',
    label_raw     = lbl.label_raw,
    label_norm    = CASE WHEN lbl.label_raw IS NULL THEN NULL
                         ELSE LOWER(REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(4000), lbl.label_raw) COLLATE Latin1_General_100_CI_AI,
                                                     NCHAR(228), N''a''), NCHAR(246), N''o''), NCHAR(252), N''u''), NCHAR(223), N''ss'')) END,
    value_pref    = ' + @num_expr + N'
FROM ' + @src + N'
CROSS APPLY (VALUES(' + @label_expr + N')) AS lbl(label_raw);
';
EXEC sys.sp_executesql @sql;

DECLARE @sqlTokens nvarchar(max) = N'
CREATE OR ALTER VIEW core.v_social_discovery_tokens AS
WITH base AS (
    SELECT year_guess, value_pref, label_norm
    FROM core.v_social_discovery
), clean AS (
    SELECT year_guess, value_pref,
           txt = LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                 REPLACE(REPLACE(label_norm, __Q__.__Q__, __Q__ __Q__), __Q__,__Q__, __Q__ __Q__), __Q__;__Q__, __Q__ __Q__), __Q__:__Q__, __Q__ __Q__), __Q__/__Q__, __Q__ __Q__), __Q__\__Q__, __Q__ __Q__),
                 __Q__(__Q__, __Q__ __Q__), __Q__)__Q__, __Q__ __Q__), __Q__-__Q__, __Q__ __Q__))
    FROM base
    WHERE label_norm IS NOT NULL AND label_norm <> __Q____Q__
), split AS (
    SELECT year_guess, value_pref, LTRIM(RTRIM(s.value)) AS token
    FROM clean c
    CROSS APPLY STRING_SPLIT(c.txt, __Q__ __Q__) s
)
SELECT
    token,
    years_covered = COUNT(DISTINCT year_guess),
    hits          = COUNT_BIG(*),
    sum_value     = SUM(value_pref)
FROM split
WHERE token IS NOT NULL
  AND token <> __Q____Q__
  AND LEN(token) >= 3
  AND token NOT LIKE __Q__%[0-9]%__Q__
  AND token NOT IN (__Q__und__Q__,__Q__oder__Q__,__Q__der__Q__,__Q__die__Q__,__Q__das__Q__,__Q__ein__Q__,__Q__eine__Q__,__Q__ist__Q__,__Q__von__Q__,__Q__im__Q__,__Q__in__Q__,__Q__am__Q__,__Q__an__Q__,__Q__zum__Q__,__Q__zur__Q__,__Q__mit__Q__,__Q__ohne__Q__,__Q__per__Q__,__Q__pro__Q__,__Q__je__Q__)
GROUP BY token;
';
SET @sqlTokens = REPLACE(@sqlTokens, '__Q__', '''');
EXEC sys.sp_executesql @sqlTokens;

SELECT TOP (20) * FROM core.v_social_discovery ORDER BY year_guess DESC;
SELECT TOP (20) * FROM core.v_social_discovery_tokens ORDER BY ISNULL(sum_value, 0) DESC, hits DESC;
