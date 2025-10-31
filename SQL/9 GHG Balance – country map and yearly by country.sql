USE [Vonovia_ESG_DB];
GO
/* File: 9 GHG Balance – country map and yearly by country.sql
   Purpose:
     - Detect DE/AT/SE captions in the GHG Balance sheet (right-side block)
     - Produce a tidy yearly-by-country fact with parsed numbers and units
   Depends on:
     - core.v_ghg_balance_matrix
     - core.v_ghg_balance_header_map
     - core.sheet_units (unit override)
*/

------------------------------------------------------------
-- 1) Country header map
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_ghg_balance_country_map
AS
WITH cand AS (
    SELECT
        m.year_label,
        m.row_num,
        c13 = NULLIF(LTRIM(RTRIM(m.c13)), N''),
        c15 = NULLIF(LTRIM(RTRIM(m.c15)), N''),
        c17 = NULLIF(LTRIM(RTRIM(m.c17)), N'')
    FROM core.v_ghg_balance_matrix m
),
hit AS (
    SELECT DISTINCT
        year_label, col_key, caption
    FROM cand
    CROSS APPLY (VALUES (N'c13', c13), (N'c15', c15), (N'c17', c17)) v(col_key, caption)
    WHERE caption IS NOT NULL
)
SELECT
    year_label,
    col_key,
    country_name =
        CASE
            WHEN caption LIKE N'%Germany%' OR caption LIKE N'%Deutschland%' THEN N'Germany'
            WHEN caption LIKE N'%Austria%'  OR caption LIKE N'%Österreich%' THEN N'Austria'
            WHEN caption LIKE N'%Sweden%'   OR caption LIKE N'%Schweden%'    THEN N'Sweden'
            ELSE caption
        END
FROM hit
WHERE (caption LIKE N'%Germany%' OR caption LIKE N'%Deutschland%'
    OR caption LIKE N'%Austria%'  OR caption LIKE N'%Österreich%'
    OR caption LIKE N'%Sweden%'   OR caption LIKE N'%Schweden%');
GO

------------------------------------------------------------
-- 2) Country dim (inline)
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_country_dim_inline
AS
SELECT * FROM (VALUES
    (N'Germany', N'DE'),
    (N'Austria', N'AT'),
    (N'Sweden',  N'SE')
) v(country_name, iso2);
GO

------------------------------------------------------------
-- 3) Yearly-by-country tidy view
--    NOTE: many reports show the country split for the latest year only.
--    We therefore map this split to MAX(y1,y2,y3) for the workbook.
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_ghg_balance_yearly_country
AS
WITH base AS (
    SELECT
        m.year_label, m.row_num,
        label = m.label,
        unit  = NULLIF(LTRIM(RTRIM(m.c02)), N''),
        m.c13, m.c15, m.c17
    FROM core.v_ghg_balance_matrix m
    WHERE m.label IS NOT NULL
      AND m.label NOT IN (N'Key Figures', N'Key figures', N'Greenhouse Gas Balance (1)')
),
unpvt AS (
    SELECT b.year_label, b.row_num, b.label, b.unit, v.col_key, v.value_text
    FROM base b
    CROSS APPLY (VALUES (N'c13', b.c13),(N'c15', b.c15),(N'c17', b.c17)) v(col_key, value_text)
),
mapped AS (
    SELECT u.year_label, u.row_num, u.label, u.unit, u.col_key, u.value_text, cm.country_name
    FROM unpvt u
    INNER JOIN core.v_ghg_balance_country_map cm
      ON cm.year_label = u.year_label AND cm.col_key = u.col_key
),
norm AS (
    SELECT
        m.year_label,
        m.row_num, m.label, m.unit, m.value_text, m.country_name,
        raw = TRIM(REPLACE(REPLACE(m.value_text, NCHAR(160), N' '), NCHAR(8239), N' '))
    FROM mapped m
),
clean AS (
    SELECT
        n.year_label, n.row_num, n.label, n.unit, n.country_name, n.value_text,
        base_txt = REPLACE(REPLACE(REPLACE(n.raw, N'−', N'-'), NCHAR(9), N''), N' ', N'')
    FROM norm n
),
numtxt AS (
    SELECT
        c.year_label,
        c.row_num,
        c.label,
        c.unit,
        c.country_name,
        c.value_text,
        num_txt =
            CASE
                WHEN c.base_txt LIKE '(%' AND c.base_txt LIKE '%)'
                    THEN '-' +
                         CASE
                             WHEN c.base_txt LIKE '%,%' AND c.base_txt LIKE '%.%' THEN REPLACE(REPLACE(SUBSTRING(c.base_txt, 2, LEN(c.base_txt)-2), '.', ''), ',', '.')
                             WHEN c.base_txt LIKE '%,%'                          THEN REPLACE(SUBSTRING(c.base_txt, 2, LEN(c.base_txt)-2), ',', '.')
                             ELSE REPLACE(SUBSTRING(c.base_txt, 2, LEN(c.base_txt)-2), ',', '')
                         END
                WHEN c.base_txt LIKE '%,%' AND c.base_txt LIKE '%.%' THEN REPLACE(REPLACE(c.base_txt, '.', ''), ',', '.')
                WHEN c.base_txt LIKE '%,%'                          THEN REPLACE(c.base_txt, ',', '.')
                ELSE REPLACE(c.base_txt, ',', '')
            END
    FROM clean c
)
SELECT
    n.year_label,
    [year] = (SELECT MAX(v) FROM (VALUES (h.y1),(h.y2),(h.y3)) AS t(v)),
    n.row_num,
    n.label,
    COALESCE(NULLIF(n.unit,N''), su_exact.unit, su_any.unit) AS unit,
    n.country_name,
    cd.iso2,
    n.value_text,
    TRY_CONVERT(decimal(38,4), NULLIF(n.num_txt, N'')) AS value_num
FROM numtxt n
INNER JOIN core.v_ghg_balance_header_map h ON h.year_label = n.year_label
LEFT JOIN core.v_country_dim_inline cd     ON cd.country_name = n.country_name
LEFT JOIN core.sheet_units su_exact
       ON su_exact.sheet_name = N'Greenhouse Gas Balance' AND su_exact.year_label = n.year_label
LEFT JOIN core.sheet_units su_any
       ON su_any.sheet_name = N'Greenhouse Gas Balance' AND su_any.year_label = -1
WHERE n.value_text IS NOT NULL;
GO

------------------------------------------------------------
-- 4) Materialize table & MERGE
------------------------------------------------------------
IF OBJECT_ID('core.ghg_balance_yearly_country', 'U') IS NULL
BEGIN
    CREATE TABLE core.ghg_balance_yearly_country
    (
        year_label int NOT NULL,
        [year] int NOT NULL,
        row_num int NOT NULL,
        label nvarchar(400) NOT NULL,
        unit nvarchar(100) NULL,
        country_name nvarchar(100) NOT NULL,
        iso2 char(2) NULL,
        value_text nvarchar(200) NULL,
        value_num decimal(38,4) NULL,
        CONSTRAINT PK_core_ghg_balance_yearly_country PRIMARY KEY (year_label, [year], row_num, country_name)
    );
END;

;WITH src AS (SELECT * FROM core.v_ghg_balance_yearly_country)
MERGE core.ghg_balance_yearly_country AS t
USING src AS s
ON (t.year_label = s.year_label AND t.[year] = s.[year] AND t.row_num = s.row_num AND t.country_name = s.country_name)
WHEN MATCHED AND (ISNULL(t.value_num, -999999) <> ISNULL(s.value_num, -999999)
               OR ISNULL(t.value_text, N'') <> ISNULL(s.value_text, N'')
               OR ISNULL(t.unit, N'') <> ISNULL(s.unit, N'')
               OR ISNULL(t.iso2, N'') <> ISNULL(s.iso2, N''))
    THEN UPDATE SET t.unit = s.unit, t.value_text = s.value_text, t.value_num = s.value_num, t.iso2 = s.iso2
WHEN NOT MATCHED BY TARGET
    THEN INSERT (year_label, [year], row_num, label, unit, country_name, iso2, value_text, value_num)
         VALUES (s.year_label, s.[year], s.row_num, s.label, s.unit, s.country_name, s.iso2, s.value_text, s.value_num)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
GO

-- Peek
SELECT TOP (30) * FROM core.v_ghg_balance_yearly_country
ORDER BY year_label DESC, [year] DESC, country_name, row_num;
GO
