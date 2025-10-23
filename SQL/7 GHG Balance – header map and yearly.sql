USE [Vonovia_ESG_DB];
GO
/* Depends on:
   - core.v_ghg_balance_matrix
   - core.v_ghg_balance_long_generic
*/

------------------------------------------------------------
-- 1) Map c05/c07/c09 to real years per workbook
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_ghg_balance_header_map
AS
WITH headers AS (
    SELECT
        m.year_label,
        m.row_num,
        y1 = TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m.c05)), '')),
        y2 = TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m.c07)), '')),
        y3 = TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m.c09)), ''))
    FROM core.v_ghg_balance_matrix AS m
    -- Be tolerant to casing/variants like "Key figures"
    WHERE m.label LIKE N'Key%Figure%'
)
SELECT
    h.year_label,
    y1 = COALESCE(h.y1,
                  TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c05)), ''))),
    y2 = COALESCE(h.y2,
                  TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c07)), ''))),
    y3 = COALESCE(h.y3,
                  TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c09)), '')))
FROM headers h
OUTER APPLY (
    SELECT TOP (1) *
    FROM core.v_ghg_balance_matrix m2
    WHERE m2.year_label = h.year_label
      AND m2.row_num > h.row_num
      AND (
           TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c05)), '')) BETWEEN 2000 AND 2100 OR
           TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c07)), '')) BETWEEN 2000 AND 2100 OR
           TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(m2.c09)), '')) BETWEEN 2000 AND 2100
      )
    ORDER BY m2.row_num
) m2;
GO

------------------------------------------------------------
-- 2) Yearly typed view (robust value parsing)
------------------------------------------------------------
CREATE OR ALTER VIEW core.v_ghg_balance_yearly
AS
WITH base AS (
    SELECT
        m.year_label,
        m.row_num,
        label = m.label,
        unit  = NULLIF(LTRIM(RTRIM(m.c02)), ''),
        m.c05, m.c07, m.c09
    FROM core.v_ghg_balance_matrix m
    WHERE m.label IS NOT NULL
      AND m.label NOT IN (N'Key Figures', N'Key figures', N'Greenhouse Gas Balance (1)')
),
unpivoted AS (
    SELECT
        b.year_label, b.row_num, b.label, b.unit, ca.col_key, ca.value_text
    FROM base b
    CROSS APPLY (VALUES (N'c05', b.c05), (N'c07', b.c07), (N'c09', b.c09)) ca(col_key, value_text)
),
mapped AS (
    SELECT
        u.year_label, u.row_num, u.label, u.unit, u.col_key, u.value_text,
        mapped_year = CASE u.col_key WHEN N'c05' THEN h.y1 WHEN N'c07' THEN h.y2 WHEN N'c09' THEN h.y3 END
    FROM unpivoted u
    INNER JOIN core.v_ghg_balance_header_map h
        ON h.year_label = u.year_label
),
/* Normalize text to a numeric string:
   - replace NBSP/thin space with space, then trim
   - normalize unicode minus to '-'
   - remove spaces and tabs
   - remove thousands separators ('.' or ',') – keep one decimal separator:
       * If there’s both '.' and ',', assume European style and keep ',' as decimal (remove '.').
       * Else if there’s only ',', treat it as decimal and convert to '.'
       * Else keep '.' decimal as is.
*/
norm AS (
    SELECT
        year_label,
        mapped_year AS [year],
        row_num,
        label,
        unit,
        value_text,
        raw = TRIM(REPLACE(REPLACE(value_text, NCHAR(160), N' '), NCHAR(8239), N' '))  -- NBSP & thin space
    FROM mapped
),
clean AS (
    SELECT
        year_label, [year], row_num, label, unit, value_text,
        -- normalize minus and remove spaces/tabs first
        base_txt = REPLACE(
                       REPLACE(
                           REPLACE(raw, N'−', N'-'),
                       NCHAR(9), N''),  -- TAB
                   N' ', N'')
    FROM norm
),
pick AS (
    SELECT
        year_label, [year], row_num, label, unit, value_text,
        -- decide which decimal convention applies and produce a canonical number string using '.'
        num_txt =
            CASE
                WHEN base_txt LIKE '(%' AND base_txt LIKE '%)'
                    THEN '-' + -- parenthesis negative
                         CASE
                             WHEN base_txt LIKE '%,%' AND base_txt LIKE '%.%'
                                 THEN REPLACE( -- assume EU: ',' decimal, '.' thousands
                                         REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), '.', ''),
                                     ',', '.')
                             WHEN base_txt LIKE '%,%'
                                 THEN REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), ',', '.')
                             ELSE REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), ',', '')
                         END
                WHEN base_txt LIKE '%,%' AND base_txt LIKE '%.%'
                    THEN REPLACE(REPLACE(base_txt, '.', ''), ',', '.')    -- EU style
                WHEN base_txt LIKE '%,%'
                    THEN REPLACE(base_txt, ',', '.')                       -- comma as decimal
                ELSE REPLACE(base_txt, ',', '')                             -- plain, or dot decimal
            END
    FROM clean
)
SELECT
    year_label, [year], row_num, label, unit, value_text,
    TRY_CONVERT(decimal(38,4), NULLIF(num_txt, N'')) AS value_num
FROM pick
WHERE [year] BETWEEN 2000 AND 2100
  AND label IS NOT NULL
  AND (value_text IS NOT NULL);
GO

------------------------------------------------------------
-- 3) Optional materialized table & MERGE
------------------------------------------------------------
IF OBJECT_ID('core.ghg_balance_yearly', 'U') IS NULL
BEGIN
    CREATE TABLE core.ghg_balance_yearly
    (
        year_label int NOT NULL,
        [year] int NOT NULL,
        row_num int NOT NULL,
        label nvarchar(400) NOT NULL,
        unit nvarchar(100) NULL,
        value_text nvarchar(200) NULL,
        value_num decimal(38,4) NULL,
        CONSTRAINT PK_core_ghg_balance_yearly PRIMARY KEY (year_label, [year], row_num, label)
    );
END;

;WITH src AS (SELECT * FROM core.v_ghg_balance_yearly)
MERGE core.ghg_balance_yearly AS t
USING src AS s
ON (t.year_label = s.year_label AND t.[year] = s.[year] AND t.row_num = s.row_num AND t.label = s.label)
WHEN MATCHED AND (ISNULL(t.value_num, -999999) <> ISNULL(s.value_num, -999999)
               OR ISNULL(t.value_text, N'') <> ISNULL(s.value_text, N'')
               OR ISNULL(t.unit, N'') <> ISNULL(s.unit, N''))
    THEN UPDATE SET t.unit = s.unit, t.value_text = s.value_text, t.value_num = s.value_num
WHEN NOT MATCHED BY TARGET
    THEN INSERT (year_label, [year], row_num, label, unit, value_text, value_num)
         VALUES (s.year_label, s.[year], s.row_num, s.label, s.unit, s.value_text, s.value_num)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE;
GO

------------------------------------------------------------
-- 4) Quick QA
------------------------------------------------------------
-- Header map should have 3 non-null years per year_label
SELECT * FROM core.v_ghg_balance_header_map;

-- Peek yearly view
SELECT TOP (100) * FROM core.v_ghg_balance_yearly
ORDER BY year_label DESC, [year] DESC, row_num;
GO


SELECT TOP (100) * FROM core.v_ghg_balance_yearly