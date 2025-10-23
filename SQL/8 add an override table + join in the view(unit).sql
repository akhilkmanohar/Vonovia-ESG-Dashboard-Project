


USE [Vonovia_ESG_DB];
GO

-- 1) Create a tiny reference table (idempotent)
IF OBJECT_ID('core.sheet_units', 'U') IS NULL
BEGIN
    CREATE TABLE core.sheet_units
    (
        sheet_name   nvarchar(200) NOT NULL,
        year_label   int           NOT NULL,  -- use -1 as "all years"
        unit         nvarchar(100) NOT NULL,
        CONSTRAINT PK_core_sheet_units PRIMARY KEY (sheet_name, year_label)
    );
END
GO

-- 2) (Optional) seed an example row.
--    TODO: replace 't CO2e' with the real wording you confirm from the source sheet.
--    If it should apply to all files, use year_label = -1.
IF NOT EXISTS (
    SELECT 1 FROM core.sheet_units
    WHERE sheet_name = N'Greenhouse Gas Balance' AND year_label = -1
)
BEGIN
    INSERT INTO core.sheet_units (sheet_name, year_label, unit)
    VALUES (N'Greenhouse Gas Balance', -1, N't CO2e');  -- <- adjust if needed
END
GO

-- 3) Re-create the yearly view to expose a derived unit
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
        base_txt = REPLACE(REPLACE(REPLACE(raw, N'−', N'-'), NCHAR(9), N''), N' ', N'')
    FROM norm
),
pick AS (
    SELECT
        year_label, [year], row_num, label, unit, value_text,
        num_txt =
            CASE
                WHEN base_txt LIKE '(%' AND base_txt LIKE '%)'
                    THEN '-' +
                         CASE
                             WHEN base_txt LIKE '%,%' AND base_txt LIKE '%.%' THEN REPLACE(REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), '.', ''), ',', '.')
                             WHEN base_txt LIKE '%,%'                          THEN REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), ',', '.')
                             ELSE REPLACE(SUBSTRING(base_txt, 2, LEN(base_txt)-2), ',', '')
                         END
                WHEN base_txt LIKE '%,%' AND base_txt LIKE '%.%' THEN REPLACE(REPLACE(base_txt, '.', ''), ',', '.')
                WHEN base_txt LIKE '%,%'                          THEN REPLACE(base_txt, ',', '.')
                ELSE REPLACE(base_txt, ',', '')
            END
    FROM clean
)
SELECT
    p.year_label,
    p.[year],
    p.row_num,
    p.label,
    -- Use row-level unit if present, else sheet-level override for this year, else override for all years (-1)
    derived_unit = COALESCE(
        NULLIF(p.unit, N''),
        su_exact.unit,
        su_any.unit
    ),
    p.value_text,
    TRY_CONVERT(decimal(38,4), NULLIF(p.num_txt, N'')) AS value_num
FROM pick p
LEFT JOIN core.sheet_units su_exact
       ON su_exact.sheet_name = N'Greenhouse Gas Balance'
      AND su_exact.year_label = p.year_label
LEFT JOIN core.sheet_units su_any
       ON su_any.sheet_name = N'Greenhouse Gas Balance'
      AND su_any.year_label = -1
WHERE p.[year] BETWEEN 2000 AND 2100
  AND p.label IS NOT NULL
  AND p.value_text IS NOT NULL;
GO

-- 4) Refresh the materialized table from the updated view (optional)
;WITH src AS (SELECT * FROM core.v_ghg_balance_yearly)
MERGE core.ghg_balance_yearly AS t
USING src AS s
ON (t.year_label = s.year_label AND t.[year] = s.[year] AND t.row_num = s.row_num AND t.label = s.label)
WHEN MATCHED AND (ISNULL(t.value_num, -999999) <> ISNULL(s.value_num, -999999)
               OR ISNULL(t.value_text, N'') <> ISNULL(s.value_text, N'')
               OR ISNULL(t.unit, N'') <> ISNULL(s.derived_unit, N''))
    THEN UPDATE SET t.unit = s.derived_unit, t.value_text = s.value_text, t.value_num = s.value_num
WHEN NOT MATCHED BY TARGET
    THEN INSERT (year_label, [year], row_num, label, unit, value_text, value_num)
         VALUES (s.year_label, s.[year], s.row_num, s.label, s.derived_unit, s.value_text, s.value_num)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE;
GO
