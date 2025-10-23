USE [Vonovia_ESG_DB];
GO

-- Matrix view keeps the Excel layout for readability
CREATE OR ALTER VIEW core.v_ghg_balance_matrix
AS
SELECT TOP (100) PERCENT
    year_label,
    row_num,
    label = NULLIF(LTRIM(RTRIM(c01)), ''),
    c02,
    c03,
    c04,
    c05,
    c06,
    c07,
    c08,
    c09,
    c10,
    c11,
    c12,
    c13,
    c14,
    c15,
    c16,
    c17,
    c18
FROM stg.v_raw_all_with_cat
WHERE sheet_name = N'Greenhouse Gas Balance'
ORDER BY year_label, row_num;
GO

-- Generic long view unpivots to col_key/value_text; real year mapping comes later
CREATE OR ALTER VIEW core.v_ghg_balance_long_generic
AS
SELECT
    m.year_label,
    m.row_num,
    m.label,
    ca.col_key,
    ca.value_text
FROM core.v_ghg_balance_matrix AS m
CROSS APPLY (VALUES
    (N'c02', m.c02),
    (N'c03', m.c03),
    (N'c04', m.c04),
    (N'c05', m.c05),
    (N'c06', m.c06),
    (N'c07', m.c07),
    (N'c08', m.c08),
    (N'c09', m.c09),
    (N'c10', m.c10),
    (N'c11', m.c11),
    (N'c12', m.c12),
    (N'c13', m.c13),
    (N'c14', m.c14),
    (N'c15', m.c15),
    (N'c16', m.c16),
    (N'c17', m.c17),
    (N'c18', m.c18)
) AS ca(col_key, value_text);
GO

-- A) Peek the wide/matrix view
SELECT TOP (20) *
FROM core.v_ghg_balance_matrix
ORDER BY year_label, row_num;

-- B) Peek the long/unpivoted view
SELECT TOP (50) *
FROM core.v_ghg_balance_long_generic
ORDER BY year_label, row_num, col_key;

-- C) Count rows by year_label and col_key (sanity: expected density)
SELECT
    year_label,
    col_key,
    COUNT(*) AS rows_per_col
FROM core.v_ghg_balance_long_generic
GROUP BY year_label, col_key
ORDER BY year_label DESC, col_key;
GO
