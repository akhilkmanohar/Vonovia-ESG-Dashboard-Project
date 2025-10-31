/* =========================================================================================
   Module 14c – Environment: Energy Consumption
   Refinement: strict column-based year mapping + future-year cap
   - Accept as year only cells that are exactly a 4-digit token (after trimming spaces/NBSP)
   - Allowed year range: 1990 .. (YEAR(GETUTCDATE()) + 1)
   - Rebuild yearly view to respect the refined map and re-MERGE facts
   Dependencies:
     - core.v_energy_matrix               (from Module 14)
     - core.v_energy_yearly (to be recreated here, depends on year column map)
     - core.sheet_units(sheet_name, year_label, unit)
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Refined column-based year map
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_year_col_map','V') IS NOT NULL
    DROP VIEW core.v_energy_year_col_map;
GO
CREATE VIEW core.v_energy_year_col_map
AS
WITH colnames AS (
    SELECT 'c01' AS col_name UNION ALL SELECT 'c02' UNION ALL SELECT 'c03' UNION ALL SELECT 'c04'
    UNION ALL SELECT 'c05' UNION ALL SELECT 'c06' UNION ALL SELECT 'c07' UNION ALL SELECT 'c08'
    UNION ALL SELECT 'c09' UNION ALL SELECT 'c10' UNION ALL SELECT 'c11' UNION ALL SELECT 'c12'
    UNION ALL SELECT 'c13' UNION ALL SELECT 'c14' UNION ALL SELECT 'c15' UNION ALL SELECT 'c16'
    UNION ALL SELECT 'c17' UNION ALL SELECT 'c18' UNION ALL SELECT 'c19' UNION ALL SELECT 'c20'
    UNION ALL SELECT 'c21' UNION ALL SELECT 'c22' UNION ALL SELECT 'c23' UNION ALL SELECT 'c24'
    UNION ALL SELECT 'c25' UNION ALL SELECT 'c26' UNION ALL SELECT 'c27' UNION ALL SELECT 'c28'
),
scan AS (
    SELECT
        m.sheet_name,
        c.col_name AS value_col,
        UPPER(LTRIM(RTRIM(REPLACE(CAST(
            CASE c.col_name
                WHEN 'c01' THEN m.c01 WHEN 'c02' THEN m.c02 WHEN 'c03' THEN m.c03 WHEN 'c04' THEN m.c04
                WHEN 'c05' THEN m.c05 WHEN 'c06' THEN m.c06 WHEN 'c07' THEN m.c07 WHEN 'c08' THEN m.c08
                WHEN 'c09' THEN m.c09 WHEN 'c10' THEN m.c10 WHEN 'c11' THEN m.c11 WHEN 'c12' THEN m.c12
                WHEN 'c13' THEN m.c13 WHEN 'c14' THEN m.c14 WHEN 'c15' THEN m.c15 WHEN 'c16' THEN m.c16
                WHEN 'c17' THEN m.c17 WHEN 'c18' THEN m.c18 WHEN 'c19' THEN m.c19 WHEN 'c20' THEN m.c20
                WHEN 'c21' THEN m.c21 WHEN 'c22' THEN m.c22 WHEN 'c23' THEN m.c23 WHEN 'c24' THEN m.c24
                WHEN 'c25' THEN m.c25 WHEN 'c26' THEN m.c26 WHEN 'c27' THEN m.c27 WHEN 'c28' THEN m.c28
            END AS nvarchar(4000)
        ), CHAR(160), '')))) AS cell_trimmed
    FROM core.v_energy_matrix m
    CROSS JOIN colnames c
),
tok AS (
    SELECT
        s.sheet_name,
        s.value_col,
        s.cell_trimmed,
        TRY_CONVERT(int, s.cell_trimmed) AS year_token
    FROM scan s
    WHERE LEN(s.cell_trimmed) = 4
      AND s.cell_trimmed NOT LIKE '%[^0-9]%'
),
rng AS (
    SELECT
        t.sheet_name,
        t.value_col,
        t.year_token AS year
    FROM tok t
    WHERE t.year_token BETWEEN 1990 AND (YEAR(GETUTCDATE()) + 1)
)
SELECT
    sheet_name,
    value_col,
    MAX(year) AS year
FROM rng
GROUP BY sheet_name, value_col;
GO

---------------------------------------------------------------------------------------------
-- 2) Recreate yearly parsed view to consume refined year map
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_yearly','V') IS NOT NULL
    DROP VIEW core.v_energy_yearly;
GO
CREATE VIEW core.v_energy_yearly
AS
WITH unpvt AS (
    SELECT
        m.sheet_name,
        m.category,
        CAST(NULL AS nvarchar(255)) AS subcategory,
        COALESCE(NULLIF(m.c03,''), NULLIF(m.c02,''), NULLIF(m.c01,'')) AS row_label,
        y.value_col,
        y.year,
        CAST(
            CASE y.value_col
                WHEN 'c01' THEN m.c01 WHEN 'c02' THEN m.c02 WHEN 'c03' THEN m.c03 WHEN 'c04' THEN m.c04
                WHEN 'c05' THEN m.c05 WHEN 'c06' THEN m.c06 WHEN 'c07' THEN m.c07 WHEN 'c08' THEN m.c08
                WHEN 'c09' THEN m.c09 WHEN 'c10' THEN m.c10 WHEN 'c11' THEN m.c11 WHEN 'c12' THEN m.c12
                WHEN 'c13' THEN m.c13 WHEN 'c14' THEN m.c14 WHEN 'c15' THEN m.c15 WHEN 'c16' THEN m.c16
                WHEN 'c17' THEN m.c17 WHEN 'c18' THEN m.c18 WHEN 'c19' THEN m.c19 WHEN 'c20' THEN m.c20
                WHEN 'c21' THEN m.c21 WHEN 'c22' THEN m.c22 WHEN 'c23' THEN m.c23 WHEN 'c24' THEN m.c24
                WHEN 'c25' THEN m.c25 WHEN 'c26' THEN m.c26 WHEN 'c27' THEN m.c27 WHEN 'c28' THEN m.c28
            END AS nvarchar(4000)
        ) AS raw_value_text
    FROM core.v_energy_matrix m
    INNER JOIN core.v_energy_year_col_map y
      ON y.sheet_name = m.sheet_name
),
tok AS (
    SELECT
        u.*,
        UPPER(REPLACE(REPLACE(COALESCE(u.raw_value_text,''), CHAR(160), ''), ' ', '')) AS up_no_space
    FROM unpvt u
),
unit_tag AS (
    SELECT
        t.*,
        CASE
            WHEN up_no_space LIKE '%GWH%' THEN 'GWh'
            WHEN up_no_space LIKE '%MWH%' THEN 'MWh'
            WHEN up_no_space LIKE '%KWH%' THEN 'kWh'
            WHEN up_no_space LIKE '%TJ%'  THEN 'TJ'
            WHEN up_no_space LIKE '%GJ%'  THEN 'GJ'
            ELSE NULL
        END AS unit_in_cell
    FROM tok t
),
rm_units AS (
    SELECT
        u2.*,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(u2.raw_value_text,'GWh',''),'MWh',''),'kWh',''),'TJ',''),'GJ','') AS s0
    FROM unit_tag u2
),
neg_fix AS (
    SELECT
        r.*,
        CASE
            WHEN r.s0 LIKE '(%' OR r.s0 LIKE '%)' THEN '-' + REPLACE(REPLACE(r.s0,'(',''),')','')
            ELSE r.s0
        END AS s1
    FROM rm_units r
),
c0 AS (SELECT n.*, REPLACE(n.s1, CHAR(160), '') AS s2 FROM neg_fix n),
c1 AS (SELECT c0.*, REPLACE(c0.s2, '−','-') AS s3 FROM c0),
c2 AS (SELECT c1.*, REPLACE(c1.s3, '–','-') AS s4 FROM c1),
c3 AS (SELECT c2.*, REPLACE(c2.s4, '—','-') AS s5 FROM c2),
c4 AS (SELECT c3.*, REPLACE(c3.s5, '%','') AS s6 FROM c3),
c5 AS (SELECT c4.*, REPLACE(c4.s6, '*','') AS s7 FROM c4),
c6 AS (SELECT c5.*, REPLACE(c5.s7, '†','') AS s8 FROM c5),
c7 AS (SELECT c6.*, REPLACE(c6.s8, '·','') AS s9 FROM c6),
c8 AS (SELECT c7.*, REPLACE(c7.s9, '/a','') AS s10 FROM c7),
c9 AS (SELECT c8.*, REPLACE(c8.s10, '/yr','') AS s11 FROM c8),
cleaned AS (SELECT c9.*, LTRIM(RTRIM(c9.s11)) AS txt_clean FROM c9),
parsed_raw AS (
    SELECT
        cl.*,
        TRY_CONVERT(decimal(38,6), REPLACE(REPLACE(cl.txt_clean, '.', ''), ',', '.')) AS try1,
        TRY_CONVERT(decimal(38,6), REPLACE(cl.txt_clean, ',', '')) AS try2,
        TRY_PARSE(cl.txt_clean AS decimal(38,6) USING 'de-DE') AS try3,
        TRY_PARSE(cl.txt_clean AS decimal(38,6) USING 'en-US') AS try4
    FROM cleaned cl
),
parsed AS (
    SELECT
        p.sheet_name, p.category, p.subcategory, p.year, p.row_label, p.unit_in_cell,
        COALESCE(p.try1, p.try2, p.try3, p.try4) AS value_raw
    FROM parsed_raw p
),
scaled AS (
    SELECT
        parsed.*,
        CASE unit_in_cell
            WHEN 'GWh' THEN 1000.0
            WHEN 'kWh' THEN 0.001
            WHEN 'TJ'  THEN 277.7777778
            WHEN 'GJ'  THEN 0.2777777778
            ELSE 1.0
        END AS scale_to_mwh
    FROM parsed
),
to_mwh AS (
    SELECT
        sheet_name, category, subcategory, year, row_label,
        CASE WHEN value_raw IS NOT NULL THEN value_raw * scale_to_mwh END AS value_mwh
    FROM scaled
),
aggregated AS (
    SELECT
        sheet_name,
        category,
        subcategory,
        year,
        row_label,
        SUM(value_mwh) AS value_mwh
    FROM to_mwh
    GROUP BY sheet_name, category, subcategory, year, row_label
),
unit_resolved AS (
    SELECT
        t.*,
        COALESCE(u_year.unit, u_sheet.unit, N'MWh') AS derived_unit
    FROM aggregated t
    OUTER APPLY (
        SELECT TOP (1) u.unit
        FROM core.sheet_units u
        WHERE u.sheet_name = t.sheet_name
          AND TRY_CONVERT(int, NULLIF(u.year_label,'')) = t.year
    ) AS u_year
    OUTER APPLY (
        SELECT TOP (1) u.unit
        FROM core.sheet_units u
        WHERE u.sheet_name = t.sheet_name
          AND (u.year_label IS NULL OR u.year_label = '')
    ) AS u_sheet
)
SELECT
    sheet_name,
    category,
    subcategory,
    year,
    row_label,
    value_mwh AS value,
    derived_unit
FROM unit_resolved
WHERE value_mwh IS NOT NULL
  AND year BETWEEN 1990 AND (YEAR(GETUTCDATE()) + 1);
GO

---------------------------------------------------------------------------------------------
-- 3) Re-MERGE into materialized table
---------------------------------------------------------------------------------------------
;WITH src AS (SELECT * FROM core.v_energy_yearly)
MERGE core.energy_yearly AS tgt
USING src
   ON tgt.sheet_name = src.sheet_name
  AND tgt.year       = src.year
  AND ISNULL(tgt.row_label,'') = ISNULL(src.row_label,'')
WHEN MATCHED AND (
       ISNULL(tgt.value, 0)       <> ISNULL(src.value, 0)
    OR ISNULL(tgt.derived_unit,'') <> ISNULL(src.derived_unit,'')
    OR ISNULL(tgt.category,'')    <> ISNULL(src.category,'')
    OR ISNULL(tgt.subcategory,'') <> ISNULL(src.subcategory,'')
)
THEN UPDATE SET
    tgt.value        = src.value,
    tgt.derived_unit = src.derived_unit,
    tgt.category     = src.category,
    tgt.subcategory  = src.subcategory,
    tgt.load_dts     = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET
THEN INSERT (sheet_name, category, subcategory, year, row_label, value, derived_unit)
     VALUES (src.sheet_name, src.category, src.subcategory, src.year, src.row_label, src.value, src.derived_unit)
WHEN NOT MATCHED BY SOURCE
THEN DELETE
;

-- Peek
SELECT TOP (15) year, sheet_name, row_label, value
FROM core.energy_yearly
ORDER BY year DESC, sheet_name, row_label;
