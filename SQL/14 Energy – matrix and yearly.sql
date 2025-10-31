/* =========================================================================================
   Module 14 – Environment: Energy Consumption
   ROW_KEY + COLUMN-BASED YEAR MAP + UNIT-AWARE PARSING (values normalized to MWh)
   Uses core.sheet_units(sheet_name, year_label, unit) for derived_unit preference.
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Energy matrix (inline filter + row_key)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_energy_matrix','V') IS NOT NULL
    DROP VIEW core.v_energy_matrix;
GO
CREATE VIEW core.v_energy_matrix
AS
SELECT
    CONVERT(varbinary(32),
        HASHBYTES('SHA2_256',
            CONCAT_WS('|',
                s.sheet_name,
                s.c01, s.c02, s.c03, s.c04,
                s.c05, s.c06, s.c07, s.c08,
                s.c09, s.c10, s.c11, s.c12,
                s.c13, s.c14, s.c15, s.c16,
                s.c17, s.c18, s.c19, s.c20,
                s.c21, s.c22, s.c23, s.c24,
                s.c25, s.c26, s.c27, s.c28
            )
        )
    ) AS row_key,
    s.sheet_name,
    s.category,
    CAST(NULL AS nvarchar(255)) AS subcategory,
    s.c01, s.c02, s.c03, s.c04,
    s.c05, s.c06, s.c07, s.c08,
    s.c09, s.c10, s.c11, s.c12,
    s.c13, s.c14, s.c15, s.c16,
    s.c17, s.c18, s.c19, s.c20,
    s.c21, s.c22, s.c23, s.c24,
    s.c25, s.c26, s.c27, s.c28
FROM stg.v_raw_all_with_cat s
WHERE (s.category LIKE '%Environment%' OR s.category LIKE '%Environmental%')
  AND s.sheet_name NOT LIKE '%Greenhouse Gas Balance%'
  AND s.sheet_name NOT LIKE '%GHG%'
  AND (
        s.sheet_name LIKE '%Energy%' OR
        s.sheet_name LIKE '%Energie%' OR
        s.sheet_name LIKE '%Consumption%' OR
        s.sheet_name LIKE '%Verbrauch%'
      );
GO

---------------------------------------------------------------------------------------------
-- 2) COLUMN-BASED YEAR MAP
--    Scan every column (c01..c28); if a 4-digit year appears anywhere in that column, map it.
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
        c.col_name,
        CAST(
            CASE c.col_name
                WHEN 'c01' THEN m.c01 WHEN 'c02' THEN m.c02 WHEN 'c03' THEN m.c03 WHEN 'c04' THEN m.c04
                WHEN 'c05' THEN m.c05 WHEN 'c06' THEN m.c06 WHEN 'c07' THEN m.c07 WHEN 'c08' THEN m.c08
                WHEN 'c09' THEN m.c09 WHEN 'c10' THEN m.c10 WHEN 'c11' THEN m.c11 WHEN 'c12' THEN m.c12
                WHEN 'c13' THEN m.c13 WHEN 'c14' THEN m.c14 WHEN 'c15' THEN m.c15 WHEN 'c16' THEN m.c16
                WHEN 'c17' THEN m.c17 WHEN 'c18' THEN m.c18 WHEN 'c19' THEN m.c19 WHEN 'c20' THEN m.c20
                WHEN 'c21' THEN m.c21 WHEN 'c22' THEN m.c22 WHEN 'c23' THEN m.c23 WHEN 'c24' THEN m.c24
                WHEN 'c25' THEN m.c25 WHEN 'c26' THEN m.c26 WHEN 'c27' THEN m.c27 WHEN 'c28' THEN m.c28
            END AS nvarchar(4000)
        ) AS cell_text
    FROM core.v_energy_matrix m
    CROSS JOIN colnames c
),
detect AS (
    SELECT
        sheet_name,
        col_name AS value_col,
        NULLIF(PATINDEX('%[12][0-9][0-9][0-9]%', cell_text), 0) AS yr_pos,
        TRY_CONVERT(int, SUBSTRING(cell_text, NULLIF(PATINDEX('%[12][0-9][0-9][0-9]%', cell_text),0), 4)) AS year
    FROM scan
)
SELECT
    sheet_name,
    value_col,
    MAX(year) AS year
FROM detect
WHERE yr_pos IS NOT NULL
  AND year BETWEEN 1990 AND 2100
GROUP BY sheet_name, value_col;
GO

---------------------------------------------------------------------------------------------
-- 3) Seed a generic unit row in core.sheet_units (MWh) – schema: (sheet_name, year_label, unit)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sheet_units') IS NOT NULL
BEGIN
    MERGE core.sheet_units AS tgt
    USING (SELECT CAST(N'Energy Consumption' AS nvarchar(255)) AS sheet_name,
                  CAST(-1 AS int)                               AS year_label,
                  CAST(N'MWh' AS nvarchar(50))                  AS unit) AS src
       ON  tgt.sheet_name = src.sheet_name
       AND tgt.year_label = src.year_label
    WHEN MATCHED AND ISNULL(tgt.unit,'') <> src.unit
        THEN UPDATE SET unit = src.unit
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (sheet_name, year_label, unit)
             VALUES (src.sheet_name, src.year_label, src.unit);
END
;

---------------------------------------------------------------------------------------------
-- 4) Yearly parsed view: UNPIVOT → clean → parse → scale to MWh → derive unit
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
        v.value_col,
        CAST(
            CASE v.value_col
                WHEN 'c01' THEN m.c01 WHEN 'c02' THEN m.c02 WHEN 'c03' THEN m.c03 WHEN 'c04' THEN m.c04
                WHEN 'c05' THEN m.c05 WHEN 'c06' THEN m.c06 WHEN 'c07' THEN m.c07 WHEN 'c08' THEN m.c08
                WHEN 'c09' THEN m.c09 WHEN 'c10' THEN m.c10 WHEN 'c11' THEN m.c11 WHEN 'c12' THEN m.c12
                WHEN 'c13' THEN m.c13 WHEN 'c14' THEN m.c14 WHEN 'c15' THEN m.c15 WHEN 'c16' THEN m.c16
                WHEN 'c17' THEN m.c17 WHEN 'c18' THEN m.c18 WHEN 'c19' THEN m.c19 WHEN 'c20' THEN m.c20
                WHEN 'c21' THEN m.c21 WHEN 'c22' THEN m.c22 WHEN 'c23' THEN m.c23 WHEN 'c24' THEN m.c24
                WHEN 'c25' THEN m.c25 WHEN 'c26' THEN m.c26 WHEN 'c27' THEN m.c27 WHEN 'c28' THEN m.c28
            END AS nvarchar(4000)
        ) AS raw_value_text,
        v.year
    FROM core.v_energy_matrix m
    INNER JOIN core.v_energy_year_col_map v
      ON v.sheet_name = m.sheet_name
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
        sheet_name,
        category,
        subcategory,
        year,
        row_label,
        unit_in_cell,
        COALESCE(try1, try2, try3, try4) AS value_raw
    FROM parsed_raw
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
unit_resolved AS (
    SELECT
        t.*,
        COALESCE(u_year.unit, u_sheet.unit, N'MWh') AS derived_unit
    FROM to_mwh t
    OUTER APPLY (
        SELECT TOP (1) u.unit
        FROM core.sheet_units u
        WHERE u.sheet_name = t.sheet_name
          AND TRY_CONVERT(int, u.year_label) = t.year
        ORDER BY u.year_label DESC
    ) AS u_year
    OUTER APPLY (
        SELECT TOP (1) u.unit
        FROM core.sheet_units u
        WHERE u.sheet_name = t.sheet_name
          AND TRY_CONVERT(int, u.year_label) = -1
        ORDER BY u.sheet_name
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
WHERE value_mwh IS NOT NULL;
GO

---------------------------------------------------------------------------------------------
-- 5) Materialized table + MERGE
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.energy_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.energy_yearly
    (
        energy_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
        sheet_name   nvarchar(255) NOT NULL,
        category     nvarchar(255) NULL,
        subcategory  nvarchar(255) NULL,
        year         int NOT NULL,
        row_label    nvarchar(1000) NULL,
        value        decimal(38,6) NULL,
        derived_unit nvarchar(50) NOT NULL,
        load_dts     datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_energy_yearly_year  ON core.energy_yearly(year);
    CREATE INDEX IX_energy_yearly_sheet ON core.energy_yearly(sheet_name, year);
END
;

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
SELECT TOP (25)
    sheet_name, year, row_label, value, derived_unit
FROM core.energy_yearly
ORDER BY sheet_name, year, row_label;
