/* =========================================================================================
   Module 16 – Environment: Waste
   ROW_KEY + STRICT COLUMN-BASED YEAR MAP + UNIT-AWARE PARSING (values normalized to t)
   Source: stg.v_raw_all_with_cat
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Waste matrix (inline filter + row_key)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_matrix','V') IS NOT NULL
    DROP VIEW core.v_waste_matrix;
GO
CREATE VIEW core.v_waste_matrix
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
  AND s.sheet_name NOT LIKE '%Energy%'
  AND s.sheet_name NOT LIKE '%Water%'
  AND (
        s.sheet_name LIKE '%Waste%' OR s.sheet_name LIKE N'%Abfall%' OR
        s.sheet_name LIKE '%Recycl%' OR s.sheet_name LIKE N'%Entsorg%' OR
        s.sheet_name LIKE '%Disposal%' OR s.sheet_name LIKE '%Garbage%'
      );
GO

---------------------------------------------------------------------------------------------
-- 2) STRICT column-based year map (exact 4-digit token per column; 1990..current+1)
--    Implemented via CROSS APPLY to keep expressions simple.
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_year_col_map','V') IS NOT NULL
    DROP VIEW core.v_waste_year_col_map;
GO
CREATE VIEW core.v_waste_year_col_map
AS
WITH scan AS (
    SELECT
        m.sheet_name,
        v.col_name AS value_col,
        UPPER(LTRIM(RTRIM(REPLACE(CAST(v.cell AS nvarchar(4000)), CHAR(160), '')))) AS cell_trimmed
    FROM core.v_waste_matrix m
    CROSS APPLY (VALUES
        ('c01', m.c01), ('c02', m.c02), ('c03', m.c03), ('c04', m.c04),
        ('c05', m.c05), ('c06', m.c06), ('c07', m.c07), ('c08', m.c08),
        ('c09', m.c09), ('c10', m.c10), ('c11', m.c11), ('c12', m.c12),
        ('c13', m.c13), ('c14', m.c14), ('c15', m.c15), ('c16', m.c16),
        ('c17', m.c17), ('c18', m.c18), ('c19', m.c19), ('c20', m.c20),
        ('c21', m.c21), ('c22', m.c22), ('c23', m.c23), ('c24', m.c24),
        ('c25', m.c25), ('c26', m.c26), ('c27', m.c27), ('c28', m.c28)
    ) AS v(col_name, cell)
),
tok AS (
    SELECT
        s.sheet_name,
        s.value_col,
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
-- 3) Seed a default unit in core.sheet_units (t) – schema: (sheet_name, year_label, unit)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sheet_units') IS NOT NULL
BEGIN
    MERGE core.sheet_units AS tgt
    USING (SELECT CAST(N'Waste' AS nvarchar(255)) AS sheet_name,
                  CAST(N'' AS nvarchar(50))        AS year_label,
                  CAST(N't' AS nvarchar(50))       AS unit) AS src
       ON  tgt.sheet_name = src.sheet_name
       AND ISNULL(tgt.year_label,'') = ISNULL(src.year_label,'')
    WHEN MATCHED AND ISNULL(tgt.unit,'') <> src.unit
        THEN UPDATE SET unit = src.unit
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (sheet_name, year_label, unit)
             VALUES (src.sheet_name, src.year_label, src.unit);
END
;

---------------------------------------------------------------------------------------------
-- 4) Yearly parsed view (unit-aware → normalize to t) via step-wise CROSS APPLY
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_waste_yearly','V') IS NOT NULL
    DROP VIEW core.v_waste_yearly;
GO
CREATE VIEW core.v_waste_yearly
AS
WITH vals AS (
    SELECT
        m.sheet_name,
        m.category,
        CAST(NULL AS nvarchar(255)) AS subcategory,
        COALESCE(NULLIF(m.c03,''), NULLIF(m.c02,''), NULLIF(m.c01,'')) AS row_label,
        v.col_name AS value_col,
        CAST(v.cell AS nvarchar(4000)) AS raw_value_text
    FROM core.v_waste_matrix m
    CROSS APPLY (VALUES
        ('c01', m.c01), ('c02', m.c02), ('c03', m.c03), ('c04', m.c04),
        ('c05', m.c05), ('c06', m.c06), ('c07', m.c07), ('c08', m.c08),
        ('c09', m.c09), ('c10', m.c10), ('c11', m.c11), ('c12', m.c12),
        ('c13', m.c13), ('c14', m.c14), ('c15', m.c15), ('c16', m.c16),
        ('c17', m.c17), ('c18', m.c18), ('c19', m.c19), ('c20', m.c20),
        ('c21', m.c21), ('c22', m.c22), ('c23', m.c23), ('c24', m.c24),
        ('c25', m.c25), ('c26', m.c26), ('c27', m.c27), ('c28', m.c28)
    ) AS v(col_name, cell)
),
join_year AS (
    SELECT v.*, y.year
    FROM vals v
    INNER JOIN core.v_waste_year_col_map y
      ON y.sheet_name = v.sheet_name
     AND y.value_col  = v.value_col
),
tok AS (
    SELECT
        j.*,
        UPPER(REPLACE(REPLACE(COALESCE(j.raw_value_text,''), CHAR(160), ''), ' ', '')) AS up_no_space
    FROM join_year j
),
unit_and_mag AS (
    SELECT
        t.*,
        CASE
            WHEN up_no_space LIKE '%MT%'  THEN 'Mt'   -- mega tonne (1,000,000 t)
            WHEN up_no_space LIKE '%KT%'  THEN 'kt'   -- kilo tonne (1,000 t)
            WHEN up_no_space LIKE '%MG%'  THEN 'Mg'   -- megagram = 1 t
            WHEN up_no_space LIKE '%T%'   THEN 't'    -- tonne (watch after more specific matches)
            WHEN up_no_space LIKE '%KG%'  THEN 'kg'   -- kilogram
            ELSE NULL
        END AS unit_in_cell,
        CASE
            WHEN up_no_space LIKE '%MIO%' OR up_no_space LIKE '%MN%' OR up_no_space LIKE '%MILLION%' OR up_no_space LIKE N'%MILLIONEN%' THEN 'MILLION'
            WHEN up_no_space LIKE '%MRD%' OR up_no_space LIKE '%BN%' OR up_no_space LIKE '%BILLION%' OR up_no_space LIKE N'%MILLIARDEN%' THEN 'BILLION'
            WHEN up_no_space LIKE '%TSD%' OR up_no_space LIKE N'%TAUSEND%' THEN 'THOUSAND'
            ELSE NULL
        END AS magnitude_word
    FROM tok t
),
-- strip unit tokens progressively
ru0 AS (SELECT u.*, REPLACE(u.raw_value_text, 'Mt','') AS s0 FROM unit_and_mag u),
ru1 AS (SELECT r.*, REPLACE(r.s0, 'kt','') AS s1 FROM ru0 r),
ru2 AS (SELECT r.*, REPLACE(r.s1, 'Mg','') AS s2 FROM ru1 r),
ru3 AS (SELECT r.*, REPLACE(r.s2, 'kg','') AS s3 FROM ru2 r),
ru4 AS (SELECT r.*, REPLACE(r.s3, 't','')  AS s4 FROM ru3 r),
neg AS (
    SELECT r.*,
           CASE WHEN r.s4 LIKE '(%' OR r.s4 LIKE '%)' THEN '-' + REPLACE(REPLACE(r.s4,'(',''),')','') ELSE r.s4 END AS s5
    FROM ru4 r
),
c0 AS (SELECT n.*, REPLACE(n.s5, CHAR(160), '') AS s6 FROM neg n),
c1 AS (SELECT c.*, REPLACE(c.s6, '−','-') AS s7 FROM c0 c),
c2 AS (SELECT c.*, REPLACE(c.s7, '–','-') AS s8 FROM c1 c),
c3 AS (SELECT c.*, REPLACE(c.s8,'—','-') AS s9 FROM c2 c),
c4 AS (SELECT c.*, REPLACE(c.s9,'%','') AS s10 FROM c3 c),
c5 AS (SELECT c.*, REPLACE(c.s10,'*','') AS s11 FROM c4 c),
c6 AS (SELECT c.*, REPLACE(c.s11,'†','') AS s12 FROM c5 c),
c7 AS (SELECT c.*, REPLACE(c.s12,'·','') AS s13 FROM c6 c),
c8 AS (SELECT c.*, REPLACE(c.s13,'/a','') AS s14 FROM c7 c),
c9 AS (SELECT c.*, REPLACE(c.s14,'/yr','') AS s15 FROM c8 c),
clean AS (SELECT c.*, LTRIM(RTRIM(c.s15)) AS txt_clean FROM c9 c),
parse_try AS (
    SELECT
        cl.*,
        TRY_CONVERT(decimal(38,6), REPLACE(REPLACE(cl.txt_clean, '.', ''), ',', '.')) AS try1,
        TRY_CONVERT(decimal(38,6), REPLACE(cl.txt_clean, ',', '')) AS try2,
        TRY_PARSE(cl.txt_clean AS decimal(38,6) USING 'de-DE') AS try3,
        TRY_PARSE(cl.txt_clean AS decimal(38,6) USING 'en-US') AS try4
    FROM clean cl
),
parsed AS (
    SELECT
        p.sheet_name, p.category, p.subcategory, p.year, p.row_label,
        p.unit_in_cell, p.magnitude_word,
        COALESCE(p.try1, p.try2, p.try3, p.try4) AS value_raw
    FROM parse_try p
),
scale AS (
    SELECT
        parsed.*,
        -- unit → t
        CASE unit_in_cell
            WHEN 'Mt' THEN 1000000.0
            WHEN 'kt' THEN 1000.0
            WHEN 'Mg' THEN 1.0
            WHEN 'kg' THEN 0.001
            WHEN 't'  THEN 1.0
            ELSE 1.0
        END *
        CASE magnitude_word
            WHEN 'BILLION'  THEN 1000000000.0
            WHEN 'MILLION'  THEN 1000000.0
            WHEN 'THOUSAND' THEN 1000.0
            ELSE 1.0
        END AS scale_to_t
    FROM parsed
),
to_t AS (
    SELECT
        sheet_name, category, subcategory, year, row_label,
        CASE WHEN value_raw IS NOT NULL THEN value_raw * scale_to_t END AS value_t
    FROM scale
),
unit_resolved AS (
    SELECT
        t.*,
        COALESCE(u_year.unit, u_sheet.unit, N't') AS derived_unit
    FROM to_t t
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
    value_t AS value,
    derived_unit
FROM unit_resolved
WHERE value_t IS NOT NULL
  AND year BETWEEN 1990 AND (YEAR(GETUTCDATE()) + 1);
GO

---------------------------------------------------------------------------------------------
-- 5) Optional materialized table + MERGE
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.waste_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.waste_yearly
    (
        waste_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
        sheet_name   nvarchar(255) NOT NULL,
        category     nvarchar(255) NULL,
        subcategory  nvarchar(255) NULL,
        year         int NOT NULL,
        row_label    nvarchar(1000) NULL,
        value        decimal(38,6) NULL,
        derived_unit nvarchar(50) NOT NULL,
        load_dts     datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_waste_yearly_year  ON core.waste_yearly(year);
    CREATE INDEX IX_waste_yearly_sheet ON core.waste_yearly(sheet_name, year);
END
;

DECLARE @merge_output TABLE(action nvarchar(10));

;WITH src AS (
    SELECT
        sheet_name,
        category,
        subcategory,
        year,
        row_label,
        SUM(value) AS value,
        derived_unit
    FROM core.v_waste_yearly
    GROUP BY sheet_name, category, subcategory, year, row_label, derived_unit
)
MERGE core.waste_yearly AS tgt
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
OUTPUT $action INTO @merge_output(action);

SELECT
    SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END) AS merge_inserted,
    SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END) AS merge_updated,
    SUM(CASE WHEN action = 'DELETE' THEN 1 ELSE 0 END) AS merge_deleted
FROM @merge_output;

-- Peek
SELECT TOP (25)
    sheet_name, year, row_label, value, derived_unit
FROM core.waste_yearly
ORDER BY sheet_name, year, row_label;
