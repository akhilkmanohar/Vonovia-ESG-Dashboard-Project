/* =========================================================================================
   Module 15 – Environment: Water Consumption (Refactored)
   - Deterministic row_key matrix
   - Strict column-based year map (exact 4-digit token; ≤ current year + 1)
   - Unit-aware parsing (L/kL/ML/GL/m³/Hm³/Mm³ → m³) implemented with step-wise CROSS APPLY
   - Seeds core.sheet_units defaults to m³ (schema: sheet_name, year_label, unit)
   ========================================================================================= */

SET NOCOUNT ON;

---------------------------------------------------------------------------------------------
-- 1) Water matrix (inline filter + row_key)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_water_matrix','V') IS NOT NULL
    DROP VIEW core.v_water_matrix;
GO
CREATE VIEW core.v_water_matrix
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
        s.sheet_name LIKE '%Water%' OR s.sheet_name LIKE N'%Wasser%' OR
        s.sheet_name LIKE '%Consumption%' OR s.sheet_name LIKE N'%Verbrauch%' OR
        s.sheet_name LIKE '%Usage%' OR s.sheet_name LIKE '%Use%'
      );
GO

---------------------------------------------------------------------------------------------
-- 2) Strict column-based year map
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_water_year_col_map','V') IS NOT NULL
    DROP VIEW core.v_water_year_col_map;
GO
CREATE VIEW core.v_water_year_col_map
AS
WITH flat AS (
    SELECT
        m.sheet_name,
        v.col_name,
        UPPER(LTRIM(RTRIM(REPLACE(CONVERT(nvarchar(4000), v.cell), CHAR(160), '')))) AS cell_trimmed
    FROM core.v_water_matrix m
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
        f.sheet_name,
        f.col_name,
        TRY_CONVERT(int, f.cell_trimmed) AS year_token
    FROM flat f
    WHERE LEN(f.cell_trimmed) = 4
      AND f.cell_trimmed NOT LIKE '%[^0-9]%'
),
rng AS (
    SELECT
        t.sheet_name,
        t.col_name,
        t.year_token
    FROM tok t
    WHERE t.year_token BETWEEN 1990 AND (YEAR(GETUTCDATE()) + 1)
)
SELECT
    sheet_name,
    col_name AS value_col,
    MAX(year_token) AS year
FROM rng
GROUP BY sheet_name, col_name;
GO

---------------------------------------------------------------------------------------------
-- 3) Seed default unit override
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.sheet_units') IS NOT NULL
BEGIN
    MERGE core.sheet_units AS tgt
    USING (SELECT CAST(N'Water Consumption' AS nvarchar(255)) AS sheet_name,
                  CAST(-1 AS int)                               AS year_label,
                  CAST(N'm³' AS nvarchar(50))                   AS unit) AS src
       ON tgt.sheet_name = src.sheet_name
      AND tgt.year_label = src.year_label
    WHEN MATCHED AND ISNULL(tgt.unit,'') <> src.unit
        THEN UPDATE SET unit = src.unit
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (sheet_name, year_label, unit)
             VALUES (src.sheet_name, src.year_label, src.unit);
END
;

---------------------------------------------------------------------------------------------
-- 4) Yearly parsed view (unit-aware → normalize to m³)
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.v_water_yearly','V') IS NOT NULL
    DROP VIEW core.v_water_yearly;
GO
CREATE VIEW core.v_water_yearly
AS
WITH flat AS (
    SELECT
        m.sheet_name,
        m.category,
        CAST(NULL AS nvarchar(255)) AS subcategory,
        COALESCE(NULLIF(m.c03,''), NULLIF(m.c02,''), NULLIF(m.c01,'')) AS row_label,
        v.col_name,
        CONVERT(nvarchar(4000), v.cell) AS raw_value_text
    FROM core.v_water_matrix m
    CROSS APPLY (VALUES
        ('c01', m.c01), ('c02', m.c02), ('c03', m.c03), ('c04', m.c04),
        ('c05', m.c05), ('c06', m.c06), ('c07', m.c07), ('c08', m.c08),
        ('c09', m.c09), ('c10', m.c10), ('c11', m.c11), ('c12', m.c12),
        ('c13', m.c13), ('c14', m.c14), ('c15', m.c15), ('c16', m.c16),
        ('c17', m.c17), ('c18', m.c18), ('c19', m.c19), ('c20', m.c20),
        ('c21', m.c21), ('c22', m.c22), ('c23', m.c23), ('c24', m.c24),
        ('c25', m.c25), ('c26', m.c26), ('c27', m.c27), ('c28', m.c28)
    ) AS v(col_name, cell)
    INNER JOIN core.v_water_year_col_map y
        ON y.sheet_name = m.sheet_name
       AND y.value_col  = v.col_name
),
tokenized AS (
    SELECT
        f.sheet_name,
        f.category,
        f.subcategory,
        y.year,
        f.row_label,
        f.raw_value_text,
        UPPER(REPLACE(REPLACE(COALESCE(f.raw_value_text,''), CHAR(160), ''), ' ', '')) AS up_no_space
    FROM flat f
    INNER JOIN core.v_water_year_col_map y
        ON y.sheet_name = f.sheet_name
       AND y.value_col  = f.col_name
),
unit_det AS (
    SELECT
        t.*,
        CASE
            WHEN up_no_space LIKE '%HM3%' THEN 'Hm3'
            WHEN up_no_space LIKE '%MM3%' THEN 'Mm3'
            WHEN up_no_space LIKE '%GL%'  THEN 'GL'
            WHEN up_no_space LIKE '%ML%'  THEN 'ML'
            WHEN up_no_space LIKE '%KL%'  THEN 'kL'
            WHEN up_no_space LIKE '%M³%'  OR up_no_space LIKE '%M3%' OR up_no_space LIKE '%M^3%' THEN 'm3'
            WHEN up_no_space LIKE '%L%'   THEN 'L'
            ELSE NULL
        END AS unit_in_cell,
        CASE
            WHEN up_no_space LIKE '%MIO%' OR up_no_space LIKE '%MN%' OR up_no_space LIKE '%MILLION%' OR up_no_space LIKE N'%MILLIONEN%' THEN 'MILLION'
            WHEN up_no_space LIKE '%MRD%' OR up_no_space LIKE '%BN%' OR up_no_space LIKE '%BILLION%' OR up_no_space LIKE N'%MILLIARDEN%' THEN 'BILLION'
            WHEN up_no_space LIKE '%TSD%' OR up_no_space LIKE N'%TAUSEND%' THEN 'THOUSAND'
            ELSE NULL
        END AS magnitude_word
    FROM tokenized t
),
ca_rm0 AS (
    SELECT
        u.*,
        REPLACE(u.raw_value_text, 'Hm3','') AS s0
    FROM unit_det u
),
ca_rm1 AS (SELECT r.*, REPLACE(r.s0, 'Mm3','') AS s1 FROM ca_rm0 r),
ca_rm2 AS (SELECT r.*, REPLACE(REPLACE(r.s1, 'm3',''), N'm³','') AS s2 FROM ca_rm1 r),
ca_rm3 AS (SELECT r.*, REPLACE(r.s2, 'GL','') AS s3 FROM ca_rm2 r),
ca_rm4 AS (SELECT r.*, REPLACE(r.s3, 'ML','') AS s4 FROM ca_rm3 r),
ca_rm5 AS (SELECT r.*, REPLACE(r.s4, 'kL','') AS s5 FROM ca_rm4 r),
ca_rm6 AS (SELECT r.*, REPLACE(r.s5, 'L','')  AS s6 FROM ca_rm5 r),
ca_neg AS (
    SELECT
        r.*,
        CASE WHEN r.s6 LIKE '(%' OR r.s6 LIKE '%)' THEN '-' + REPLACE(REPLACE(r.s6,'(',''),')','') ELSE r.s6 END AS s7
    FROM ca_rm6 r
),
ca_clean0 AS (SELECT r.*, REPLACE(r.s7, CHAR(160), '') AS s8 FROM ca_neg r),
ca_clean1 AS (SELECT r.*, REPLACE(r.s8, '−','-') AS s9 FROM ca_clean0 r),
ca_clean2 AS (SELECT r.*, REPLACE(r.s9, '–','-') AS s10 FROM ca_clean1 r),
ca_clean3 AS (SELECT r.*, REPLACE(r.s10,'—','-') AS s11 FROM ca_clean2 r),
ca_clean4 AS (SELECT r.*, REPLACE(r.s11,'%','') AS s12 FROM ca_clean3 r),
ca_clean5 AS (SELECT r.*, REPLACE(r.s12,'*','') AS s13 FROM ca_clean4 r),
ca_clean6 AS (SELECT r.*, REPLACE(r.s13,'†','') AS s14 FROM ca_clean5 r),
ca_clean7 AS (SELECT r.*, REPLACE(r.s14,'·','') AS s15 FROM ca_clean6 r),
ca_clean8 AS (SELECT r.*, REPLACE(r.s15,'/a','') AS s16 FROM ca_clean7 r),
ca_clean9 AS (SELECT r.*, REPLACE(r.s16,'/yr','') AS s17 FROM ca_clean8 r),
final_clean AS (SELECT r.*, LTRIM(RTRIM(r.s17)) AS txt_clean FROM ca_clean9 r),
parsed_raw AS (
    SELECT
        f.*,
        TRY_CONVERT(decimal(38,6), REPLACE(REPLACE(f.txt_clean, '.', ''), ',', '.')) AS try1,
        TRY_CONVERT(decimal(38,6), REPLACE(f.txt_clean, ',', '')) AS try2,
        TRY_PARSE(f.txt_clean AS decimal(38,6) USING 'de-DE') AS try3,
        TRY_PARSE(f.txt_clean AS decimal(38,6) USING 'en-US') AS try4
    FROM final_clean f
),
parsed AS (
    SELECT
        p.sheet_name, p.category, p.subcategory, p.year, p.row_label,
        p.unit_in_cell, p.magnitude_word,
        COALESCE(p.try1, p.try2, p.try3, p.try4) AS value_raw
    FROM parsed_raw p
),
scaled AS (
    SELECT
        parsed.*,
        CASE unit_in_cell
            WHEN 'Hm3' THEN 1000000.0
            WHEN 'Mm3' THEN 1000000.0
            WHEN 'GL'  THEN 1000000.0
            WHEN 'ML'  THEN 1000.0
            WHEN 'kL'  THEN 1.0
            WHEN 'L'   THEN 0.001
            ELSE 1.0
        END *
        CASE magnitude_word
            WHEN 'BILLION'  THEN 1000000000.0
            WHEN 'MILLION'  THEN 1000000.0
            WHEN 'THOUSAND' THEN 1000.0
            ELSE 1.0
        END AS scale_to_m3
    FROM parsed
),
to_m3 AS (
    SELECT
        sheet_name, category, subcategory, year, row_label,
        CASE WHEN value_raw IS NOT NULL THEN value_raw * scale_to_m3 END AS value_m3
    FROM scaled
),
unit_resolved AS (
    SELECT
        t.*,
        COALESCE(u_year.unit, u_sheet.unit, N'm³') AS derived_unit
    FROM to_m3 t
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
    value_m3 AS value,
    derived_unit
FROM unit_resolved
WHERE value_m3 IS NOT NULL
  AND year BETWEEN 1990 AND (YEAR(GETUTCDATE()) + 1);
GO

---------------------------------------------------------------------------------------------
-- 5) Optional materialized table + MERGE
---------------------------------------------------------------------------------------------
IF OBJECT_ID('core.water_yearly','U') IS NULL
BEGIN
    CREATE TABLE core.water_yearly
    (
        water_yearly_id bigint IDENTITY(1,1) PRIMARY KEY,
        sheet_name   nvarchar(255) NOT NULL,
        category     nvarchar(255) NULL,
        subcategory  nvarchar(255) NULL,
        year         int NOT NULL,
        row_label    nvarchar(1000) NULL,
        value        decimal(38,6) NULL,
        derived_unit nvarchar(50) NOT NULL,
        load_dts     datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_water_yearly_year  ON core.water_yearly(year);
    CREATE INDEX IX_water_yearly_sheet ON core.water_yearly(sheet_name, year);
END
;

;WITH src_raw AS (
    SELECT * FROM core.v_water_yearly
),
src AS (
    SELECT
        sheet_name,
        MIN(category)    AS category,
        MIN(subcategory) AS subcategory,
        year,
        row_label,
        SUM(value)       AS value,
        MAX(derived_unit) AS derived_unit
    FROM src_raw
    GROUP BY sheet_name, year, row_label
)
MERGE core.water_yearly AS tgt
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
THEN DELETE;

-- Peek
SELECT TOP (25)
    sheet_name, year, row_label, value, derived_unit
FROM core.water_yearly
ORDER BY sheet_name, year, row_label;
