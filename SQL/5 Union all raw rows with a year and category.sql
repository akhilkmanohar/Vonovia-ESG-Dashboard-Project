

-- Union all raw rows with a year tag

CREATE OR ALTER VIEW stg.v_raw_all_union AS
SELECT '2024' AS year_label, sheet_name, row_num, source_file,
       c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,
       c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28
FROM stg.raw_fb24_all
UNION ALL
SELECT '2023', sheet_name, row_num, source_file,
       c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,
       c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28
FROM stg.raw_fb23_all
UNION ALL
SELECT '2022', sheet_name, row_num, source_file,
       c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,
       c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28
FROM stg.raw_sr22_all;
GO

-- Attach category (E/S/G/General)

CREATE OR ALTER VIEW stg.v_raw_all_with_cat AS
SELECT u.*, c.category
FROM stg.v_raw_all_union u
LEFT JOIN core.sheet_catalog c
  ON c.year_label = u.year_label AND c.sheet_name = u.sheet_name;
GO
