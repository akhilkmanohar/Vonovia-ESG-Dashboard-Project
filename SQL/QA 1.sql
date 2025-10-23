


Select * from [stg].[py_inbox]
select * from [core].[sheet_catalog]
select * from [stg].[raw_fb23_all]
select * from [stg].[raw_fb24_all]
select * from [stg].[raw_sr22_all]

select DIstinct sheet_name from [stg].[raw_fb23_all]


-- 2024 unmatched sheets in raw vs catalog
WITH raw_sheets AS (
  SELECT DISTINCT '2024' AS year_label, sheet_name FROM stg.raw_fb24_all
)
SELECT r.year_label, r.sheet_name
FROM raw_sheets r
LEFT JOIN core.sheet_catalog c
  ON c.year_label = r.year_label AND c.sheet_name = r.sheet_name
WHERE c.sheet_id IS NULL
ORDER BY r.sheet_name;

-- 2023 unmatched sheets in raw vs catalog
WITH raw_sheets AS (
  SELECT DISTINCT '2023' AS year_label, sheet_name FROM stg.raw_fb23_all
)
SELECT r.year_label, r.sheet_name
FROM raw_sheets r
LEFT JOIN core.sheet_catalog c
  ON c.year_label = r.year_label AND c.sheet_name = r.sheet_name
WHERE c.sheet_id IS NULL
ORDER BY r.sheet_name;

-- 2022 unmatched sheets in raw vs catalog
WITH raw_sheets AS (
  SELECT DISTINCT '2022' AS year_label, sheet_name FROM stg.raw_sr22_all
)
SELECT r.year_label, r.sheet_name
FROM raw_sheets r
LEFT JOIN core.sheet_catalog c
  ON c.year_label = r.year_label AND c.sheet_name = r.sheet_name
WHERE c.sheet_id IS NULL
ORDER BY r.sheet_name;
