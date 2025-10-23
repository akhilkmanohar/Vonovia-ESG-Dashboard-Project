




-- 2024
WITH raw_sheets AS (SELECT DISTINCT '2024' AS year_label, sheet_name FROM stg.raw_fb24_all)
SELECT r.year_label, r.sheet_name
FROM raw_sheets r
LEFT JOIN core.sheet_catalog c
  ON c.year_label=r.year_label AND c.sheet_name=r.sheet_name
WHERE c.sheet_id IS NULL;

-- repeat for 2023 (stg.raw_fb23_all) and 2022 (stg.raw_sr22_all)
