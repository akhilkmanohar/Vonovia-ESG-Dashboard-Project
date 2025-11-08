SET NOCOUNT ON;
SELECT [schema], view_name, category, notes
FROM rpt.v_gov_import_catalog
ORDER BY [schema], view_name;
