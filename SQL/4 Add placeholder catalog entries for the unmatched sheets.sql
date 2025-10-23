

USE Vonovia_ESG_DB;
-- 2024
MERGE core.sheet_catalog AS t
USING (VALUES
  ('2024', N'Composition of the Sustainabili', N'General', N'manual'),
  ('2024', N'Coverage',                         N'General', N'manual'),
  ('2024', N'Portfolio',                        N'General', N'manual')
) AS s(year_label, sheet_name, category, source_file)
ON (t.year_label=s.year_label AND t.sheet_name=s.sheet_name)
WHEN MATCHED THEN UPDATE SET category=s.category, source_file=s.source_file, updated_at=SYSDATETIME()
WHEN NOT MATCHED THEN INSERT(year_label,sheet_name,category,source_file) VALUES(s.year_label,s.sheet_name,s.category,s.source_file);

-- 2023
MERGE core.sheet_catalog AS t
USING (VALUES
  ('2023', N'General Key Figures', N'General', N'manual')
) AS s(year_label, sheet_name, category, source_file)
ON (t.year_label=s.year_label AND t.sheet_name=s.sheet_name)
WHEN MATCHED THEN UPDATE SET category=s.category, source_file=s.source_file, updated_at=SYSDATETIME()
WHEN NOT MATCHED THEN INSERT(year_label,sheet_name,category,source_file) VALUES(s.year_label,s.sheet_name,s.category,s.source_file);

-- 2022
MERGE core.sheet_catalog AS t
USING (VALUES
  ('2022', N'General Key Figures',        N'General', N'manual'),
  ('2022', N'Investment in Maintenance',  N'General', N'manual')
) AS s(year_label, sheet_name, category, source_file)
ON (t.year_label=s.year_label AND t.sheet_name=s.sheet_name)
WHEN MATCHED THEN UPDATE SET category=s.category, source_file=s.source_file, updated_at=SYSDATETIME()
WHEN NOT MATCHED THEN INSERT(year_label,sheet_name,category,source_file) VALUES(s.year_label,s.sheet_name,s.category,s.source_file);




ALTER TABLE core.sheet_catalog
ADD CONSTRAINT CK_sheet_catalog_category
CHECK (category IN (N'Environment', N'Social', N'Governance', N'General'));

select * from [core].[sheet_catalog]