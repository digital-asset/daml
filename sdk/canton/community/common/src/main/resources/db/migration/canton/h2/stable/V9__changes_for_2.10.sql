
-- adding two flags in reserve for future use (orchestrating package removal)
ALTER TABLE daml_packages ADD COLUMN flag INT NOT NULL DEFAULT 0;
ALTER TABLE dars ADD COLUMN flag INT NOT NULL DEFAULT 0;
