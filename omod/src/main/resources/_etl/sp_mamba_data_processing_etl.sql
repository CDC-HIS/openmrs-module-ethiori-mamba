-- $BEGIN
-- add base folder SP here --

-- Flatten OpenMRS Observational Data
CALL sp_mamba_data_processing_flatten();

-- Add the implementation-specific ETL processes here
CALL sp_data_processing_derived_care_and_treatment();
-- $END