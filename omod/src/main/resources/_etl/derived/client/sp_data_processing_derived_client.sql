-- $BEGIN
-- Rebuild dim_client (rename-to-staging in sp_mamba_drop_all_derived_tables handles the brief gap)
CALL sp_dim_client();

-- Build fact_client staging while old fact_client stays live
DROP TABLE IF EXISTS mamba_fact_client_staging;
CALL sp_fact_client();

-- Guard against a pre-existing _old table left by a previously crashed ETL run
DROP TABLE IF EXISTS mamba_fact_client_old;

-- Atomic swap: old data stays live until this single RENAME completes
IF (SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mamba_fact_client') > 0 THEN
    RENAME TABLE mamba_fact_client         TO mamba_fact_client_old,
                 mamba_fact_client_staging TO mamba_fact_client;
    DROP TABLE IF EXISTS mamba_fact_client_old;
ELSE
    RENAME TABLE mamba_fact_client_staging TO mamba_fact_client;
END IF;
-- $END