-- $BEGIN


SET session group_concat_max_len = 20000;

SELECT GROUP_CONCAT(DISTINCT
                    CONCAT(
                            'MAX(CASE WHEN coarse_age_group = ''',
                            coarse_age_group,
                            ''' THEN count ELSE 0 END) AS `',
                            coarse_age_group, '`'
                    )
       )
INTO @coarse_age_group_cols
FROM mamba_dim_client_care_and_treatment;

SELECT GROUP_CONCAT(DISTINCT
                    CONCAT(
                            'MAX(CASE WHEN fine_age_group = ''',
                            fine_age_group,
                            ''' THEN count ELSE 0 END) AS `',
                            fine_age_group, '`'
                    )
       )
INTO @fine_age_group_cols
FROM mamba_dim_client_care_and_treatment;

SET @sql = CONCAT('
        SELECT
          sex,
          ', @coarse_age_group_cols, ',
          ', @fine_age_group_cols, '
        FROM (
          SELECT
            sex,
            coarse_age_group,
            fine_age_group,
            COUNT(*) AS count
          FROM mamba_dim_client_care_and_treatment
          GROUP BY sex, coarse_age_group, fine_age_group
        ) AS subquery
        GROUP BY sex
        ');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- $END
