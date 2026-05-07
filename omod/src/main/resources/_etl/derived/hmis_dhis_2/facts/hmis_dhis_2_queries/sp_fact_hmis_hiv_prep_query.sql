DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_hiv_prep_query;

CREATE PROCEDURE sp_fact_hmis_hiv_prep_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH PreExposure AS (select screening.client_id,
                                screening.encounter_id,
                                screening.encounter_datetime,
                                screening.sex_worker,
                                screening.prep_started,
                                screening.type_of_client,
                                screening_1.treatment_start_date,
                                screening_1.do_you_have_an_hiv_positive_partner,
                                screening_1.antiretroviral_art_dispensed_dose_in_days as dose_dispensed,
                                follow_up.follow_up_status,
                                follow_up.pregnancy_status                            as f_pregnancy_status,
                                follow_up_1.prep_dose_end_date,
                                follow_up_1.followup_date_followup_1,
                                screening.screening_date,
                                final_hiv_test_result,
                                currently_breastfeeding_child
                         FROM mamba_flat_encounter_pre_exposure_scree screening
                                  left join mamba_flat_encounter_pre_exposure_scree_1 screening_1
                                            on screening.encounter_id = screening_1.encounter_id
                                  LEFT JOIN mamba_flat_encounter_pre_exposure_follo follow_up
                                            on screening.client_id = follow_up.client_id
                                  LEFT JOIN mamba_flat_encounter_pre_exposure_follo_1 follow_up_1
                                            on follow_up.encounter_id = follow_up_1.encounter_id),
         tmp_latest_follow_up as (select prep_dose_end_date,
                                         prep_started,
                                         date_of_birth,
                                         sex,
                                         do_you_have_an_hiv_positive_partner,
                                         sex_worker,
                                         treatment_start_date,
                                         follow_up_status,
                                         final_hiv_test_result,
                                         type_of_client,
                                         client.client_id,
                                         followup_date_followup_1,
                                         dose_dispensed,
                                         ROW_NUMBER() OVER (
                                             PARTITION BY PreExposure.client_id
                                             ORDER BY
                                                 CASE
                                                     WHEN PreExposure.followup_date_followup_1 IS NULL THEN 1
                                                     ELSE 0
                                                     END,
                                                 PreExposure.followup_date_followup_1 DESC,
                                                 PreExposure.encounter_id DESC
                                             ) AS row_num
                                  from PreExposure
                                           join mamba_dim_client client on PreExposure.client_id = client.client_id
                                  where (followup_date_followup_1 <= REPORT_END_DATE
                                      or followup_date_followup_1 is null)
                                    AND TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 15),
         tx_new as (select *
                    from tmp_latest_follow_up
                    WHERE treatment_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                      AND type_of_client = 'New client'
                      AND row_num = 1

             -- AND follow_up_status
         ),
         tx_curr as (select *
                     from tmp_latest_follow_up
                     where row_num = 1
                         and ((prep_dose_end_date >= REPORT_END_DATE or
                               DATE_ADD(treatment_start_date, INTERVAL dose_dispensed DAY) >=
                               REPORT_END_DATE) -- Param 6 @end_date, Param 7 @end_date
                               )
                        or (followup_date_followup_1 BETWEEN REPORT_START_DATE AND REPORT_END_DATE AND
                            (final_hiv_test_result = 'Positive' OR follow_up_status = 'Restart medication' OR follow_up_status = 'Alive')) -- Param 8 @end_date, Param 9 @end_date
                         and prep_started = 'Yes'),
         prep_new_agg AS (
             SELECT
                 COUNT(*) AS Value,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u19_m,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END) AS u19_f,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u24_m,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END) AS u24_f,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u29_m,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END) AS u29_f,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u34_m,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END) AS u34_f,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u39_m,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END) AS u39_f,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u44_m,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END) AS u44_f,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u49_m,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END) AS u49_f,
                 SUM(CASE WHEN age >= 50 AND sex = 'Male'                THEN 1 ELSE 0 END) AS o50_m,
                 SUM(CASE WHEN age >= 50 AND sex = 'Female'              THEN 1 ELSE 0 END) AS o50_f,
                 SUM(CASE WHEN do_you_have_an_hiv_positive_partner = 'Yes' OR sex_worker = 'Yes' OR (do_you_have_an_hiv_positive_partner = 'No' AND sex_worker = 'No') THEN 1 ELSE 0 END) AS cat_total,
                 SUM(CASE WHEN do_you_have_an_hiv_positive_partner = 'Yes' OR sex_worker != 'Yes' THEN 1 ELSE 0 END) AS discordant,
                 SUM(CASE WHEN sex_worker = 'Yes' OR (do_you_have_an_hiv_positive_partner = 'No' AND sex_worker = 'No') THEN 1 ELSE 0 END) AS fsw
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM tx_new) t
         ),
         prep_curr_agg AS (
             SELECT
                 COUNT(*) AS Value,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u19_m,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND sex = 'Female' THEN 1 ELSE 0 END) AS u19_f,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u24_m,
                 SUM(CASE WHEN age BETWEEN 20 AND 24 AND sex = 'Female' THEN 1 ELSE 0 END) AS u24_f,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u29_m,
                 SUM(CASE WHEN age BETWEEN 25 AND 29 AND sex = 'Female' THEN 1 ELSE 0 END) AS u29_f,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u34_m,
                 SUM(CASE WHEN age BETWEEN 30 AND 34 AND sex = 'Female' THEN 1 ELSE 0 END) AS u34_f,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u39_m,
                 SUM(CASE WHEN age BETWEEN 35 AND 39 AND sex = 'Female' THEN 1 ELSE 0 END) AS u39_f,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u44_m,
                 SUM(CASE WHEN age BETWEEN 40 AND 44 AND sex = 'Female' THEN 1 ELSE 0 END) AS u44_f,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Male'   THEN 1 ELSE 0 END) AS u49_m,
                 SUM(CASE WHEN age BETWEEN 45 AND 49 AND sex = 'Female' THEN 1 ELSE 0 END) AS u49_f,
                 SUM(CASE WHEN age >= 50 AND sex = 'Male'                THEN 1 ELSE 0 END) AS o50_m,
                 SUM(CASE WHEN age >= 50 AND sex = 'Female'              THEN 1 ELSE 0 END) AS o50_f,
                 SUM(CASE WHEN do_you_have_an_hiv_positive_partner = 'Yes' OR sex_worker = 'Yes' OR (do_you_have_an_hiv_positive_partner = 'No' AND sex_worker = 'No') THEN 1 ELSE 0 END) AS cat_total,
                 SUM(CASE WHEN do_you_have_an_hiv_positive_partner = 'Yes' AND sex_worker != 'Yes'                                                                     THEN 1 ELSE 0 END) AS discordant,
                 SUM(CASE WHEN sex_worker = 'Yes' OR (do_you_have_an_hiv_positive_partner = 'No' AND sex_worker = 'No')                                                 THEN 1 ELSE 0 END) AS fsw
             FROM (SELECT *, TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age FROM tx_curr) t
         )
    SELECT 'HIV_PrEP'    AS S_NO, 'Number of individuals receiving Pre-Exposure Prophylaxis'                              AS Activity, Value    AS Value FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1',    'PrEP (New Number of individuals who were newly enrolled on PrEP)',                Value    FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1',  'By age and Sex',                                                                  Value    FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 1',  '15 - 19 years, Male',   COALESCE(u19_m, 0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 2',  '15 - 19 years, Female', COALESCE(u19_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 3',  '20 - 24 years, Male',   COALESCE(u24_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 4',  '20 - 24 years, Female', COALESCE(u24_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 5',  '25 - 29 years, Male',   COALESCE(u29_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 6',  '25 - 29 years, Female', COALESCE(u29_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 7',  '30 - 34 years, Male',   COALESCE(u34_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 8',  '30 - 34 years, Female', COALESCE(u34_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 9',  '35 - 39 years, Male',   COALESCE(u39_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 10', '35 - 39 years, Female', COALESCE(u39_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 11', '40 - 44 years, Male',   COALESCE(u44_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 12', '40 - 44 years, Female', COALESCE(u44_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 13', '45 - 49 years, Male',   COALESCE(u49_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 14', '45 - 49 years, Female', COALESCE(u49_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 15', '>= 50 years, Male',     COALESCE(o50_m,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.1. 16', '>= 50 years, Female',   COALESCE(o50_f,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.2',  'By Client Category',        COALESCE(cat_total,0)  FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.2. 1', 'Discordant Couple',       COALESCE(discordant,0) FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP.1.2. 2', 'Female sex worker[FSW]',  COALESCE(fsw,0)        FROM prep_new_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1', 'PrEP Curr (Number of individuals that received oral PrEP during the reporting period)', Value FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1',  'By age and Sex',          Value  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 1',  '15 - 19 years, Male',   COALESCE(u19_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 2',  '15 - 19 years, Female', COALESCE(u19_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 3',  '20 - 24 years, Male',   COALESCE(u24_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 4',  '20 - 24 years, Female', COALESCE(u24_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 5',  '25 - 29 years, Male',   COALESCE(u29_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 6',  '25 - 29 years, Female', COALESCE(u29_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 7',  '30 - 34 years, Male',   COALESCE(u34_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 8',  '30 - 34 years, Female', COALESCE(u34_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 9',  '35 - 39 years, Male',   COALESCE(u39_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 10', '35 - 39 years, Female', COALESCE(u39_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 11', '40 - 44 years, Male',   COALESCE(u44_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 12', '40 - 44 years, Female', COALESCE(u44_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 13', '45 - 49 years, Male',   COALESCE(u49_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 14', '45 - 49 years, Female', COALESCE(u49_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 15', '>= 50 years, Male',     COALESCE(o50_m,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.1. 16', '>= 50 years, Female',   COALESCE(o50_f,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.2',   'By Client Category',      COALESCE(cat_total,0)  FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.2. 1', 'Discordant Couple',      COALESCE(discordant,0) FROM prep_curr_agg
    UNION ALL SELECT 'HIV_PrEP_CURR.2. 2', 'Female sex worker[FSW]', COALESCE(fsw,0)        FROM prep_curr_agg;
END //

DELIMITER ;