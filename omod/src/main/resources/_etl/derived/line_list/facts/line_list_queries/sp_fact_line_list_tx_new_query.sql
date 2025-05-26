DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_new_query;

CREATE PROCEDURE sp_fact_line_list_tx_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_status,
                             follow_up_date_followup_            AS follow_up_date,
                             art_antiretroviral_start_date       AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child          breast_feeding_status,
                             pregnancy_status,
                             transferred_in_check_this_for_all_t as transferred_in,
                             cd4_count,
                             weight_text_                        as weight,
                             current_who_hiv_stage,
                             nutritional_screening_result,
                             screening_test_result_tuberculosis  as TB_SreeningResult,
                             date_of_event                       as hiv_confirmed_date,
                             antiretroviral_art_dispensed_dose_i    ARTDoseDays
                      FROM mamba_flat_encounter_follow_up follow_up
                               left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                    ON follow_up.encounter_id = follow_up_1.encounter_id
                               left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                    ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id),
         tmp_latest_follow_up as (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         art_start_date,
                                         cd4_count,
                                         weight,
                                         current_who_hiv_stage,
                                         nutritional_screening_result,
                                         TB_SreeningResult,
                                         hiv_confirmed_date,
                                         breast_feeding_status,
                                         ARTDoseDays,
                                         regimen,
                                         next_visit_date,
                                         treatment_end_date,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up as (SELECT *
                              from tmp_latest_follow_up
                              where row_num = 1),
         tmp_first_follow_up as (SELECT client_id,
                                        follow_up_date                                                                     AS FollowupDate,
                                        encounter_id,
                                        transferred_in,
                                        pregnancy_status,
                                        follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id ) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL),
         first_follow_up as (select * from tmp_first_follow_up where row_num = 1),

         tx_new_tmp as (select latest_follow_up.client_id,
                               latest_follow_up.art_start_date,
                               latest_follow_up.FollowupDate,
                               transferred_in,
                               pregnancy_status,
                               cd4_count,
                               weight,
                               current_who_hiv_stage,
                               nutritional_screening_result,
                               TB_SreeningResult,
                               hiv_confirmed_date,
                               breast_feeding_status,
                               regimen,
                               ARTDoseDays,
                               next_visit_date,
                               treatment_end_date
                        from latest_follow_up
                                 join first_follow_up on latest_follow_up.client_id = first_follow_up.client_id
                        where art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                          AND (first_follow_up.transferred_in is null or first_follow_up.transferred_in != 'Yes')
                          AND first_follow_up.follow_up_status in ('Alive', 'Restart medication')),
         tx_new as (select tx_new_tmp.client_id,
                           patient_name,
                           sex,
                           mrn,
                           uan,
                           weight,
                           current_who_hiv_stage,
                           nutritional_screening_result,
                           TB_SreeningResult,
                           hiv_confirmed_date,
                           art_start_date,
                           pregnancy_status,
                           breast_feeding_status,
                           regimen,
                           date_of_birth,
                           ARTDoseDays,
                           next_visit_date,
                           treatment_end_date,
                           mobile_no,
                           TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)               as age,
                           (SELECT datim_agegroup
                            from mamba_dim_agegroup
                            where age = TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)) as fine_age_group,
                           (SELECT normal_agegroup
                            from mamba_dim_agegroup
                            where age = TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)) as coarse_age_group,
                           cd4_count,
                           (SELECT datim_age_val
                            from mamba_dim_agegroup
                            where age = TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)) as datim_age_val
                    from tx_new_tmp
                             join mamba_dim_client client on tx_new_tmp.client_id = client.client_id)
    select patient_name                                                          AS 'Patient Name',
           MRN                                                                AS 'MRN',
           uan                                                                AS 'UAN',
           age                                                                AS 'Age',
           sex                                                                AS 'Sex',
           weight                                                             AS 'Weight',
           cd4_count                                                          AS 'CD4',
           current_who_hiv_stage                                              AS 'WHO Stage',
           nutritional_screening_result                                       AS 'Nutritional Status',
           TB_SreeningResult                                                  AS 'TB Screening Result',
           current_who_hiv_stage                                              AS 'Enrollment Date in E.C',
           hiv_confirmed_date    AS 'HIV Confirmed Date EC.',
           hiv_confirmed_date    AS 'HIV Confirmed Date',
           tx_new.art_start_date AS 'ART Start Date',
           tx_new.art_start_date AS 'ART Start Date EC.',
           TIMESTAMPDIFF(DAY, hiv_confirmed_date, tx_new.art_start_date)      AS 'Days Difference',
           pregnancy_status                                                   AS 'Pregnant?',
           breast_feeding_status                                              AS 'Breastfeeding?',
           regimen                                                            AS 'Regimen',
           ARTDoseDays                                                        AS 'ARV Dose Days',
           next_visit_date                                                    AS 'Next Visit Date',
           next_visit_date                                                    AS 'Next Visit Date EC.',
           treatment_end_date                                                 AS 'Treatment End Date',
           treatment_end_date                                                 AS 'Treatment End Date EC.',
           mobile_no                                                          AS 'Mobile No.'
    FROM tx_new;

END //

DELIMITER ;