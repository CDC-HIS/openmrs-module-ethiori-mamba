DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_tb_art_query;

CREATE PROCEDURE sp_fact_line_list_tx_tb_art_query(IN REPORT_START_DATE DATE,
                                                   IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             CASE follow_up_status
                                 WHEN 'Alive' THEN 'Alive on ART'
                                 WHEN 'Restart medication' THEN 'Restart'
                                 WHEN 'Transferred out' THEN 'TO'
                                 WHEN 'Stop all' THEN 'Stop'
                                 WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                 WHEN 'Ran away' THEN 'Drop'
                                 END                                   as follow_up_status,
                             follow_up_date_followup_                  as follow_up_date,
                             art_antiretroviral_start_date             as art_start_date,
                             height,
                             pregnancy_status,
                             currently_breastfeeding_child,
                             specimen_sent_to_lab,
                             mfeia.date_hiv_confirmed                  as date_hiv_confirmed,
                             current_who_hiv_stage,
                             tb_screening_date,
                             follow_up_2.weight_text_                  as weight,
                             antiretroviral_art_dispensed_dose_i       AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level           as adherence,
                             next_visit_date,
                             treatment_end_date,
                             lf_lam_result,
                             gene_xpert_result,
                             diagnostic_test                           as other_diagnostic_test,
                             tb_diagnostic_test_result,
                             diagnosis_date,
                             tb_treatment_status,
                             screening_test_result_tuberculosis        as screening_result,
                             tuberculosis_drug_treatment_start_d       as tb_treatment_start_date,
                             date_active_tbrx_completed       as tb_treatment_completed_date,
                             date_active_tbrx_dc       as tb_treatment_discontinued_date,
                             mfepe.operation_triple_zero_enrollment_da as booking_date
                      FROM mamba_flat_encounter_intake_a mfeia
                               LEFT JOIN mamba_flat_encounter_follow_up follow_up
                                         on follow_up.client_id = mfeia.client_id
                               LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                         ON follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                         ON follow_up.encounter_id = follow_up_2.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                         ON follow_up.encounter_id = follow_up_3.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                         ON follow_up.encounter_id = follow_up_4.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5
                                         ON follow_up.encounter_id = follow_up_5.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6
                                         ON follow_up.encounter_id = follow_up_6.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7
                                         ON follow_up.encounter_id = follow_up_7.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8
                                         ON follow_up.encounter_id = follow_up_8.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9
                                         ON follow_up.encounter_id = follow_up_9.encounter_id
                               LEFT JOIN mamba_flat_encounter_registration reg
                                         ON reg.client_id = follow_up.client_id
                               LEFT JOIN mamba_flat_encounter_pmtct_enrollment as mfepe
                                         ON mfepe.client_id = follow_up.client_id
                      WHERE tuberculosis_drug_treatment_start_d >= COALESCE(REPORT_START_DATE, CURDATE())
                        AND tuberculosis_drug_treatment_start_d <= COALESCE(REPORT_END_DATE, CURDATE())
                       or  (diagnosis_date >= COALESCE(REPORT_START_DATE, CURDATE()) and
                            diagnosis_date <= COALESCE(REPORT_END_DATE, CURDATE()))
                        AND follow_up_status in ('Alive', 'Restart medication')),


         tmp_latest_follow_up AS (SELECT f.*,
                                         ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp as f
                                           join mamba_dim_client client on f.client_id = client.client_id),


         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1),

        only_active_tb as (select * from latest_follow_up where
        tb_treatment_completed_date is null and
        tb_treatment_discontinued_date is null)


    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.art_start_date                               as `ART Start Date EC.`,
           f_case.art_start_date                               as `ART Start Date G.C`,
           f_case.follow_up_date                               as `Follow-Up Date EC.`,
           f_case.follow_up_date                               as `Follow-Up Date G.C.`,
           f_case.follow_up_status                             AS `Follow-Up Status`,
           f_case.regimen                                      as `Regimen`,
           f_case.art_dose_days                                as `ARV Dose Days`,
           f_case.adherence                                    as ` Adherence`,
           pregnancy_status                                    as `Pregnant?`,
           currently_breastfeeding_child                       as `Breastfeeding?`,
           case
               when client.sex = 'MALE'
                   then ''
               when booking_date is not null
                   then 'Yes'
               else 'No' end
                                                               as `On PMTCT?`,
           tb_screening_date                                   as `TB Screening Date EC.`,
           tb_screening_date                                   as `TB Screening Date G.C.`,
           screening_result                                    as `TB Screening Result`,
           specimen_sent_to_lab                                as `Specimen Sent to Lab?`,
           lf_lam_result                                       as `LF LAM Result`,
           gene_xpert_result                                   as `Gene Xpert Result`,
           other_diagnostic_test                               AS `Other Diagnostic Test`,
           tb_diagnostic_test_result                           AS `Other TB Diagnostic Test Result`,
           diagnosis_date                                      as `Active TB Diagnosed Date EC.`,
           diagnosis_date                                      as `Active TB Diagnosed Date G.C.`,
           tb_treatment_start_date                             as `TB Treatment Start Date EC.`,
           tb_treatment_start_date                             as `TB Treatment Start Date G.C.`,
           tb_treatment_status                                 as `TB Treatment Status`,
           tb_treatment_completed_date                         as `TB Treatment Completed Date EC.`,
           tb_treatment_completed_date                         as `TB Treatment Completed Date G.C.`,
           tb_treatment_discontinued_date                      as `TB Treatment Discontinued Date EC.`,
           tb_treatment_discontinued_date                      as `TB Treatment Discontinued Date G.C.`

    FROM only_active_tb AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
    ORDER BY client.patient_name;


END //

DELIMITER ;
