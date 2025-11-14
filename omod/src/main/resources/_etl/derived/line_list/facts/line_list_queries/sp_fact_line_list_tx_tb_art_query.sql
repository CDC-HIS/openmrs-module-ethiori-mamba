DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_tb_art_query;

CREATE PROCEDURE sp_fact_line_list_tx_tb_art_query(IN REPORT_START_DATE DATE,
                                                   IN REPORT_END_DATE DATE)
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
                             was_the_patient_screened_for_tuberc as tb_screened,
                             tb_screening_date,
                             tb_treatment_status,
                             transferred_in_check_this_for_all_t as transferred_in,
                             date_started_on_tuberculosis_prophy,
                             screening_test_result_tuberculosis     screening_result,
                             lf_lam_result,
                             patient_diagnosed_with_active_tuber,
                             tb_diagnostic_test_result,
                             diagnostic_test                     as other_diagnostic_test,
                             gene_xpert_result,
                             diagnosis_date,
                             diagnosis_date                      as active_tb_diagnosed_date,
                             tuberculosis_drug_treatment_start_d as tb_treatment_start_date,
                             date_active_tbrx_completed,
                             date_active_tbrx_dc,
                             currently_taking_tuberculosis_proph,
                             specimen_sent_to_lab,
                             antiretroviral_art_dispensed_dose_i as art_dose_days,
                             date_active_tbrx_completed          as tb_treatment_completed_date,
                             date_active_tbrx_dc                 as tb_treatment_discontinued_date,
                             adherence
                      FROM mamba_flat_encounter_follow_up follow_up
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id),
         tmp_latest_follow_up as (SELECT *,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         tb_art as (select tmp_latest_follow_up.*,
                           sex,
                           date_of_birth,
                           mrn,
                           uan
                    from tmp_latest_follow_up
                             join mamba_dim_client client on client.client_id = tmp_latest_follow_up.client_id
                    where follow_up_status in ('Alive', 'Restart medication')
                      and row_num = 1
                      and art_start_date <= REPORT_END_DATE
                      and (active_tb_diagnosed_date <= REPORT_END_DATE OR tb_treatment_start_date <= REPORT_END_DATE)
                      and (
                        (date_active_tbrx_dc is null and date_active_tbrx_completed is null and
                         (DATE_ADD(tb_treatment_start_date, INTERVAL 1 YEAR) >= REPORT_END_DATE))
                            OR
                        (date_active_tbrx_completed > REPORT_END_DATE OR date_active_tbrx_dc > REPORT_END_DATE)
                        ))


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
           breast_feeding_status                               as `Breastfeeding?`,
           ''
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

    FROM tb_art AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
    ORDER BY client.patient_name;


END //

DELIMITER ;
