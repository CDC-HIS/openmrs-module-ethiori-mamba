DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_tbt_denominator_query;

CREATE PROCEDURE sp_fact_line_list_tx_tbt_denominator_query(IN REPORT_START_DATE DATE,
                                                            IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_date_followup_            as follow_up_date,
                             art_antiretroviral_start_date       as art_start_date,
                             height,
                             specimen_sent_to_lab,
                             mfeia.date_hiv_confirmed            as date_hiv_confirmed,
                             follow_up_2.weight_text_            as weight,
                             treatment_end_date,
                             date_started_on_tuberculosis_prophy as tpt_start_date,
                             date_completed_tuberculosis_prophyl as tpt_completed_date,
                             tb_prophylaxis_type                 as tpt_type,
                             tpt_followup_6h_                    as tpt_followup_status,
                             date_discontinued_tuberculosis_prop as tpt_discontinue_date,
                             tpt_dispensed_dose_in_days_inh_     as tpt_dosday_type,
                             adherence                           as tpt_adherence,
                             eligibility_status

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
                      WHERE date_started_on_tuberculosis_prophy >= COALESCE(REPORT_START_DATE, CURDATE())
                        AND date_started_on_tuberculosis_prophy <= COALESCE(REPORT_END_DATE, CURDATE())),


         tmp_latest_tpt AS (SELECT f.*,
                                   ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY follow_up_date DESC,
                                       encounter_id DESC) AS row_num
                            FROM FollowUp as f
                                     join mamba_dim_client client on f.client_id = client.client_id),


         latest_tpt AS (select *
                        from tmp_latest_tpt
                        where row_num = 1),

         temp_latest_follow_up as (select tpt.eligibility_status,
                                          tpt.follow_up_date,
                                          tpt.tpt_type,
                                          tpt.tpt_adherence,
                                          tpt.tpt_dosday_type,
                                          tpt.tpt_discontinue_date,
                                          tpt.tpt_start_date,
                                          tpt.date_hiv_confirmed,
                                          tpt.art_start_date,
                                          tpt.tpt_completed_date,
                                          tpt.weight,
                                          tpt.client_id,
                                          tpt.tpt_followup_status,
                                          follow_6.encounter_id,
                                          follow_6.follow_up_date_followup_        as last_follow_up_date,
                                          CASE follow_4.follow_up_status
                                              WHEN 'Alive' THEN 'Alive on ART'
                                              WHEN 'Restart medication' THEN 'Restart'
                                              WHEN 'Transferred out' THEN 'TO'
                                              WHEN 'Stop all' THEN 'Stop'
                                              WHEN 'Loss to follow-up (LTFU)' THEN 'Lost'
                                              WHEN 'Ran away' THEN 'Drop'
                                              END                                  as last_follow_up_status,
                                          follow_3.next_visit_date,
                                          follow_1.regimen                         as last_regmin,
                                          follow_8
                                              .antiretroviral_art_dispensed_dose_i as last_arv_dose_day,
                                          follow_7.anitiretroviral_adherence_level as last_adherence,
                                          transfer_in
                                   from mamba_flat_encounter_follow_up_6 follow_6
                                            inner join latest_tpt as tpt on tpt.client_id =
                                                                            follow_6.client_id
                                            inner join mamba_flat_encounter_follow_up_4 follow_4 on
                                       follow_4.encounter_id = follow_6.encounter_id
                                            left join mamba_flat_encounter_follow_up_3 follow_3 on
                                       follow_3.encounter_id = follow_6.encounter_id
                                            left join mamba_flat_encounter_follow_up_7 as follow_7 on
                                       follow_7.encounter_id = follow_6.encounter_id
                                            left join mamba_flat_encounter_follow_up_8 as follow_8 on
                                       follow_8.encounter_id = follow_6.encounter_id
                                            left join mamba_flat_encounter_follow_up_1 as follow_1 on
                                       follow_1.encounter_id = follow_4.encounter_id
                                   where follow_6.follow_up_date_followup_ >= COALESCE(REPORT_START_DATE, CURDATE())
                                     AND follow_6.follow_up_date_followup_ <= COALESCE(REPORT_END_DATE, CURDATE())),

         tmp_latest_follow_up AS (SELECT *,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY last_follow_up_date DESC,
                                             encounter_id DESC) AS r_n
                                  FROM temp_latest_follow_up),


         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where r_n = 1)

    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           date_hiv_confirmed                                  as `Hiv Confirmed Date in EC.`,
           date_hiv_confirmed                                  as `Hiv Confirmed Date in G.C.`,
           f_case.art_start_date                               as `ART Start Date EC.`,
           f_case.art_start_date                               as `ART Start Date G.C`,
           f_case.tpt_start_date                               as `TPT Start Date in EC.`,
           f_case.tpt_start_date                               as `TPT Start Date in G.C.`,
           f_case.tpt_completed_date                           as `TPT Completed Date in EC.`,
           f_case.tpt_completed_date                           as `TPT Completed Date in G.C.`,
           f_case.tpt_discontinue_date                         as `TPT Discontinued Date in EC.`,
           f_case.tpt_discontinue_date                         as `TPT Discontinued Date in G.C.`,
           f_case.tpt_type                                     as `TpT Type`,
           f_case.tpt_followup_status                          as `TPT Follow-up status`,
           f_case.tpt_dosday_type                              as `TPT Dispensed Dose`,
           f_case.tpt_adherence                                as `TPT Adherence`,
           f_case.last_follow_up_date                          as `Latest Follow-up Date in EC.`,
           f_case.last_follow_up_date                          as `Latest Follow-up Date in G.C.`,
           f_case.last_follow_up_status                        as `Latest Follow-up Status`,
           f_case.last_regmin                                  as `Latest Regimen`,
           f_case.last_arv_dose_day                            as `Latest ARV Dose Days`,
           f_case.last_adherence                               as `Latest Adherence`,
           f_case.next_visit_date                              as `Next Visit Date in EC.`,
           f_case.next_visit_date                              as `Next Visit Date in G.C.`,
           f_case.transfer_in                                  as `TI?`

    FROM latest_follow_up AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
    ORDER BY client.patient_name;


END //

DELIMITER ;
