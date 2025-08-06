DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_dsd_query;

CREATE PROCEDURE sp_fact_line_list_dsd_query(IN REPORT_START_DATE DATE,
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
                                 END                             as follow_up_status,
                             follow_up_date_followup_            as follow_up_date,
                             art_antiretroviral_start_date       as art_start_date,
                             height,
                             mfeia.date_hiv_confirmed            as date_hiv_confirmed,
                             current_who_hiv_stage,
                             cd4_count,
                             follow_up_2.weight_text_            as weight,
                             antiretroviral_art_dispensed_dose_i AS art_dose_days,
                             regimen,
                             anitiretroviral_adherence_level     as adherence,
                             next_visit_date,
                             treatment_end_date,
                             assessment_status,
                             assessment_date                     as assessment_date,
                             dsd_category                        as current_dsd_category
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
                      WHERE assessment_date >= COALESCE(REPORT_START_DATE, CURDATE())
                        AND assessment_date <= COALESCE(REPORT_END_DATE, CURDATE())),


         tmp_latest_follow_up AS (SELECT f.*,
                                         ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY assessment_date DESC,
                                             encounter_id DESC) AS row_num
                                  FROM FollowUp as f
                                           join mamba_dim_client client on f.client_id = client.client_id),


         latest_follow_up AS (select *
                              from tmp_latest_follow_up
                              where row_num = 1),


         initial_dsd_assesment as (select follow_up_3.encounter_id,
                                          follow_up_3.client_id,
                                          follow_up_3.assessment_date    as initial_dsd_assesment_date,
                                          follow_up_2.dsd_category       as dsd_category_at_enrollement,
                                          follow_up_8.dsd_category_changed_by_this_date,
                                          follow_up_7.reason_for_dsd_category_change,
                                          ROW_NUMBER() OVER (PARTITION BY follow_up_3.client_id ORDER BY follow_up_3.assessment_date ,
                                              follow_up_3.encounter_id ) AS row_num
                                   from mamba_flat_encounter_follow_up_3 as follow_up_3
                                            inner join
                                        latest_follow_up as l on
                                            follow_up_3
                                                .client_id = l.client_id
                                            inner join mamba_flat_encounter_follow_up_2 as follow_up_2 on
                                       follow_up_2.encounter_id = follow_up_3.encounter_id and follow_up_3
                                                                                                   .assessment_date <>
                                                                                               l.assessment_date
                                            inner join mamba_flat_encounter_follow_up_8 as follow_up_8 on
                                       follow_up_8.encounter_id = follow_up_3.encounter_id
                                            inner join mamba_flat_encounter_follow_up_7 as follow_up_7 on
                                       follow_up_7.encounter_id = follow_up_2.encounter_id),

         deduplicat_dsd_assesment AS (SELECT * FROM initial_dsd_assesment WHERE row_num = 1)


    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,
           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.date_hiv_confirmed                           as `Date Confirmed HIV+ EC.`,
           f_case.date_hiv_confirmed                           as `Date Confirmed HIV+ G.C.`,
           f_case.art_start_date                               as `ART Start Date EC.`,
           f_case.art_start_date                               as `ART Start Date G.C`,
           client.mobile_no                                    as `Mobile Phone Number.`,
           f_case.follow_up_date                               as `Latest Follow-Up Date EC.`,
           f_case.follow_up_date                               as `Latest Follow-Up Date G.C.`,
           f_case.follow_up_status                             AS `Latest Follow-Up Status`,
           f_case.regimen                                      as `Latest Regimen`,
           f_case.art_dose_days                                as `Latest ARV Dose Days`,
           f_case.adherence                                    as `Latest Adherence`,
           assessment_status                                   as `Assessment Status`,
           f_case.next_visit_date                              as `Next Visit Date EC.`,
           f_case.next_visit_date                              as `Next Visit Date G.C.`,
           f_case.treatment_end_date                           as `Last TX_CURR Date EC.`,
           f_case.treatment_end_date                           as `Last TX_CURR Date G.C.`,
           case
               when initial_dsd_assesment_date is null
                   then f_case.assessment_date
               else initial_dsd_assesment_date end
                                                               as `Initial DSD Assessment Date EC.`,
           case
               when initial_dsd_assesment_date is null
                   then f_case.assessment_date
               else initial_dsd_assesment_date end             as `Initial DSD Assessment Date G.C`,

           case
               when initial_dsd_assesment_date is null
                   then f_case.current_dsd_category
               else dsd_category_at_enrollement end            as `DSD Category at Enrollement`,
           f_case.assessment_date                              as `Current DSD Category Entry Date in EC.`,
           f_case.assessment_date                              as `Current DSD Category Entry Date in G.C.`,
           current_dsd_category                                as `Current DSD Category`,
           dsd_category_changed_by_this_date                   as `DSD Category Changed by this Date`,
           reason_for_dsd_category_change                      as `Reason for DSD Category Change`
    FROM latest_follow_up AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
             LEFT JOIN deduplicat_dsd_assesment as d on d.client_id = f_case.client_id
    ORDER BY client.patient_name;


END //

DELIMITER ;
