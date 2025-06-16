DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_hvl_query;

CREATE PROCEDURE sp_fact_line_list_hvl_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    WITH FollowUp AS (select follow_up.encounter_id,
                             follow_up.client_id,
                             follow_up_date_followup_                          as follow_up_date,
                             follow_up_status,
                             art_antiretroviral_start_date                     as art_start_date,
                             regimen_change                                    as switch,
                             date_of_reported_hiv_viral_load                      viral_load_sent_date,
                             date_viral_load_results_received                     viral_load_performed_date,
                             viral_load_test_status,
                             hiv_viral_load                                    as viral_load_count,
                             date_third_enhanced_adherence_counseling_provided as eac_3,
                             date_second_enhanced_adherence_counseling_provided   eac_2,
                             date_first_enhanced_adherence_counseling_provided    eac_1,
                             weight_text_                                      as weight,
                             date_of_event                                     as hiv_confirmed_date,
                             pregnancy_status,
                             antiretroviral_art_dispensed_dose_i                  dispensed_dose,
                             regimen,
                             next_visit_date,
                             treatment_end_date                                   art_dose_end_date,
                             hiv_viral_load_status,
                             viral_load_test_indication,
                             COALESCE(
                                     at_3436_weeks_of_gestation,
                                     viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                     viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                     every_six_months_until_mtct_ends,
                                     six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                     three_months_after_delivery,
                                     at_the_first_antenatal_care_visit,
                                     annual_viral_load_test,
                                     second_viral_load_test_at_12_months_post_art,
                                     first_viral_load_test_at_6_months_or_longer_post_art,
                                     first_viral_load_test_at_3_months_or_longer_post_art
                             )                                                 AS routine_viral_load_test_indication,
                             COALESCE(
                                     repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                     suspected_antiretroviral_failure
                             )                                                 AS targeted_viral_load_test_indication,
                             annual_viral_load_test
                      from mamba_flat_encounter_follow_up follow_up
                               LEFT join mamba_flat_encounter_follow_up_1 follow_up_1
                                         on follow_up.encounter_id = follow_up_1.encounter_id
                               LEFT join mamba_flat_encounter_follow_up_2 follow_up_2
                                         on follow_up.encounter_id = follow_up_2.encounter_id
                               left join mamba_flat_encounter_follow_up_3 follow_up_3
                                         on follow_up.encounter_id = follow_up_3.encounter_id
                               left join mamba_flat_encounter_follow_up_4 follow_up_4
                                         on follow_up.encounter_id = follow_up_4.encounter_id),


         tmp_vl_performed_date as (select encounter_id,
                                          client_id,
                                          viral_load_performed_date,
                                          viral_load_sent_date,
                                          follow_up_date,
                                          hiv_viral_load_status,
                                          viral_load_test_indication,
                                          routine_viral_load_test_indication,
                                          targeted_viral_load_test_indication,
                                          viral_load_count,
                                          viral_load_test_status,
                                          eac_1,
                                          eac_2,
                                          eac_3,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_performed_date DESC , FollowUp.encounter_id DESC) AS row_num
                                   from FollowUp
                                   where hiv_viral_load_status = 'Completed'
                                     and (
                                       (viral_load_test_indication = 'Routine viral load test indication'
                                           and routine_viral_load_test_indication in
                                               ('First viral load test at 6 months or longer post ART',
                                                'Second viral load test at 12 months post ART',
                                                'Annual viral load test',
                                                'First viral load test at 3 months or longer post ART',
                                                'At the first antenatal care visit',
                                                'At 34-36 weeks of gestation'
                                                    'Three months after delivery',
                                                'Six months after the first viral load test at postnatal period',
                                                'Every six months until MTCT ends'))
                                           OR (viral_load_test_indication = 'Targeted viral load test indication'
                                           and targeted_viral_load_test_indication in
                                               ('Suspected Antiretroviral failure')
                                           and (viral_load_count > 50 or viral_load_test_status in ('Uncontrolled',
                                                                                                    'HIV infection with high viral load',
                                                                                                    'Low-level viremia'))
                                           )
                                       )
                                     and (
                                           (REPORT_START_DATE is not null and REPORT_END_DATE is not null and
                                            (viral_load_performed_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE))
                                               OR
                                           (REPORT_START_DATE is null and REPORT_END_DATE is null and
                                            viral_load_performed_date <= CURDATE())
                                       )),
         vl_performed_date as (select * from tmp_vl_performed_date where row_num = 1),

         tmp_vl_performed_date_cf as (select FollowUp.encounter_id,
                                             FollowUp.client_id,
                                             FollowUp.viral_load_performed_date,
                                             FollowUp.viral_load_sent_date,
                                             FollowUp.follow_up_date,
                                             FollowUp.hiv_viral_load_status,
                                             FollowUp.viral_load_test_indication,
                                             FollowUp.routine_viral_load_test_indication,
                                             FollowUp.targeted_viral_load_test_indication,
                                             FollowUp.viral_load_count,
                                             FollowUp.viral_load_test_status,
                                             FollowUp.eac_1,
                                             FollowUp.eac_2,
                                             FollowUp.eac_3,
                                             ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_performed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                      from FollowUp
                                               left join vl_performed_date on FollowUp.client_id = vl_performed_date.client_id
                                      where FollowUp.hiv_viral_load_status = 'Completed'
                                        and (
                                          (FollowUp.viral_load_test_indication = 'Routine viral load test indication'
                                              and FollowUp.routine_viral_load_test_indication in
                                                  ('Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                   'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'
                                                      ))
                                              OR
                                          (FollowUp.viral_load_test_indication = 'Targeted viral load test indication'
                                              and FollowUp.targeted_viral_load_test_indication in
                                                  ('Repeat or confirmatory VL: initial viral load greater than 1000')
                                              )
                                          )
                                        and FollowUp.viral_load_performed_date >=
                                            vl_performed_date.viral_load_performed_date
                                        and FollowUp.viral_load_performed_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         vl_performed_date_cf as (select * from tmp_vl_performed_date_cf where row_num = 1),

         tmp_switch_sub_date AS (SELECT FollowUp.encounter_id,
                                        FollowUp.client_id,
                                        FollowUp.follow_up_date                                                                                                 as switch_date,
                                        ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                 FROM FollowUp
                                          left join vl_performed_date on FollowUp.client_id = vl_performed_date.client_id
                                 WHERE switch is not null
                                   and FollowUp.follow_up_date >= vl_performed_date.viral_load_performed_date
                                   and FollowUp.follow_up_date <= COALESCE(REPORT_END_DATE, CURDATE())),
         switch_sub_date AS (select * from tmp_switch_sub_date where row_num = 1),
         tmp_latest_follow_up AS (SELECT client_id,
                                         FollowUp.encounter_id,
                                         follow_up_status,
                                         art_dose_end_date,
                                         art_start_date,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) as row_num
                                  FROM FollowUp
                                  where follow_up_status Is Not Null),
         latest_follow_up as (select *
                              from tmp_latest_follow_up
                              where row_num = 1),

         hvl as (SELECT client.patient_name                                                             as `Patient Name`,
                        client.patient_uuid                                                             as `UUID`,
                        client.mrn,
                        client.uan,
                        TIMESTAMPDIFF(YEAR, client.date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) AS age,
                        Sex,
                        f_case.weight,
                        f_case.hiv_confirmed_date                                                       as `HIV Confirmed Date`,
                        f_case.hiv_confirmed_date                                                       as `HIV Confirmed Date EC.`,
                        f_case.art_start_date as `ART Start Date`,
                        f_case.art_start_date as `ART Start Date EC.`,
                        f_case.follow_up_date                                                           as `Follow Up Date`,
                        f_case.follow_up_date                                                           as `Follow Up Date EC.`,
                        f_case.pregnancy_status                                                         as IsPregnant,
                        f_case.dispensed_dose                                                           as `ART Regimen`,
                        f_case.regimen                                                                  as art_dose,
                        f_case.next_visit_date as `Next Visit Date`,
                        f_case.next_visit_date as `Next Visit Date EC.`,
                        f_case.follow_up_status,
                        f_case.art_dose_end_date                                                        as `ART Dose End Date`,
                        f_case.art_dose_end_date                                                        as `ART Dose End Date EC.`,
                        vlperfdate.viral_load_sent_date                                                 as `First VL Sent Date`,
                        vlperfdate.viral_load_sent_date                                                 as `First VL Sent Date EC.`,
                        vlperfdate.viral_load_performed_date                                            as `First VL Received Date`,
                        vlperfdate.viral_load_performed_date                                            as `First VL Received Date EC.`,
                        CASE vlperfdate.viral_load_test_status
                            WHEN 'Uncontrolled'
                                THEN 'Unsuppressed'
                            WHEN 'HIV infection with high viral load'
                                THEN 'High Viral Load (>1000 copies/ML)'
                            WHEN 'Low-level viremia'
                                THEN 'Low Level Viremia (51-1000)'
                            END                                                                         as `First VL Status`,
                        vlperfdate.viral_load_count                                                     as `First VL Count`,
                        COALESCE(vlperfdate.targeted_viral_load_test_indication,
                                 vlperfdate.routine_viral_load_test_indication)                         as `First VL Indication`,
                        sub_switch_date.switch_date                                                     as `Regimen Change Date`,
                        sub_switch_date.switch_date                                                     as `Regimen Change Date EC.`,
                        vlperfdate.eac_1                                                                as `First VL EAC1`,
                        vlperfdate.eac_1                                                                as `First VL EAC1 EC.`,
                        vlperfdate.eac_2                                                                as `First VL EAC2`,
                        vlperfdate.eac_2                                                                as `First VL EAC2 EC.`,
                        vlperfdate.eac_3                                                                as `First VL EAC3`,
                        vlperfdate.eac_3                                                                as `First VL EAC3 EC.`,
                        vlperfdate_cf.viral_load_sent_date                                              as `Confirmatory VL Sent Date`,
                        vlperfdate_cf.viral_load_sent_date                                              as `Confirmatory VL Sent Date EC.`,
                        vlperfdate_cf.viral_load_performed_date                                         as `Confirmatory VL Received Date`,
                        vlperfdate_cf.viral_load_performed_date                                         as `Confirmatory VL Received Date EC.`,
                        CASE vlperfdate_cf.viral_load_test_status
                            WHEN 'Uncontrolled'
                                THEN 'Unsuppressed'
                            WHEN 'HIV infection with high viral load'
                                THEN 'High Viral Load (>1000 copies/ML)'
                            WHEN 'Low-level viremia'
                                THEN 'Low Level Viremia (51-1000)'
                            END
                                                                                                        as `Confirmatory VL Status`,
                        vlperfdate_cf.viral_load_count                                                  as `Confirmatory VL Count`,
                        COALESCE(vlperfdate_cf.targeted_viral_load_test_indication,
                                 vlperfdate_cf.routine_viral_load_test_indication)                      as `Confirmatory VL Indication`,
                        vlperfdate_cf.eac_1                                                      as `Confirmatory EAC1  Date`,
                        vlperfdate_cf.eac_1                                                      as `Confirmatory EAC1  Date EC.`,
                        vlperfdate_cf.eac_2                                                      as `Confirmatory EAC2  Date`,
                        vlperfdate_cf.eac_2                                                      as `Confirmatory EAC2  Date EC.`,
                        vlperfdate_cf.eac_3                                                      as `Confirmatory EAC3  Date`,
                        vlperfdate_cf.eac_3                                                      as `Confirmatory EAC3  Date EC.`
                 FROM FollowUp AS f_case
                          JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
                          JOIN vl_performed_date as vlperfdate ON vlperfdate.client_id = f_case.client_id
                          LEFT JOIN mamba_dim_client client on f_case.client_id = client.client_id
                          LEFT JOIN vl_performed_date_cf as vlperfdate_cf
                                    ON vlperfdate_cf.client_id = f_case.client_id
                          Left join switch_sub_date as sub_switch_date ON sub_switch_date.client_id = f_case.client_id)
    select *
    from hvl;

END //

DELIMITER ;