DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_contacts_query;

CREATE PROCEDURE sp_fact_line_list_ict_contacts_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH contact_list as (select g.client_id                                              as index_client_id,
                                 contact.client_id,
                                 contact.elicited_date,
                                 contact.hiv_test_date,
                                 contact.hiv_test_result,
                                 contact.trial_date_1st,
                                 contact.trial_date_2nd,
                                 contact.trial_date_3rd,
                                 contact.contact_category,
                                 contact.hivst_result,
                                 contact.hivst_report_date,
                                 contact.first_name                                       as contact_first_name,
                                 contact.middle_name                                      as contact_middle_name,
                                 contact.last_name                                        as contact_last_name,
                                 contact_1.hivst_kit_distributed_date,
                                 contact_1.is_ipv_intimate_partner_violence_risk_assessed,
                                 contact_1.prior_hiv_test_result,
                                 contact_1.prior_test_date_estimated,
                                 contact_1.ipv_intimate_partner_violence_risk_outcome,
                                 contact_1.hiv_status_disclosed_by_index_client,
                                 contact_1.trial_outcome_1st,
                                 contact_1.trial_outcome_2nd,
                                 contact_1.trial_outcome_3rd,
                                 contact_1.date_of_case_closure,
                                 contact_1.notification_plan,
                                 COALESCE(contact_1.ae_type_3rd_report, contact_1.ae_type_2nd_report,
                                          contact_1.ae_type_1st_report)                   as adverse_event_type,
                                 COALESCE(contact_1.ae_for_ipv_report_date_3rd, contact_1.ae_for_ipv_report_date_2nd,
                                          contact_1.ae_for_ipv_report_date_1st)           as adverse_event_reported,
                                 contact_1.date_linked_to_care,
                                 contact_1.reason_if_not_linked_to_ict,
                                 contact_1.date_linked_to_ict_service,
                                 contact_1.art_antiretroviral_start_date,
                                 g.ict_serial_number,
                                 g.hiv_selftest_performed,
                                 g.ict_indexclient_testing,
                                 g.link_date_ict,
                                 index_contact.state_province,
                                 index_contact.county_district,
                                 index_contact.city_village,
                                 index_contact.phone_no,
                                 index_contact.mobile_no,
                                 index_contact.mrn,
                                 index_contact.uan,

                                 index_client.patient_uuid,
                                 index_client.given_name,
                                 index_client.middle_name,
                                 index_client.family_name,
                                 index_client.mrn                                         as index_mrn,
                                 index_client.uan                                         as index_uan,
                                 coalesce(contact_birthdate, index_contact.date_of_birth) as birthdate,
                                 CASE
                                     WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                                          ('F', 'Female gender') THEN 'Female'
                                     WHEN COALESCE(index_contact.sex, contact_1.respondent_gender) IN
                                          ('M', 'Male gender') THEN 'Male'
                                     ELSE COALESCE(index_contact.sex, contact_1.respondent_gender)
                                     END                                                  AS sex

                          from mamba_flat_encounter_index_contact_followup contact
                                   left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                             on contact.encounter_id = contact_1.encounter_id
                                   left join mamba_flat_encounter_ict_general g on contact.client_id = g.client_id
                                   left join mamba_dim_client index_client on contact.client_id = index_client.client_id
                                   left join mamba_dim_encounter encounter
                                             on contact.encounter_id = encounter.encounter_id
                                   left join mamba_dim_person_attribute attribute on encounter.uuid = attribute.value
                                   left join mamba_dim_client index_contact
                                             on attribute.person_id = index_contact.client_id),
         FollowUp AS (SELECT follow_up.client_id,
                             follow_up.encounter_id,
                             date_viral_load_results_received AS viral_load_perform_date,
                             date_of_reported_hiv_viral_load  as viral_load_sent_date,
                             viral_load_received_,
                             follow_up_status,
                             follow_up_date_followup_         AS follow_up_date,
                             art_antiretroviral_start_date       art_start_date,
                             viral_load_test_status,
                             hiv_viral_load                   AS viral_load_count,
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
                             )                                AS routine_viral_load_test_indication,
                             COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                      suspected_antiretroviral_failure
                             )                                AS targeted_viral_load_test_indication,
                             viral_load_test_indication,
                             pregnancy_status,
                             currently_breastfeeding_child    AS breastfeeding_status,
                             antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
                             regimen,
                             next_visit_date,
                             treatment_end_date,
                             date_of_event                       date_hiv_confirmed,
                             weight_text_                     as weight,
                             adherence,
                             cd4_,
                             cd4_count
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id
                               LEFT JOIN mamba_flat_encounter_follow_up_10 follow_up_10
                                         ON follow_up.encounter_id = follow_up_10.encounter_id),
         latest_follow_up_tmp AS (SELECT client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         encounter_id,
                                         follow_up_status,
                                         adherence,
                                         next_visit_date,
                                         regimen,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status IS NOT NULL
                                    AND art_start_date IS NOT NULL
                                    AND follow_up_date <= REPORT_END_DATE),
         latest_follow_up AS (select * from latest_follow_up_tmp where row_num = 1),
         vl_performed_tmp AS (SELECT FollowUp.encounter_id,
                                          FollowUp.client_id,
                                          FollowUp.viral_load_perform_date,
                                          FollowUp.viral_load_test_status,
                                          follow_up_status,
                                          arv_dispensed_dose,
                                          viral_load_count,
                                          cd4_,
                                          cd4_count,
                                          routine_viral_load_test_indication,
                                          targeted_viral_load_test_indication,
                                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                   FROM FollowUp
                                   WHERE follow_up_status IS NOT NULL
                                     AND art_start_date IS NOT NULL
                                     AND viral_load_perform_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         latest_vl_performed as (select * from vl_performed_tmp where row_num = 1)
    select CONCAT_WS(' ', contact_first_name, contact_middle_name, contact_last_name) as `Contact’s Full Name`,
           elicited_date                                                              as `Elicited date GC.`,
           elicited_date                                                              as `Elicited date EC.`,

           contact_list.state_province                                                as Region,
           contact_list.county_district                                               as Zone,
           contact_list.city_village                                                  as Woreda,
           contact_list.phone_no                                                      as `Phone`,
           contact_list.mobile_no                                                     as `Mobile`,
           contact_category                                                           as `Contact Category (1-5)`,
           is_ipv_intimate_partner_violence_risk_assessed                             as `IPV Risk Assessed`,
           ipv_intimate_partner_violence_risk_outcome                                 as `IPV outcome [1-5]`,
           hiv_status_disclosed_by_index_client                                       as `HIV status disclosed by Index Client?`,
           notification_plan                                                          as `Notification plan`,
           trial_date_1st                                                             as `1st  Trial Date GC.`,
           trial_date_1st                                                             as `1st  Trial Date EC.`,
           trial_outcome_1st                                                          as `1stTrial Outcome`,
           trial_date_2nd                                                             as `2nd Trial Date GC.`,
           trial_date_2nd                                                             as `2nd Trial Date EC.`,
           trial_outcome_2nd                                                          as `2nd Trial Outcome`,
           trial_date_3rd                                                             as `3rd Trial Date GC.`,
           trial_date_3rd                                                             as `3rd Trial Date EC.`,
           trial_outcome_3rd                                                          as `3rd Trial Outcome`,
           prior_hiv_test_result                                                      as `Prior HIV Test Result`,
           hivst_kit_distributed_date                                                 as `Date Self-Test Kit distributed GC.`,
           hivst_kit_distributed_date                                                 as `Date Self-Test Kit distributed EC.`,
           hivst_report_date                                                          as `HIV Self-Test Report Date GC.`,
           hivst_report_date                                                          as `HIV Self-Test Report Date EC.`,
           hivst_result                                                               as `HIV Self-Test Result`,
           CASE WHEN hiv_test_date IS NOT NULL THEN 'YES' ELSE 'NO' END               AS `HIV Test Done by National Algorithm`,
           hiv_test_date                                                              as `Date HIV Test Done by National Algorithm GC.`,
           hiv_test_date                                                              as `Date HIV Test Done by National Algorithm EC.`,
           hiv_test_result                                                            as `HIV Test Result by National Algorithm`,
           date_linked_to_care                                                           `Date linked to care GC.`,
           date_linked_to_care                                                           `Date linked to care EC.`,
           art_antiretroviral_start_date                                                 `Date ART started GC.`,
           art_antiretroviral_start_date                                                 `Date ART started EC.`,
           CONCAT('''', contact_list.uan)                                             as `UAN of newly identified HIV +ve`,
           contact_list.mrn                                                           as `MRN of newly identified HIV +ve`,
           ict_indexclient_testing                                                       `Client Linked to ICT Service`,
           reason_if_not_linked_to_ict                                                   `Reason if not linked to ICT`,
           date_linked_to_ict_service                                                    `Date linked to ICT Service GC.`,
           date_linked_to_ict_service                                                    `Date linked to ICT Service EC.`,
           CASE WHEN adverse_event_reported is not null THEN 'Yes' ELSE 'No' END      as `Adverse Event Reported`,
           adverse_event_type                                                         as `Adverse Event Type`,
           patient_uuid                                                               as `GUID of Index Case`,
           index_mrn                                                                  as `MRN of Index Case`,
           CONCAT('''', index_uan)                                                    as `UAN of Index Case`,
           CONCAT_WS(' ', given_name, middle_name, family_name)                       as `Full Name of Index Case`,
           latest_follow_up.follow_up_status                                          as `Follow up Status`,
           latest_follow_up.next_visit_date                                           as `Next Visit Date`,
           latest_follow_up.regimen                                                   as `Regimen`,
           latest_follow_up.adherence                                                 as `Adherence`,
           latest_vl.viral_load_test_status                                           as `Viral Load Test Status`,
           COALESCE(latest_vl.routine_viral_load_test_indication,
                    latest_vl.targeted_viral_load_test_indication)                    as `Indication`,
           latest_vl.viral_load_count                                                 as `VL Count`,
           ict_serial_number                                                          as `ICT#`
    from contact_list
             LEFT JOIN latest_follow_up
                       ON latest_follow_up.client_id = contact_list.client_id
             INNER JOIN latest_vl_performed as latest_vl
                        ON latest_vl.client_id = contact_list.client_id
    WHERE elicited_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE;

END //

DELIMITER ;