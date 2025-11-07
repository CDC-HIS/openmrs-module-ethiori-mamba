DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_contacts_query;

CREATE PROCEDURE sp_fact_line_list_ict_contacts_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH contact_list as (select contact.client_id,
                                 contact.elicited_date,
                                 contact.hiv_test_date,
                                 contact.hiv_test_result,
                                 hivst_result,
                                 hivst_kit_distributed_date,
                                 hivst_report_date,
                                 contact_1.is_ipv_intimate_partner_violence_risk_assessed,
                                 contact_1.prior_hiv_test_result,
                                 g.ict_serial_number,
                                 contact_1.prior_test_date_estimated,
                                 contact_1.ipv_intimate_partner_violence_risk_outcome,
                                 contact_1.hiv_status_disclosed_by_index_client,
                                 contact.trial_date_1st,
                                 contact_1.trial_outcome_1st,
                                 contact.trial_date_2nd,
                                 contact_1.trial_outcome_2nd,
                                 contact.trial_date_3rd,
                                 contact_1.trial_outcome_3rd,
                                 contact_1.date_of_case_closure,
                                 contact_1.notification_plan,
                                 contact.contact_category,
                                 COALESCE(ae_type_3rd_report, ae_type_2nd_report, ae_type_1st_report) as adverse_event_type,
                                 COALESCE(ae_for_ipv_report_date_3rd, ae_for_ipv_report_date_2nd,
                                          ae_for_ipv_report_date_1st)                                 as adverse_event_reported,
                                 hiv_selftest_performed,
                                 date_linked_to_care,
                                 ict_indexclient_testing,
                                 reason_if_not_linked_to_ict,
                                 date_linked_to_ict_service,
                                 art_antiretroviral_start_date,
                                 link_date_ict,
                                 state_province,
                                 contact.first_name,
                                 contact.middle_name,
                                 contact.last_name,
                                 person.gender,
                                 person.uuid,
                                 person.person_name_long,
                                 client.uan,
                                 client.phone_no,
                                 city_village,
                                 mrn
                          from mamba_flat_encounter_ict_general g
                                   join mamba_flat_encounter_index_contact_followup contact
                                        on g.client_id = contact.client_id
                                   left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                             on contact.encounter_id = contact_1.encounter_id
                                   left join mamba_dim_encounter encounter
                                             on contact.encounter_id = encounter.encounter_id
                                   left join mamba_dim_person_attribute attribute on encounter.uuid = attribute.value
                                   left join mamba_dim_relationship relationship on attribute.person_id = person_b
                                   left join mamba_dim_person person on relationship.person_b = person.person_id
                                   left join mamba_dim_client client on person.person_id = client.client_id)
    select CONCAT_WS(' ', first_name, middle_name, last_name)                    as `Contact’s Full Name`,
           elicited_date                                                         as `Elicited date`,

           state_province                                                        as Region,
           ''                                                                    as Zone,
           city_village                                                          as Woreda,
           phone_no                                                              as `Phone`,
           contact_category                                                      as `Contact Category (1-5)`,
           is_ipv_intimate_partner_violence_risk_assessed                        as `IPV Risk Assessed`,
           ipv_intimate_partner_violence_risk_outcome                            as `IPV outcome [1-5]`,
           hiv_status_disclosed_by_index_client                                  as `HIV status disclosed by Index Client?`,
           notification_plan                                                     as `Notification plan`,
           trial_date_1st                                                        as `1st  Trial Date`,
           trial_outcome_1st                                                     as `1stTrial Outcome`,
           trial_date_2nd                                                        as `2nd Trial Date`,
           trial_outcome_2nd                                                     as `2nd Trial Outcome`,
           trial_date_3rd                                                        as `3rd Trial Date`,
           trial_outcome_3rd                                                     as `3rd Trial Outcome`,

           hivst_kit_distributed_date                                            as `Date Self-Test Kit distributed`,
           hivst_report_date                                                     as `HIV Self-Test Report Date`,
           hivst_result                                                          as `HIV Self-Test Result`,
           CASE WHEN hiv_test_date IS NOT NULL THEN 'YES' ELSE 'NO' END          AS `HIV Test Done by National Algorithm`,
           hiv_test_date                                                         as `Date HIV Test Done by National Algorithm`,
           hiv_test_result                                                       as `HIV Test Result by National Algorithm`,
           date_linked_to_care                                                      `Date linked to care`,
           art_antiretroviral_start_date                                            `Date ART started`,
           uan                                                                   as `UAN of newly identified HIV +ve`,
           mrn                                                                   as `MRN of newly identified HIV +ve`,
           ict_indexclient_testing                                                  `Client Linked to ICT Service`,
           reason_if_not_linked_to_ict                                              `Reason if not linked to ICT`,
           date_linked_to_ict_service                                               `Date linked to ICT Service`,
           CASE WHEN adverse_event_reported is not null THEN 'Yes' ELSE 'No' END as `Adverse Event Reported`,
           adverse_event_type                                                    as `Adverse Event Type`,
           uuid                                                                  as `GUID of Index Case`,
           person_name_long                                                      as `Full Name of Index Case`,
           ict_serial_number                                                     as `ICT#`
    from contact_list
    WHERE elicited_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE;

END //

DELIMITER ;