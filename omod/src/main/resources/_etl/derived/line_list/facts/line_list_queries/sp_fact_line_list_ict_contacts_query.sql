DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_screening_query;

CREATE PROCEDURE sp_fact_line_list_ict_screening_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH contact_list as (select contact.client_id,
                                 contact.elicited_date,
                                 contact.hiv_test_date,
                                 contact.hiv_test_result,
                                 is_ipv_intimate_partner_violence_risk_assessed,
                                 prior_hiv_test_result,
                                 ict_serial_number,
                                 prior_test_date_estimated,
                                 ipv_intimate_partner_violence_risk_outcome,
                                 hiv_status_disclosed_by_index_client,
                                 trial_date_1st,
                                 trial_outcome_1st,
                                 trial_date_2nd,
                                 trial_outcome_2nd,
                                 trial_date_3rd,
                                 trial_outcome_3rd,
                                 date_of_case_closure,
                                 notification_plan,
                                 contact_category,
                                 person.person_name_long,
                                 person.person_name_short,
                                 person.gender,
                                 person.uuid
                          from mamba_flat_encounter_ict_general g
                                   join mamba_flat_encounter_index_contact_followup contact
                                        on g.client_id = contact.client_id
                                   left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                             on contact.encounter_id = contact_1.encounter_id
                                   join mamba_dim_encounter encounter on contact.encounter_id = encounter.encounter_id
                                   join mamba_dim_person_attribute attribute on encounter.uuid = attribute.value
                                   join mamba_dim_relationship relationship on attribute.person_id = person_b
                                   join mamba_dim_person person on relationship.person_b = person.person_id)
    select uuid                                           as `GUID`,
           person_name_long                               as `Contact’s Full Name`,
           elicited_date                                  as `Elicited date`,
           contact_category                               as `Contact Category (1-5)`,
           is_ipv_intimate_partner_violence_risk_assessed as `IPV Risk Assessed`,
           ipv_intimate_partner_violence_risk_outcome     as `IPV outcome [1-5]`,
           hiv_status_disclosed_by_index_client           as `HIV status disclosed by Index Client?`,
           notification_plan                              as `Notification plan`,
           trial_date_1st                                 as `1st  Trial Date`,
           ict_serial_number                              as `ICT#`,
           person_name_short                              as `Full Name`
    from contact_list WHERE hiv_test_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE;

END //

DELIMITER ;