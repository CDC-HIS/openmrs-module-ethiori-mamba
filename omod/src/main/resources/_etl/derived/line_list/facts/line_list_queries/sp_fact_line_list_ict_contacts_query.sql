DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_screening_query;

CREATE PROCEDURE sp_fact_line_list_ict_screening_query(IN REPORT_START_DATE DATE,IN REPORT_END_DATE DATE)
BEGIN
    WITH contact_list as (select contact.client_id,
                            contact.elicited_date,
                            contact.hiv_test_date,
                            contact.hiv_test_result,
                            is_ipv_intimate_partner_violence_risk_assessed,
                            prior_hiv_test_result,
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
                            patient_name,
                            client.mrn,
                            client.uan,
                            client.sex,
                            client.patient_uuid
                     from
                         mamba_flat_encounter_index_contact_followup contact
                             left join mamba_flat_encounter_index_contact_followup_1 contact_1
                                       on contact.encounter_id = contact_1.encounter_id
                             join mamba_dim_client client on contact.client_id = client.client_id)
    select
    patient_uuid as `GUID`,
    patient_name as `Contact’s Full Name`,
    elicited_date as `Elicited date`,
    contact_category as `Contact Category (1-5)`,
        is_ipv_intimate_partner_violence_risk_assessed as `IPV Risk Assessed`,
        ipv_intimate_partner_violence_risk_outcome as `IPV outcome [1-5]`,
    hiv_status_disclosed_by_index_client as `HIV status disclosed by Index Client?`,
    notification_plan as `Notification plan`,
    trial_date_1st as `1st  Trial Date`,
    trei


    mrn,
    uan,
    patient_name as `ICT#`,
    patient_name as `Full Name`,
    visit_date_ict_offer as `ICT Visit Date (ICT Screening Date)`,
    priority_criteria as `Prioritization for ICT`,
    offered as `Offered`,
    accepted as `Accepted (Yes)`,
    elicited as  `Elicited`,
    high_risk_for_ipv as `High risk for IPV`,
    adverse_event_type as `Adverse Event Type`,
    linked_to_appropriate_care as 'Linked to appropriate care'
    from contact_list;

END //

DELIMITER ;