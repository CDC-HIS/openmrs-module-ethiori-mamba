DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_screening_query;

CREATE PROCEDURE sp_fact_line_list_ict_screening_query(IN REPORT_START_DATE DATE,IN REPORT_END_DATE DATE)
BEGIN
    With offer_list as (select client.client_id,
                               client.mrn,
                               client.uan,
                               client.sex,
                               client.patient_uuid,
                               client.patient_name,
                               offer.offered_date,
                               visit_date_ict_offer,
                               priority_criteria,
                               offered,
                               ict_serial_number,
                               offer.yes,
                               offer.no,
                               elicited,
                               high_risk_for_ipv,
                               accepted,
                               accepted_date,
                               linked_to_appropriate_care,
                               specify_other_adverse_event_type,
                               adverse_event_type,
                               ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                        from mamba_flat_encounter_ict_general ict_general
                                 join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                                 join mamba_dim_client client on ict_general.client_id = client.client_id
                        where offered_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         offer as (select * from offer_list where row_num = 1)
    select
    patient_uuid as `GUID`,
    mrn,
    uan,
    ict_serial_number as `ICT#`,
    patient_name as `Full Name`,
    visit_date_ict_offer as `ICT Visit Date (ICT Screening Date)`,
    priority_criteria as `Prioritization for ICT`,
    offered as `Offered`,
    accepted as `Accepted (Yes)`,
    elicited as  `Elicited`,
    high_risk_for_ipv as `High risk for IPV`,
    adverse_event_type as `Adverse Event Type`,
    linked_to_appropriate_care as 'Linked to appropriate care'
    from offer;

END //

DELIMITER ;