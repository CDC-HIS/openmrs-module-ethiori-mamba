DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ict_screening_query;

CREATE PROCEDURE sp_fact_line_list_ict_screening_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
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
                               reason_ict_service_not_accepted,
                               adverse_event_type,
                               ROW_NUMBER() over (PARTITION BY client.client_id ORDER BY offered_date DESC ) as row_num
                        from mamba_flat_encounter_ict_general ict_general
                                 join mamba_flat_encounter_ict_offer offer on ict_general.client_id = offer.client_id
                                 join mamba_dim_client client on ict_general.client_id = client.client_id
                        where offered_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE),
         offer as (select * from offer_list where row_num = 1),
         FollowUp as (SELECT follow_up.encounter_id,
                             follow_up.client_id                                                                                                      AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_                                                                                                 AS follow_up_date,
                             art_antiretroviral_start_date                                                                                            AS art_start_date,
                             treatment_end_date,
                             regimen,
                             antiretroviral_art_dispensed_dose_i                                                                                      AS ARTDoseDays,
                             anitiretroviral_adherence_level                                                                                          AS AdherenceLevel,
                             pregnancy_status,
                             next_visit_date,
                             date_of_reported_hiv_viral_load                                                                                          AS viral_load_sent_date,
                             date_viral_load_results_received                                                                                         AS viral_load_received_date,
                             viral_load_test_status                                                                                                   AS viral_load_result,
                             hiv_viral_load                                                                                                           as viral_load_count,
                             cd4_count,
                             cd4_                                                                                                                     as cd4_percent,
                             current_functional_status,
                             transferred_in_check_this_for_all_t,
                             visitect_cd4_result,
                             visitect_cd4_test_date,
                             date_of_event,
                             ROW_NUMBER() OVER (PARTITION BY follow_up.client_id ORDER BY follow_up_date_followup_ DESC, follow_up.encounter_id DESC) as rn
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
                      WHERE follow_up_date_followup_ <= REPORT_END_DATE),
         LatestFollowUp AS (SELECT *
                            FROM FollowUp
                            where rn = 1)
    select patient_uuid                    as `GUID`,
           mrn                             as `MRN`,
           uan                             as `UAN`,
           ict_serial_number               as `ICT#`,
           patient_name                    as `Full Name`,
           date_of_event                   as `Date Confirmed HIV+ve`,
           art_start_date                  as `ART Start Date`,
           follow_up_date                  as `Last ART Follow-Up Date`,
           visit_date_ict_offer            as `ICT Visit Date (ICT Screening Date)`,
           priority_criteria               as `Prioritization for ICT`,
           offered                         as `Offered`,
           offered_date as `Offered Date`,
           accepted                        as `Accepted (Yes)`,
           accepted_date as `Accepted Date`,
           reason_ict_service_not_accepted as `Accepted (No): Reason`,
           elicited                        as `Elicited`
    from offer
             left join LatestFollowUp on offer.client_id = LatestFollowUp.PatientId;

END //

DELIMITER ;