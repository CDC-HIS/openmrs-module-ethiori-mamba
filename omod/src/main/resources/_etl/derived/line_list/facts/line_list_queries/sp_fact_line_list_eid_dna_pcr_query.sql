DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_eid_dna_pcr_query;

CREATE PROCEDURE sp_fact_line_list_eid_dna_pcr_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN


    WITH Enrollment AS (SELECT e.client_id,
                               e.hei_code,
                               e.arv_prophylaxis as enrollment_arv_prophylaxis,
                               e.mother_status
                        FROM mamba_flat_encounter_hei_enrollment e
                        WHERE e.date_enrolled_in_care <= REPORT_END_DATE),

         LatestFollowUp AS (SELECT f.client_id,
                                   f.followup_date_followup                                                            as follow_up_date,
                                   COALESCE(f1.infant_feeding_practice_within_the_first_6_months_of_life,
                                            f1.infant_feeding_practice_older_than_6_months_of_life)                    as feeding_practice,
                                   ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.followup_date_followup DESC) as rn
                            FROM mamba_flat_encounter_hei_followup f
                                     left join mamba_flat_encounter_hei_followup_1 f1
                                               on f.encounter_id = f1.encounter_id
                            WHERE f.followup_date_followup <= REPORT_END_DATE),

         DNAPCRTests as (select hiv_test.client_id,
                                hiv_test.maternal_art_status,
                                hiv_test.initial_test,
                                hiv_test.specimen_type,
                                hiv_test.dna_pcr_sample_collection_date,
                                hiv_test.hiv_test_result                                                                                  as dna_pcr_result,
                                hiv_test.date_dbs_result_received,
                                DATEDIFF(hiv_test.date_dbs_result_received,
                                         hiv_test.dna_pcr_sample_collection_date)                                                         as tat,
                                hiv_test.specimen_received_date,
                                hiv_test.laboratory_name,
                                hiv_test.sample_quality,
                                hiv_test.reason_sample_rejected_or_test_not_done,
                                hiv_test.hiv_test_date                                                                                    as date_test_performed,
                                hiv_test.platform_used,
                                hiv_test.telephone_number_of_referringordering_clinician                                                  as mothers_phone_no,
                                ROW_NUMBER() OVER (PARTITION BY hiv_test.client_id ORDER BY hiv_test.dna_pcr_sample_collection_date DESC) as rn
                         from mamba_flat_encounter_hei_hiv_test hiv_test
                                  join mamba_dim_client client on hiv_test.client_id = client.client_id
                         where dna_pcr_sample_collection_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                         -- test_type = 'HIV DNA polymerase chain reaction, dried blood spot (DBS)'
                         ),
         hei_test as (select * from DNAPCRTests where rn = 1)


    SELECT c.patient_name                                         as `Full Name`,
           c.sex                                                  as `Sex`,
           c.date_of_birth                                        as `DOB`,
           c.date_of_birth                                        as `DOB EC.`,
           TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (in months)`,
           c.mrn                                                  as `MRN`,
           e.hei_code                                             as `HEI Code`,
           f.follow_up_date                                       as `Follow-up Date`,
           f.follow_up_date                                       as `Follow-up Date EC.`,
           f.feeding_practice                                     as `Infant on BF?`,
           e.enrollment_arv_prophylaxis                           as `ARV Prophylaxis`,
           t.maternal_art_status                                  as `Maternal ART Status`,
           t.initial_test                                         as `Test Indication`,
           t.specimen_type                                        as `Specimen Type`,
           t.dna_pcr_sample_collection_date                       as `Date of Sample Collection`,
           t.dna_pcr_sample_collection_date                       as `Date of Sample Collection EC.`,
           t.dna_pcr_result                                       as `DNA PCR Result`,
           t.date_dbs_result_received                             as `Date of Result Received by H.F`,
           t.date_dbs_result_received                             as `Date of Result Received by H.F EC.`,
           t.tat                                                  as `TAT (in days)`,
           t.specimen_received_date                               as `Date of DBS referral to regional lab`,
           t.specimen_received_date                               as `Date of DBS referral to regional lab EC.`,
           t.laboratory_name                                      as `Name of Testing Lab`,
           t.specimen_received_date                               as `Date of sample received by Lab`,
           t.specimen_received_date                               as `Date of sample received by Lab EC.`,
           t.sample_quality                                       as `Sample Quality`,
           t.reason_sample_rejected_or_test_not_done              as `Reason for Sample rejection/test not done`,
           t.date_test_performed                                  as `Date test performed by Lab`,
           t.date_test_performed                                  as `Date test performed by Lab EC.`,
           t.platform_used                                        as `Platform used`,
           t.mothers_phone_no                                     as `Mother's Phone No.`

    FROM hei_test t
             JOIN mamba_dim_client c ON t.client_id = c.client_id
             LEFT JOIN Enrollment e ON t.client_id = e.client_id
             LEFT JOIN LatestFollowUp f ON t.client_id = f.client_id AND f.rn = 1;

END //

DELIMITER ;
