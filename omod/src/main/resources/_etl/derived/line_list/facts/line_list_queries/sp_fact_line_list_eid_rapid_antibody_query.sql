DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_eid_rapid_antibody_query;

CREATE PROCEDURE sp_fact_line_list_eid_rapid_antibody_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN


    WITH Enrollment_tmp as (select client_id,
                                   hei_code,
                                   location_of_birth,
                                   ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_enrolled_in_care DESC, encounter_id DESC) as row_num
                            from mamba_flat_encounter_hei_enrollment
                            where date_enrolled_in_care  <= REPORT_END_DATE),
         Enrollment as (select * from Enrollment_tmp where row_num = 1),

         tmp_hei_follow_up as (select f.client_id,
                                      f.followup_date_followup                                                                        as follow_up_date,
                                      arv_prophylaxis,
                                      ROW_NUMBER() over (PARTITION BY f.client_id ORDER BY f.followup_date_followup, f.encounter_id ) as row_num
                               from mamba_flat_encounter_hei_followup f
                                        left join mamba_flat_encounter_hei_followup_1 f1
                                                  on f.encounter_id = f1.encounter_id
                               where f.followup_date_followup <= REPORT_END_DATE),
         latest_hei_follow_up as (select * from tmp_hei_follow_up where row_num = 1),

         AntibodyTests AS (SELECT t.client_id,
                                  t.hiv_test_result                                                                     as rapid_antibody_test_result,
                                  ROW_NUMBER() OVER (PARTITION BY t.client_id ORDER BY t.followup_date_followup_1 DESC) as rn
                           FROM mamba_flat_encounter_hei_hiv_test t
                           WHERE t.followup_date_followup_1 BETWEEN REPORT_START_DATE AND REPORT_END_DATE
                             AND (t.test_type = 'Rapid test for HIV' OR confirmatory_test_done = 'Yes'))

    SELECT c.patient_name                                         as `Full Name`,
           c.sex                                                  as `Sex`,
           c.date_of_birth                                        as `DOB`,
           TIMESTAMPDIFF(MONTH, c.date_of_birth, REPORT_END_DATE) as `Age (in months)`,
           c.mrn                                                  as `MRN`,
           e.hei_code                                             as `HEI Code`,
           f.follow_up_date                                       as `Follow-up Date`,
           t.rapid_antibody_test_result                           as `Rapid Antibody Test Result`

    FROM AntibodyTests t
             JOIN mamba_dim_client c ON t.client_id = c.client_id
             LEFT JOIN Enrollment e ON t.client_id = e.client_id
             LEFT JOIN latest_hei_follow_up f ON t.client_id = f.client_id;

END //

DELIMITER ;
