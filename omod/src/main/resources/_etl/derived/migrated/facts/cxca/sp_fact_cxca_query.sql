DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_cxca_query;

CREATE PROCEDURE  sp_fact_cxca_query()
BEGIN

-- SMARTCARE                       -    OPENMRS                                                                  MAMBA
-- hpv_dna_result_received_date    -    999,HPV DNA result received date                                 -  hpv_dna_result_received_date             - 510f2a47-3761-4903-b7eb-8ea389cecfe9
-- ccs_hpv_result                  -    2110,HPV DNA screening result                                    -  hpv_dna_screening_result                 - 8ecc6d15-26dd-4840-8667-a517a93bea5f
-- date_via_result                 -    1862,Date Visual Inspection of the Cervix with Acetic Acid       -  date_visual_inspection_of_the_cervi      - f46c7ed3-65c3-451c-a8e1-4c615f795db1
-- ccs_via_result                  -    922,VIA screening result                                         -  via_screening_result                     - ff6b60e4-7310-4ddc-98ce-a2910c32a7a0
-- cytology_result_received_date   -    993,Date cytology result received                                -  date_cytology_result_received            - f0892f21-406c-446b-abd5-bb62f3ea2387
-- cytology_result                 -    440,Cytology result * No Data                                    -  cytology_result                          - d0e78bd5-3e85-4651-a369-3e19870f0ea0
-- ccs_treat_received_date         -    1348, treatment_start_date                                       -  treatment_start_date                     - 163526AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
-- colposcopy_exam_finding         -    1858,Colposcopy of cervix findings                               -  colposcopy_of_cervix_findings            - 93bf2a3e-1675-44c9-b7ee-b8ba9cb32b22
-- ccs_next_date                   -    964,Next follow up screening date                                -  next_follow_up_screening_date            - 4ce065b6-aecb-46a3-b60b-41bc5dc8022f
-- ccs_screendoneyes               -    1871,Cervical cancer screening status                            -  cervical_cancer_screening_status         - 01c546b4-e08a-4c0c-82ef-d387cab6bbbf
WITH Follow_up AS (SELECT follow_up_date_followup_                      AS follow_up_date,
                          follow_up.client_id                           AS patient_id,
                          follow_up.encounter_id                        AS encounter_id,
                          follow_up_status,
                          next_follow_up_screening_date                 AS ccs_next_date,
                          cervical_cancer_screening_status              AS screening_status,
                          hpv_dna_result_received_date,
                          hpv_dna_screening_result                      AS ccs_hpv_result,
                          cytology_done_                                AS cytology_result,
                          date_cytology_result_received                 AS cytology_result_received_date,
                          via_screening_result                          AS ccs_via_result,
                          via_done_,
                          date_visual_inspection_of_the_cervi           AS date_via_result,
                          treatment_start_date                          AS ccs_treat_received_date,
                          colposcopy_of_cervix_findings                 AS colposcopy_exam_finding,
                          sex,
                          CEILING(TIMESTAMPDIFF(MONTH , date_of_birth, '2024-01-01')/12) AS age,
                          mrn,
                          uan,
                          patient_name                                  as NAME,
                          ''                                            as phonenumber,
                          mobile_no                                     as mobilephonenumber,
                          art_antiretroviral_start_date                 AS art_start_date,
                          regimen,
                          adherence,
                          next_visit_date
                   FROM mamba_flat_encounter_follow_up follow_up
                            INNER JOIN
                        mamba_flat_encounter_follow_up_1 follow_up_1
                        ON follow_up.encounter_id = follow_up_1.encounter_id
                            LEFT JOIN
                        mamba_dim_client_art_follow_up dim_client
                        ON follow_up.client_id = dim_client.client_id
                   WHERE follow_up_1.follow_up_date_followup_ <= '2024-01-01'
                     AND follow_up_status IS NOT NULL),

     latest_follow_up_status as (SELECT patient_id,
                                        Follow_up.follow_up_status,
                                        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY follow_up_date DESC, encounter_id DESC) AS rn
                                 from Follow_up),
     cervical_screening_performed AS (SELECT MAX(follow_up_date)                                                                                             AS max_follow_up_date,
                                             Follow_up.patient_id                                                                                            as max_patient_id,
                                             Follow_up.encounter_id                                                                                          as max_encounter_id,
                                             ROW_NUMBER() OVER (PARTITION BY Follow_up.patient_id ORDER BY follow_up_date DESC, Follow_up.encounter_id DESC) AS rn
                                      FROM Follow_up
                                               inner join latest_follow_up_status
                                                          on Follow_up.patient_id = latest_follow_up_status.patient_id
                                      WHERE screening_status = 'Cervical cancer screening performed'
                                        and rn = 1
                                        and latest_follow_up_status.follow_up_status not in ('Dead', 'Transferred out')
                                      group by Follow_up.patient_id, Follow_up.encounter_id, Follow_up.follow_up_date),
     cervical_screening_not_performed AS (SELECT MAX(follow_up_date)                                                                                             AS max_follow_up_date,
                                                 Follow_up.patient_id                                                                                            as max_patient_id,
                                                 Follow_up.encounter_id                                                                                          as max_encounter_id,
                                                 ROW_NUMBER() OVER (PARTITION BY Follow_up.patient_id ORDER BY follow_up_date DESC, Follow_up.encounter_id DESC) AS rn
                                          FROM Follow_up
                                                   inner join latest_follow_up_status
                                                              on Follow_up.patient_id = latest_follow_up_status.patient_id
                                          WHERE Follow_up.patient_id not in
                                                (select max_patient_id from cervical_screening_performed)
                                            and sex = 'FEMALE'
                                            and age > 15
                                            and rn = 1
                                            and latest_follow_up_status.follow_up_status not in
                                                ('Dead', 'Transferred out')
                                          group by Follow_up.patient_id, Follow_up.encounter_id,
                                                   Follow_up.follow_up_date),
     cervical_data as (SELECT NAME,
                              phonenumber,
                              mobilephonenumber,
                              mrn,
                              uan,
                              age,
                              art_start_date,
                              regimen,
                              adherence,
                              follow_up_status,
                              follow_up_date,
                              ConvertGregorianToEthiopian(ccs_next_date,'D/M/Y') as appointmentdate,
                              Follow_up.next_visit_date,
                              ConvertGregorianToEthiopian(Follow_up.next_visit_date, 'D/M/Y') as next_visit_date_et,
                              CASE
                                  WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, '2024-01-01') > 1095
                                      AND ccs_hpv_result = 'Negative result'
                                      THEN 'HPV Screened Negative-Need Re-screening'
                                  WHEN TIMESTAMPDIFF(DAY, date_via_result, '2024-01-01') > 730
                                      AND ccs_via_result = 'VIA negative' THEN 'VIA Screened Negative-Need Re-screening'
                                  WHEN TIMESTAMPDIFF(DAY, cytology_result_received_date, '2024-01-01') > 1095
                                      AND cytology_result = 0 THEN 'Cytology Screened Negative-Need Re-screening'
                                  WHEN ccs_treat_received_date = '1900-01-01 00:00:00'
                                      AND (colposcopy_exam_finding IN ('Low Grade', 'High Grade')
                                          OR cytology_result = 2
                                          OR ccs_via_result IN ('VIA positive: eligible for cryo/thermo-coagula',
                                                                'VIA positive: non-eligible for cryo/thermo-coagula'))
                                      THEN 'Positive Result with No Treatment Date'
                                  WHEN TIMESTAMPDIFF(DAY, ccs_treat_received_date, '2024-01-01') > 181
                                      AND ccs_treat_received_date > '1900-01-01 00:00:00'
                                      THEN 'Treated with Cryotherapy/Thermocoagulation/LEEP'
                                  WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, '2024-01-01') > 365
                                      AND ccs_hpv_result = 'Positive'
                                      AND ccs_via_result = 'VIA negative'
                                      THEN 'HPV Positive but VIA Negative- Need Re-screening'
                                  WHEN ccs_next_date <= '2024-01-01'
                                      AND ccs_next_date != '1900-01-01' THEN 'Other'
                                  END                                               AS treatmentreceived
                       FROM cervical_screening_performed
                                INNER JOIN
                            Follow_up ON Follow_up.encounter_id = cervical_screening_performed.max_encounter_id
                       WHERE cervical_screening_performed.rn = 1
                       UNION
                       select NAME,
                              phonenumber,
                              mobilephonenumber,
                              mrn,
                              uan,
                              age,
                              art_start_date,
                              regimen,
                              adherence,
                              follow_up_status,
                              follow_up_date,
                              ConvertGregorianToEthiopian(ccs_next_date,'D/M/Y') as appointmentdate,
                              Follow_up.next_visit_date,
                              ConvertGregorianToEthiopian(next_visit_date, 'D/M/Y') as next_visit_date_et,

                              'Never screened for CxCa'                             as treatmentreceived
                       from cervical_screening_not_performed
                                INNER JOIN Follow_up
                                           ON Follow_up.encounter_id = cervical_screening_not_performed.max_encounter_id
                       WHERE cervical_screening_not_performed.rn = 1)
select *
from cervical_data
where treatmentreceived is not null;

END //

DELIMITER ;