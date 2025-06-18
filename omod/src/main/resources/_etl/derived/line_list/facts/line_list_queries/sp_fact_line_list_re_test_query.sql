DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_re_test_query;

CREATE PROCEDURE sp_fact_line_list_re_test_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    WITH ReTest as (select re_test.encounter_id,
                           re_test.client_id,
                           re_test.date_enrolled_in_care,
                           re_test.date_of_initial_test,
                           re_test.expiration_date,
                           re_test.test_1_expire_date,
                           re_test.test_2_expire_date,
                           re_test.test_3_expire_date,
                           re_test_1.date_of_retesting_for_verification,
                           final_result,
                           action_taken_after_final_test,
                           entry_point,
                           antiretrovirals_started,
                           test_1_result,
                           test_2_result,
                           test_3_result
                    from mamba_flat_encounter_re_test as re_test
                             left join mamba_flat_encounter_re_test_1 as re_test_1
                                       on re_test.encounter_id = re_test_1.encounter_id),
         tmp_re_test as (select *,
                                ROW_NUMBER() over (PARTITION BY client_id ORDER BY date_of_retesting_for_verification DESC , encounter_id DESC) as row_num
                         from ReTest
                         WHERE (date_of_retesting_for_verification BETWEEN REPORT_START_DATE AND REPORT_END_DATE)
                            OR (REPORT_START_DATE IS NULL AND REPORT_END_DATE IS NULL AND
                                date_of_retesting_for_verification <= CURDATE())),
         re_test as (select * from tmp_re_test where row_num = 1)
    select client.patient_name                                                      as `Patient Name`,
           client.patient_uuid                                                      as `UUID`,
           CAST(client.mrn AS CHAR(20)) as mrn,
           uan,
           TIMESTAMPDIFF(YEAR, date_of_birth, COALESCE(REPORT_END_DATE, CURDATE())) as Age,
           sex,
           date_of_initial_test                                                     as `Date initial tested`,
           date_of_retesting_for_verification                                       as `Date retested`,
           final_result                                                             as `Final Result`,
           action_taken_after_final_test                                            as `Action taken`,
           entry_point                                                              as `Entry point`,
           COALESCE(test_3_result, test_2_result, test_1_result)                       `Previous result`,
           date_enrolled_in_care                                                    as `Date Started ART`
    from re_test
             join mamba_dim_client client on re_test.client_id = client.client_id;


END //

DELIMITER ;
