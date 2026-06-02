DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_tb_lb_lf_lam_query_v2;

CREATE PROCEDURE sp_fact_hmis_tb_lb_lf_lam_query_v2(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
WITH FollowUp AS (SELECT encounter_id,
                         client_id,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         treatment_end_date,
                         next_visit_date,
                         regimen,
                         currently_breastfeeding_child          breast_feeding_status,
                         pregnancy_status,
                         was_the_patient_screened_for_tuberc AS tb_screened,
                         tb_screening_date,
                         tb_treatment_status,
                         transferred_in_check_this_for_all_t AS transferred_in,
                         date_started_on_tuberculosis_prophy,
                         screening_test_result_tuberculosis   AS screening_result,
                         lf_lam_result
                  FROM tmp_hmis_follow_up),
     tmp_latest_follow_up as (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     follow_up_status,
                                     tb_screening_date,
                                     art_start_date,
                                     tb_screened,
                                     screening_result,
                                     lf_lam_result,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND tb_screened = 'Yes'
                                AND follow_up_date <= REPORT_END_DATE),
     latest_tb_screened_follow_up as (SELECT follow_up.client_id,
                                             sex,
                                             date_of_birth,
                                             encounter_id,
                                             follow_up_status,
                                             tb_screening_date,
                                             art_start_date,
                                             tb_screened,
                                             screening_result,
                                             lf_lam_result
                                      from tmp_latest_follow_up follow_up
                                               inner join mamba_dim_client client on follow_up.client_id = client.client_id
                                      where row_num = 1),
     lf_lam as (select *
                from latest_tb_screened_follow_up
                where tb_screening_date between REPORT_START_DATE AND REPORT_END_DATE
                  and follow_up_status in ('Alive', 'Restart medication') and art_start_date is not null
                  and lf_lam_result is not null),
     lf_lam_agg AS (
         SELECT COUNT(*)                                                                    AS total,
                SUM(CASE WHEN lf_lam_result = 'Positive'       THEN 1 ELSE 0 END)          AS positive,
                SUM(CASE WHEN lf_lam_result = 'Negative result' THEN 1 ELSE 0 END)         AS negative
         FROM lf_lam
     )

SELECT 'TB_LB_LF-LAM'                                                                              AS S_NO,
       'Total Number of tests performed using Lateral Flow Urine Lipoarabinomannan (LF-LAM) assay' as Activity,
       total                                                                                        as Value
FROM lf_lam_agg
UNION ALL SELECT 'TB_LB_LF-LAM. 1', 'Positive', COALESCE(positive,0) FROM lf_lam_agg
UNION ALL SELECT 'TB_LB_LF-LAM. 2', 'Negative', COALESCE(negative,0) FROM lf_lam_agg;
END //

DELIMITER ;
