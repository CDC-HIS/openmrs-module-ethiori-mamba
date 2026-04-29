DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_hmis_tx_curr_query_v2;

CREATE PROCEDURE sp_fact_hmis_tx_curr_query_v2(
    IN REPORT_END_DATE DATE
)
BEGIN

    WITH FollowUp AS (SELECT encounter_id,
                             client_id                     AS PatientId,
                             follow_up_status,
                             follow_up_date_followup_      AS follow_up_date,
                             art_antiretroviral_start_date AS art_start_date,
                             treatment_end_date,
                             next_visit_date,
                             regimen,
                             currently_breastfeeding_child    breast_feeding_status,
                             pregnancy_status
                      FROM tmp_hmis_follow_up),
         -- TX curr
         tx_curr_all AS (SELECT PatientId,
                                follow_up_date                                                                             AS FollowupDate,
                                encounter_id,
                                follow_up_status,
                                treatment_end_date,
                                ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_END_DATE),

         tx_curr AS (select *
                     from tx_curr_all
                     where row_num = 1
                       AND follow_up_status in ('Alive', 'Restart medication')
                       AND treatment_end_date >= REPORT_END_DATE),
         tx_curr_with_client as (select tx_curr.*,
                                        client.sex,
                                        client.date_of_birth,
                                        pregnancy_status,
                                        regimen,
                                        left(regimen, 1)                                                   as regimen_line,
                                        TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE)         as age
                                 from FollowUp
                                          inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
                                          left join mamba_dim_client client on tx_curr.PatientId = client.client_id),
         tx_curr_agg AS (
             SELECT
                 COUNT(*) AS Value,
                 SUM(CASE WHEN age < 15  THEN 1 ELSE 0 END) AS u15,
                 SUM(CASE WHEN age >= 15  THEN 1 ELSE 0 END) AS greater_than_15,
                 SUM(CASE WHEN age < 1  THEN 1 ELSE 0 END) AS u1,
                 SUM(CASE WHEN age <= 1  THEN 1 ELSE 0 END) AS under_19,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u1_first_line,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u1_first_line_male,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u1_first_line_female,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u1_second_line,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u1_second_line_male,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u1_second_line_female,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u1_third_line,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u1_third_line_male,
                 SUM(CASE WHEN age < 1  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u1_third_line_female,

                 SUM(CASE WHEN age BETWEEN 1 AND 4  THEN 1 ELSE 0 END) AS u4,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u4_first_line,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u4_first_line_male,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u4_first_line_female,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u4_second_line,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u4_second_line_male,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u4_second_line_female,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u4_third_line,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u4_third_line_male,
                 SUM(CASE WHEN age BETWEEN 1 AND 4  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u4_third_line_female,

                 SUM(CASE WHEN age BETWEEN 5 AND 9  THEN 1 ELSE 0 END) AS u9,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u9_first_line,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u9_first_line_male,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u9_first_line_female,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u9_second_line,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u9_second_line_male,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u9_second_line_female,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u9_third_line,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u9_third_line_male,
                 SUM(CASE WHEN age BETWEEN 5 AND 9  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u9_third_line_female,

                 SUM(CASE WHEN age BETWEEN 10 AND 14  THEN 1 ELSE 0 END) AS u14,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u14_first_line,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u14_first_line_male,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u14_first_line_female,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u14_second_line,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u14_second_line_male,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u14_second_line_female,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u14_third_line,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u14_third_line_male,
                 SUM(CASE WHEN age BETWEEN 10 AND 14  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u14_third_line_female,

                 SUM(CASE WHEN age BETWEEN 15 AND 19  THEN 1 ELSE 0 END) AS u19,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u19_first_line,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u19_first_line_male,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u19_first_line_female,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u19_second_line,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u19_second_line_male,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u19_second_line_female,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u19_third_line,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u19_third_line_male,
                 SUM(CASE WHEN age BETWEEN 15 AND 19  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u19_third_line_female,

                 SUM(CASE WHEN age BETWEEN 20 AND 24  THEN 1 ELSE 0 END) AS u24,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u24_first_line,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u24_first_line_male,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u24_first_line_female,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u24_second_line,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u24_second_line_male,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u24_second_line_female,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u24_third_line,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u24_third_line_male,
                 SUM(CASE WHEN age BETWEEN 20 AND 24  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u24_third_line_female,

                 SUM(CASE WHEN age BETWEEN 25 AND 29  THEN 1 ELSE 0 END) AS u29,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u29_first_line,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u29_first_line_male,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u29_first_line_female,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u29_second_line,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u29_second_line_male,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u29_second_line_female,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u29_third_line,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u29_third_line_male,
                 SUM(CASE WHEN age BETWEEN 25 AND 29  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u29_third_line_female,

                 SUM(CASE WHEN age BETWEEN 30 AND 34  THEN 1 ELSE 0 END) AS u34,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u34_first_line,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u34_first_line_male,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u34_first_line_female,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u34_second_line,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u34_second_line_male,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u34_second_line_female,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u34_third_line,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u34_third_line_male,
                 SUM(CASE WHEN age BETWEEN 30 AND 34  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u34_third_line_female,

                 SUM(CASE WHEN age BETWEEN 35 AND 39  THEN 1 ELSE 0 END) AS u39,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u39_first_line,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u39_first_line_male,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u39_first_line_female,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u39_second_line,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u39_second_line_male,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u39_second_line_female,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u39_third_line,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u39_third_line_male,
                 SUM(CASE WHEN age BETWEEN 35 AND 39  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u39_third_line_female,

                 SUM(CASE WHEN age BETWEEN 40 AND 44  THEN 1 ELSE 0 END) AS u44,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u44_first_line,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u44_first_line_male,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u44_first_line_female,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u44_second_line,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u44_second_line_male,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u44_second_line_female,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u44_third_line,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u44_third_line_male,
                 SUM(CASE WHEN age BETWEEN 40 AND 44  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u44_third_line_female,

                 SUM(CASE WHEN age BETWEEN 45 AND 49  THEN 1 ELSE 0 END) AS u49,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u49_first_line,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS u49_first_line_male,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS u49_first_line_female,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u49_second_line,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS u49_second_line_male,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS u49_second_line_female,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u49_third_line,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS u49_third_line_male,
                 SUM(CASE WHEN age BETWEEN 45 AND 49  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS u49_third_line_female,

                 SUM(CASE WHEN age >= 50  THEN 1 ELSE 0 END) AS greater_than_50,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS greater_than_50_first_line,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '1' or regimen_line = '4') AND sex='Male' THEN 1 ELSE 0 END) AS greater_than_50_first_line_male,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '1' or regimen_line = '4') AND sex='Female' THEN 1 ELSE 0 END) AS greater_than_50_first_line_female,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS greater_than_50_second_line,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '2' or regimen_line = '5') AND sex='Male' THEN 1 ELSE 0 END) AS greater_than_50_second_line_male,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '2' or regimen_line = '5') AND sex='Female' THEN 1 ELSE 0 END) AS greater_than_50_second_line_female,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS greater_than_50_third_line,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '3' or regimen_line = '6') AND sex='Male' THEN 1 ELSE 0 END) AS greater_than_50_third_line_male,
                 SUM(CASE WHEN age >= 50  AND (regimen_line = '6' or regimen_line = '6') AND sex='Female' THEN 1 ELSE 0 END) AS greater_than_50_third_line_female,

                 SUM(CASE WHEN sex='Female' THEN 1 ELSE 0 END) AS female_total,
                 SUM(CASE WHEN sex='Female' AND pregnancy_status = 'Yes' THEN 1 ELSE 0 END) AS female_pregnant,
                 SUM(CASE WHEN sex='Female' AND (pregnancy_status = 'No' or pregnancy_status is null) THEN 1 ELSE 0 END) AS female_non_pregnant,

                 SUM(CASE WHEN age < 1  AND regimen = '4f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u1_first_line_4f,
                 SUM(CASE WHEN age < 1  AND regimen = '4g - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u1_first_line_4g,
                 SUM(CASE WHEN age < 1  AND regimen = '4j - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u1_first_line_4j,
                 SUM(CASE WHEN age < 1  AND regimen = '4K - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u1_first_line_4k,
                 SUM(CASE WHEN age < 1  AND regimen not in ('4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG') AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u1_first_line_other,
                 SUM(CASE WHEN age < 1  AND regimen = '5e - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u1_second_line_5e,
                 SUM(CASE WHEN age < 1  AND regimen = '5f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u1_second_line_5f,
                 SUM(CASE WHEN age < 1  AND regimen = '5m - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u1_second_line_5m,
                 SUM(CASE WHEN age < 1  AND regimen = '5n - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u1_second_line_5n,
                 SUM(CASE WHEN age < 1  AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5j - ABC+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG') AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u1_second_line_other,
                 SUM(CASE WHEN age < 1  AND regimen = '6c - DRVr+DTG+AZT+3TC' THEN 1 ELSE 0 END) AS u1_third_line_6c,
                 SUM(CASE WHEN age < 1  AND regimen = '6f - DRV/r+DTG+ABC+3TC' THEN 1 ELSE 0 END) AS u1_third_line_6f,
                 SUM(CASE WHEN age < 1  AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6f - DRV/r+DTG+ABC+3TC') AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u1_third_line_other,

                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4d - AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u4_first_line_4d,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u4_first_line_4f,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4g - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u4_first_line_4g,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4j - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u4_first_line_4j,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4K - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u4_first_line_4k,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '4L - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u4_first_line_4l,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen not in ('4d - AZT+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr','4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV') AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u4_first_line_other,

                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '5e - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u4_second_line_5e,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '5f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u4_second_line_5f,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '5h - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u4_second_line_5h,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '5m - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u4_second_line_5m,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '5n - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u4_second_line_5n,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5h - ABC+3TC+EFV', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG') AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u4_second_line_other,

                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '6c - DRVr+DTG+AZT+3TC' THEN 1 ELSE 0 END) AS u4_third_line_6c,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '6f - DRV/r+DTG+ABC+3TC' THEN 1 ELSE 0 END) AS u4_third_line_6f,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '6g - DRV/r+ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u4_third_line_6g,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen = '6h - DRV/r+AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u4_third_line_6h,
                 SUM(CASE WHEN age BETWEEN 1 AND 4 AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6f - DRV/r+DTG+ABC+3TC', '6g - DRV/r+ABC+3TC+EFV', '6h - DRV/r+AZT+3TC+EFV')  AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u4_third_line_other,

                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4d - AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u9_first_line_4d,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4e - TDF+3TC+EFV' THEN 1 ELSE 0 END) AS u9_first_line_4e,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u9_first_line_4f,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4g - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u9_first_line_4g,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4i - TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u9_first_line_4i,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4j - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u9_first_line_4j,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4K - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u9_first_line_4k,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '4L - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u9_first_line_4l,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen not in ('4d - AZT+3TC+EFV', '4e - TDF+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr','4i - TDF+3TC+DTG', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV')  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u9_first_line_other,

                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5e - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u9_second_line_5e,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u9_second_line_5f,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5g - TDF+3TC+EFV' THEN 1 ELSE 0 END) AS u9_second_line_5g,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5h - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u9_second_line_5h,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5i - TDF+3TC+LPVr' THEN 1 ELSE 0 END) AS u9_second_line_5i,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5m - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u9_second_line_5m,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5n - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u9_second_line_5n,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '5o - TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u9_second_line_5o,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5g - TDF+3TC+EFV', '5h - ABC+3TC+EFV','5i - TDF+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG', '5o - TDF+3TC+DTG') AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u9_second_line_other,

                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '6c - DRVr+DTG+AZT+3TC' THEN 1 ELSE 0 END) AS u9_third_line_6c,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '6d - DRVr+DTG+TDF+3TC' THEN 1 ELSE 0 END) AS u9_third_line_6d,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '6f - DRV/r+DTG+ABC+3TC' THEN 1 ELSE 0 END) AS u9_third_line_6f,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '6g - DRV/r+ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u9_third_line_6g,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen = '6h - DRV/r+AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u9_third_line_6h,
                 SUM(CASE WHEN age BETWEEN 5 AND 9 AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6d - DRVr+DTG+TDF+3TC', '6f - DRV/r+DTG+ABC+3TC','6g - DRV/r+ABC+3TC+EFV', '6h - DRV/r+AZT+3TC+EFV') AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u9_third_line_other,

                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4d - AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u14_first_line_4d,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4e - TDF+3TC+EFV' THEN 1 ELSE 0 END) AS u14_first_line_4e,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u14_first_line_4f,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4g - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u14_first_line_4g,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4i - TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u14_first_line_4i,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4j - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u14_first_line_4j,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4K - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u14_first_line_4k,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '4L - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u14_first_line_4L,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen not in ('4d - AZT+3TC+EFV', '4e - TDF+3TC+EFV', '4f - AZT+3TC+LPVr', '4g - ABC+3TC+LPVr','4i - TDF+3TC+DTG', '4j - ABC+3TC+DTG', '4K - AZT+3TC+DTG', '4L - ABC+3TC+EFV')  AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u14_first_line_other,

                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5e - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u14_second_line_5e,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5f - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u14_second_line_5f,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5g - TDF+3TC+EFV' THEN 1 ELSE 0 END) AS u14_second_line_5g,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5h - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u14_second_line_5h,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5i - TDF+3TC+LPVr' THEN 1 ELSE 0 END) AS u14_second_line_5i,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5m - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u14_second_line_5m,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5n - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u14_second_line_5n,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '5o - TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u14_second_line_5o,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen not in ('5e - ABC+3TC+LPVr', '5f - AZT+3TC+LPVr', '5g - TDF+3TC+EFV', '5h - ABC+3TC+EFV','5i - TDF+3TC+LPVr', '5m - ABC+3TC+DTG', '5n - AZT+3TC+DTG', '5o - TDF+3TC+DTG') AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u14_second_line_other,

                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '6c - DRVr+DTG+AZT+3TC' THEN 1 ELSE 0 END) AS u14_third_line_6c,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '6d - DRVr+DTG+TDF+3TC' THEN 1 ELSE 0 END) AS u14_third_line_6d,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '6f - DRV/r+DTG+ABC+3TC' THEN 1 ELSE 0 END) AS u14_third_line_6f,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '6g - DRV/r+ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u14_third_line_6g,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen = '6h - DRV/r+AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u14_third_line_6h,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen not in ('6c - DRVr+DTG+AZT+3TC', '6d - DRVr+DTG+TDF+3TC', '6f - DRV/r+DTG+ABC+3TC','6g - DRV/r+ABC+3TC+EFV', '6h - DRV/r+AZT+3TC+EFV') AND (regimen_line = '3' or regimen_line = '6') THEN 1 ELSE 0 END) AS u14_third_line_other,

                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '1d - AZT+3TC+EFV' THEN 1 ELSE 0 END) AS u19_first_line_1d,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '1e - TDF+3TC+EFV' THEN 1 ELSE 0 END) AS u19_first_line_1e,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '1g - ABC+3TC+EFV' THEN 1 ELSE 0 END) AS u19_first_line_1g,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '1j - TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u19_first_line_1j,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '1k - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u19_first_line_1k,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '4j - ABC+3TC+DTG' THEN 1 ELSE 0 END) AS u19_first_line_4d,
                 SUM(CASE WHEN age BETWEEN 10 AND 14 AND regimen not in ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG') AND (regimen_line = '1' or regimen_line = '4') THEN 1 ELSE 0 END) AS u19_first_line_other,

                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2e - AZT+3TC+LPVr' THEN 1 ELSE 0 END) AS u19_second_line_2e,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2f - AZT+3TC+ATVr' THEN 1 ELSE 0 END) AS u19_second_line_2f,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2g - TDF+3TC+LPVr' THEN 1 ELSE 0 END) AS u19_second_line_2g,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2h - TDF+3TC+ATVr' THEN 1 ELSE 0 END) AS u19_second_line_2h,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2i - ABC+3TC+LPVr' THEN 1 ELSE 0 END) AS u19_second_line_2i,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2j -TDF+3TC+DTG' THEN 1 ELSE 0 END) AS u19_second_line_2j,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen = '2k - AZT+3TC+DTG' THEN 1 ELSE 0 END) AS u19_second_line_2k,
                 SUM(CASE WHEN age BETWEEN 15 AND 19 AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr','2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG') AND (regimen_line = '2' or regimen_line = '5') THEN 1 ELSE 0 END) AS u19_second_line_other,

             FROM  tx_curr_with_client
         )
-- Does health facility provide Monthly PMTCT / ART Treatment Service?
    SELECT 'HIV_HIV_Treatement.' AS S_NO, 'Does health facility provide Monthly PMTCT / ART Treatment Service?' as Activity, '' as Value
    UNION ALL SELECT 'HIV_TX_CURR_ALL','Number of adults and children who are currently on ART by age, sex and regimen category',Value FROM tx_curr_agg
-- Number of children (<15) who are currently on ART
    UNION ALL SELECT 'HIV_TX_CURR_U15','Number of children (<15) who are currently on ART',u15 FROM tx_curr_agg
-- 1 Children currently on ART aged <1 yr
    UNION ALL SELECT 'HIV_TX_CURR_U1','Children currently on ART aged <1 yr',u1  FROM tx_curr_agg
-- 1.1 Children currently on ART aged <1 yr on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U1_1','Children currently on ART aged <1 yr on First line regimen by sex',u1_first_line FROM tx_curr_agg
-- 1.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U1_1. 1','Male' ,u1_first_line_male   FROM tx_curr_agg
-- 1.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U1_1. 2','Female',u1_first_line_female    FROM tx_curr_agg
-- 1.2 Children currently on ART aged <1 yr on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U1_2' ,'Children currently on ART aged <1 yr on Second line regimen by sex',u1_second_line  FROM tx_curr_agg
-- 1.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U1_2. 1','Male', u1_second_line_male FROM tx_curr_agg
-- 1.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U1_2. 2','Female',  u1_second_line_female   FROM tx_curr_agg
-- 1.3 Children currently on ART aged <1 yr on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U1_3','Children currently on ART aged <1 yr on Third line regimen by sex' ,u1_third_line      FROM tx_curr_agg
-- 1.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U1_3. 1','Male',u1_third_line_male    FROM tx_curr_agg
-- 1.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U1_3. 2','Female',u1_third_line_female    FROM tx_curr_agg
-- 5 Children currently on ART aged 1-4 yr
    UNION ALL SELECT 'HIV_TX_CURR_U5','Children currently on ART aged 1-4 yr',u4    FROM tx_curr_agg
-- 5.1 Children currently on ART aged 1-4 yr on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U5.1','Children currently on ART aged 1-4 yr on First line regimen by sex' ,u4_first_line    FROM tx_curr_agg
-- 5.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U5.1. 1','Male',u4_first_line_male    FROM tx_curr_agg
-- 5.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U5.1. 2','Female',u4_first_line_male    FROM tx_curr_agg
-- 5.2 Children currently on ART aged 1-4 yr on Second-line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U5.2','Children currently on ART aged 1-4 yr on Second-line regimen by sex',u4_second_line   FROM tx_curr_agg
-- 5.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U5.2. 1','Male',u4_second_line_male    FROM tx_curr_agg
-- 5.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U5.2. 2','Female',u4_second_line_female    FROM tx_curr_agg
-- 5.3 Children currently on ART aged 1-4 yr on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U5.3','Children currently on ART aged 1-4 yr on Third line regimen by sex',u4_third_line    FROM tx_curr_agg
-- 5.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U5.3. 1','Male',u4_third_line_male  FROM tx_curr_agg
-- Female 5.3.2
    UNION ALL SELECT 'HIV_TX_CURR_U5.3. 2','Female',u4_third_line_female  FROM tx_curr_agg
-- 9 Children currently on ART aged 5-9 yr
    UNION ALL SELECT 'HIV_TX_CURR_U9','Children currently on ART aged 5-9 yr',u9  FROM tx_curr_agg
-- 9.1 Children currently on ART aged 5-9 yr on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U9.1','Children currently on ART aged 5-9 yr on First line regimen by sex',u9_first_line FROM tx_curr_agg
-- 9.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U9.1. 1','Male',u9_first_line_male FROM tx_curr_agg
-- 9.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U9.1. 2','Female',u9_first_line_female FROM tx_curr_agg
-- 9.2 Children currently on ART aged 5-9 yr on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U9.2','Children currently on ART aged 5-9 yr on Second line regimen by sex',u9_second_line FROM tx_curr_agg
-- 9.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U9.2. 1','Male',u9_second_line_male  FROM tx_curr_agg
-- 9.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U9.2. 2','Female',u9_second_line_female FROM tx_curr_agg
-- 9.3 Children currently on ART aged 5-9 yr on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U9.3','Children currently on ART aged 5-9 yr on Third line regimen by sex',u9_third_line FROM tx_curr_agg
-- 9.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U9.3. 1','Male',u9_third_line_male FROM tx_curr_agg
-- 9.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U9.3. 2','Female',u9_third_line_female FROM tx_curr_agg
-- 14 Children  currently on ART aged 10-14 year
    UNION ALL SELECT 'HIV_TX_CURR_U14','Children currently on ART aged 10-14 year',u14 FROM tx_curr_agg
-- 14.1 Children  currently on ART aged 10-14 year on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U14.1','Children currently on ART aged 10-14 year on First line regimen by sex',u14_first_line FROM tx_curr_agg
-- 14.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U14.1. 1','Male',u14_first_line_male FROM tx_curr_agg
-- 14.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U14.1. 2','Female',u14_first_line_female FROM tx_curr_agg
-- 14.2 Children currently on ART aged 10-14 year on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U14.2','Children currently on ART aged 10-14 year on Second line regimen by sex',u14_second_line FROM tx_curr_agg
-- 14.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U14.2. 1','Male',u14_second_line_male FROM tx_curr_agg
-- 14.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U14.2. 2','Female',u14_second_line_female    FROM tx_curr_agg
-- 14.3 Children currently on ART aged 10-14 years on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U14.3','Children currently on ART aged 10-14 years on Third line regimen by sex',u14_third_line FROM tx_curr_agg
-- 14.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U14.3. 1','Male',u14_third_line_male FROM tx_curr_agg
-- 14.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U14.3. 2','Female',u14_third_line_female FROM tx_curr_agg
-- Number of adults who are currently on ART (>=15)
    UNION ALL SELECT 'HIV_TX_CURR_ADULT','Number of adults who are currently on ART (>=15)',greater_than_15 FROM tx_curr_agg
-- 19 Adult currently on ART aged 15-19
    UNION ALL SELECT 'HIV_TX_CURR_U19','Adult currently on ART aged 15-19',u19 FROM tx_curr_agg
-- 19.1 Adult currently on ART aged 15-19 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U19.1','Adult currently on ART aged 15-19 on First line regimen by sex',u19_first_line FROM tx_curr_agg
-- 19.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U19.1. 1','Male',u19_first_line_male FROM tx_curr_agg
-- 19.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U19.1. 2','Female',u19_first_line_female FROM tx_curr_agg
-- 19.2 Adult currently on ART aged 15-19 on Second-line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U19.2','Adult currently on ART aged 15-19 on Second-line regimen by sex',u19_second_line FROM tx_curr_agg
-- 19.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U19.2. 1','Male',u19_second_line_male FROM tx_curr_agg
-- 19.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U19.2. 2','Female',u19_second_line_female FROM tx_curr_agg
-- 19.3 Adult currently on ART aged 15-19 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U19.3','Adult currently on ART aged 15-19 on Third line regimen by sex',u19_third_line  FROM tx_curr_agg
-- 19.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U19.3. 1','Male',u19_third_line_male FROM tx_curr_agg
-- 19.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U19.3. 2','Female',u19_third_line_female FROM tx_curr_agg
-- 24 Adult currently on ART aged 20-24
    UNION ALL SELECT 'HIV_TX_CURR_U24','Adult currently on ART aged 20-24',u24 FROM tx_curr_agg
-- 24.1 Adult currently on ART aged 20-24 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U24.1','Adult currently on ART aged 20-24 on First line regimen by sex',u24_first_line FROM tx_curr_agg
-- 24.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U24.1. 1','Male',u24_first_line_male FROM tx_curr_agg
-- 24.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U24.1. 2','Female',u24_first_line_female FROM tx_curr_agg
-- 24.2 Adult currently on ART aged 20-24 on Second-line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U24.2','Adult currently on ART aged 20-24 on Second-line regimen by sex',u24_second_line FROM tx_curr_agg
-- 24.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U24.2. 1','Male',u24_second_line_male FROM tx_curr_agg
-- 24.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U24.2. 2','Female',u24_second_line_female  FROM tx_curr_agg
-- 24.3 Adult currently on ART aged 20-24 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U24.3','Adult currently on ART aged 20-24 on Third line regimen by sex',u24_third_line FROM tx_curr_agg
-- 24.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U24.3. 1','Male',u24_third_line_male FROM tx_curr_agg
-- 24.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U24.3. 2','Female',u24_third_line_female FROM tx_curr_agg
-- 29 Adult currently on ART aged 25-29
    UNION ALL SELECT 'HIV_TX_CURR_U29','Adult currently on ART aged 25-29',u29 FROM tx_curr_agg
-- 29.1 Adult currently on ART aged 25-29 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U29.1','Adult currently on ART aged 25-29 on First line regimen by sex',u29_first_line FROM tx_curr_agg
-- 29.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U29.1. 1','Male',u29_first_line_male FROM tx_curr_agg
-- 29.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U29.1. 2','Female',u29_first_line_female FROM tx_curr_agg
-- 29.2 Adult currently on ART aged 25-29 on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U29.2','Adult currently on ART aged 25-29 on Second line regimen by sex',u29_second_line FROM tx_curr_agg
-- 29.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U29.2. 1','Male',u29_second_line_male FROM tx_curr_agg
-- 29.2.2 Female
    UNION ALL
    SELECT 'HIV_TX_CURR_U29.2. 2','Female',u29_second_line_female FROM tx_curr_agg
-- 29.3 Adult currently on ART aged 25-29 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U29.3','Adult currently on ART aged 25-29 on Third line regimen by sex',u29_third_line FROM tx_curr_agg
-- 29.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U29.3. 1','Male',u29_third_line_male FROM tx_curr_agg
-- 29.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U29.3. 2','Female',u29_third_line_female FROM tx_curr_agg
-- 34 Adult currently on ART aged 30-34
    UNION ALL SELECT 'HIV_TX_CURR_U34','Adult currently on ART aged 30-34',u34 FROM tx_curr_agg
-- 34.1 Adult currently on ART aged 30-34 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U34.1','Adult currently on ART aged 30-34 on First line regimen by sex',u34_first_line FROM tx_curr_agg
-- 34.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.1. 1','Male',u34_first_line_male FROM tx_curr_agg
-- 34.1.2 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.1. 2','Female',u34_first_line_female FROM tx_curr_agg
-- 34.2 Adult currently on ART aged 30-34 on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U34.2','Adult currently on ART aged 30-34 on Second line regimen by sex',u34_second_line FROM tx_curr_agg
-- 34.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.2. 1','Male',u34_second_line_male FROM tx_curr_agg
-- 34.2.2 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.2. 2','Female',u34_second_line_female  FROM tx_curr_agg
-- 34.3 Adult currently on ART aged 30-34 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U34.3','Adult currently on ART aged 30-34 on Third line regimen by sex',u34_third_line FROM tx_curr_agg
-- 34.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.3. 1','Male',u34_third_line_male FROM tx_curr_agg
-- 34.3.2 Male
    UNION ALL SELECT 'HIV_TX_CURR_U34.3. 2','Female',u34_third_line_female FROM tx_curr_agg
-- 39 Adult currently on ART aged 35-39
    UNION ALL SELECT 'HIV_TX_CURR_U39','Adult currently on ART aged 35-39',u39 FROM tx_curr_agg
-- 39.1 Adult currently on ART aged 35-39 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U39.1','Adult currently on ART aged 35-39 on First line regimen by sex',u39_first_line FROM tx_curr_agg
-- 39.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U39.1. 1','Male',u39_first_line_male FROM tx_curr_agg
-- 39.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U39.1. 2','Female',u39_first_line_female FROM tx_curr_agg
-- 39.2 Adult currently on ART aged 35-39 on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U39.2','Adult currently on ART aged 35-39 on Second line regimen by sex',u39_second_line FROM tx_curr_agg
-- 39.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U39.2. 1','Male',u39_second_line_male FROM tx_curr_agg
-- 39.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U39.2. 2','Female',u39_second_line_female FROM tx_curr_agg
-- 39.3 Adult currently on ART aged 35-39 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U39.3','Adult currently on ART aged 35-39 on Third line regimen by sex',u39_third_line FROM tx_curr_agg
-- 39.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U39.3. 1','Male',u39_third_line_male FROM tx_curr_agg
-- 39.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U39.3. 2','Female',u39_third_line_female FROM tx_curr_agg
-- 44 Adult currently on ART aged 40-44
    UNION ALL SELECT 'HIV_TX_CURR_U44','Adult currently on ART aged 40-44',u44  FROM tx_curr_agg
-- 44.1 Adult currently on ART aged 40-44 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U44.1','Adult currently on ART aged 40-44 on First line regimen by sex',u44_first_line FROM tx_curr_agg
-- 44.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U44.1. 1','Male',u44_first_line_male FROM tx_curr_agg
-- 44.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U44.1. 2','Female',u44_first_line_female FROM tx_curr_agg
-- 44.2 Adult currently on ART aged 40-44 on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U44.2','Adult currently on ART aged 40-44 on Second line regimen by sex', u44_second_line FROM tx_curr_agg
-- 44.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U44.2. 1','Male',u44_second_line_male FROM tx_curr_agg
-- 44.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U44.2. 2','Female',u44_second_line_female FROM tx_curr_agg
-- 44.3 Adult currently on ART aged 40-44 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U44.3','Adult currently on ART aged 40-44 on Third line regimen by sex',u44_third_line FROM tx_curr_agg
-- 44.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U44.3. 1','Male',u44_third_line_male FROM tx_curr_agg
-- 44.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U44.3. 2','Female',u44_third_line_female FROM tx_curr_agg
-- 49 Adult currently on ART aged 45-49
    UNION ALL SELECT 'HIV_TX_CURR_U49','Adult currently on ART aged 45-49',u49 FROM tx_curr_agg
-- 49.1 Adult currently on ART aged 45-49 on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U49.1','Adult currently on ART aged 45-49 on First line regimen by sex',u49_first_line FROM tx_curr_agg
-- 49.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U49.1. 1','Male',u49_first_line_male FROM tx_curr_agg
-- 49.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U49.1. 2','Female',u49_first_line_female FROM tx_curr_agg
-- 49.2 Adult currently on ART aged 45-49 on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U49.2','Adult currently on ART aged 45-49 on Second line regimen by sex',u49_second_line  FROM tx_curr_agg
-- 49.2.1 Male
    UNION ALL
    SELECT 'HIV_TX_CURR_U49.2. 1','Male',u49_second_line_male  FROM tx_curr_agg
-- 49.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U49.2. 2','Female',u49_second_line_female  FROM tx_curr_agg
-- 49.3 Adult currently on ART aged 45-49 on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_U49.3','Adult currently on ART aged 45-49 on Third line regimen by sex',u49_third_line FROM tx_curr_agg
-- 49.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_U49.3. 1','Male',u49_third_line_male FROM tx_curr_agg
-- 49.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_U49.3. 2','Female',u49_third_line_female FROM tx_curr_agg
-- 50 Adult currently on ART aged 50+
    UNION ALL SELECT 'HIV_TX_CURR_50','Adult currently on ART aged 50+',greater_than_50 FROM tx_curr_agg
-- 50.1 Adult currently on ART aged 50+ on First line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_50.1','Adult currently on ART aged 50+ on First line regimen by sex',greater_than_50_first_line FROM tx_curr_agg
-- 50.1.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_50.1. 1','Male',greater_than_50_first_line_male FROM tx_curr_agg
-- 50.1.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_50.1. 2','Female',greater_than_50_first_line_female FROM tx_curr_agg
-- 50.2 Adult currently on ART aged 50+ on Second line  regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_50.2','Adult currently on ART aged 50+ on Second line regimen by sex',greater_than_50_second_line FROM tx_curr_agg
-- 50.2.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_50.2. 1','Male',greater_than_50_second_line_male FROM tx_curr_agg
-- 50.2.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_50.2. 2','Female',greater_than_50_second_line_female FROM tx_curr_agg
-- 50.3 Adult currently on ART aged 50+ on Third line regimen by sex
    UNION ALL SELECT 'HIV_TX_CURR_50.3','Adult currently on ART aged 50+ on Third line regimen by sex',greater_than_50_third_line FROM tx_curr_agg
-- 50.3.1 Male
    UNION ALL SELECT 'HIV_TX_CURR_50.3. 1','Male',greater_than_50_third_line_male FROM tx_curr_agg
-- 50.3.2 Female
    UNION ALL SELECT 'HIV_TX_CURR_50.3. 2','Female',greater_than_50_third_line_female FROM tx_curr_agg
-- Currently on ART by pregnancy status
    UNION ALL SELECT 'HIV_TX_CURR_PREG','Currently on ART by pregnancy status',female_total FROM tx_curr_agg
-- Pregnant
    UNION ALL SELECT 'HIV_TX_CURR_PREG. 1','Pregnant',female_pregnant FROM tx_curr_agg
-- Non pregnant
    UNION ALL SELECT 'HIV_TX_CURR_PREG. 2','Non pregnant',female_non_pregnant FROM tx_curr_agg

-- 19 Number of children (<19) who are currently on ART by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19','Number of children (<19) who are currently on ART by regimen type',under_19 FROM tx_curr_agg
-- 1 Children currently on ART aged <1  by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1','Children currently on ART aged <1  by regimen type',u1 FROM tx_curr_agg
-- 1.1 Children currently on ART aged <1 yr on First line  regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1','Children currently on ART aged <1 yr on First line regimen by regimen type',u1_first_line FROM tx_curr_agg
-- 1.1.1 4f - AZT+3TC+LPVr - 0343175a-e931-4f6c-a0c2-99e55637bda9
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1. 1','4f=AZT + 3TC + LPV/r',u1_first_line_4f FROM tx_curr_agg
-- 1.1.2 4g - ABC+3TC+LPVr - e4353cd1-5a0e-4870-8af4-f012ca0145fe
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1. 2','4g=ABC + 3TC + LPV/r',u1_first_line_4g FROM tx_curr_agg
-- 1.1.3 4j - ABC+3TC+DTG - eae01634-65ec-4174-a95c-622456663727
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1. 3','4j= ABC +3TC + DTG',u1_first_line_4j FROM tx_curr_agg
-- 1.1.4 4K - AZT+3TC+DTG - dabcfa6d-56e5-4e5d-a2f2-faf4e18b9211
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1. 4','4k= AZT + 3TC + DTG',u1_first_line_4k FROM tx_curr_agg
-- 1.1.5 4h=other first line - 582c6b67-2f0a-4cc0-b7c0-632b4b387a17
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.1. 5','4h=other first line',u1_first_line_other FROM tx_curr_agg
-- 1.2 Children currently on ART aged <1  on second line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2','Children currently on ART aged <1  on second line regimen by regimen type',u1_second_line FROM tx_curr_agg
-- 1.2.1 5e - ABC+3TC+LPVr - fbb4db0f-4cf4-4ca3-bde0-e20eb065e7da
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2. 1','5e=ABC + 3TC + LPV/r',u1_second_line_5e FROM tx_curr_agg
-- 1.2.2 5f - AZT+3TC+LPV/r - 9ef4d67d-0e05-422a-aac5-80eaf71daadc
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2. 2','5f=AZT + 3TC + LPV/r',u1_second_line_5f FROM tx_curr_agg
-- 1.2.4 5m - ABC+3TC+DTG - 3ed5d8a0-c957-4439-8f86-6c2538e69d3a
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2. 4','5m = ABC + 3TC + DTG',u1_second_line_5m FROM tx_curr_agg
-- 1.2.5 5n - AZT+3TC+DTG - 10c0f78a-cd78-40cd-9824-8d24dc95bb90
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2. 5','5n= AZT + 3TC + DTG',u1_second_line_5n FROM tx_curr_agg
-- 1.2.6 5j - Other Child 2nd line regimen (5j=other secondline) - 0cc9e467-c1ce-4eff-879d-8936a81b6ea3
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.2. 6','5j=other secondline',u1_second_line_other FROM tx_curr_agg
-- 1.3 Children currently on ART aged <1  on Third line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.3','Children currently on ART aged <1  on third line regimen by regimen type',u1_third_line FROM tx_curr_agg
-- 1.3.1 6c - DRVr+DTG+AZT+3TC - 5c8d27eb-4642-4c6f-b131-1182df8d1276
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.3. 1','6c=DRV/r+DTG+AZT+3TC',u1_third_line_6c   FROM tx_curr_agg
-- 1.3.2 6f - DRV/r+DTG+ABC+3TC - e3eb6ae1-fd70-47b2-b1a6-702fae87b061
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.3. 2','6f= DRV/r + DTG + ABC + 3TC',u1_third_line_6f FROM tx_curr_agg
-- 1.3.3 6e - Other Child 3rd line regimen - 57eef726-e977-4efd-996b-a94860341320
    UNION ALL SELECT 'HIV_TX_CURR_REG_U1.3. 3','6e=Other thirdline regimen',u1_third_line_other FROM tx_curr_agg
-- 4 Children currently on ART aged 1-4  by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4','Children currently on ART aged 1-4  by regimen type',u4 FROM tx_curr_agg
-- 4.1 Children currently on ART aged 1-4  on First line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1','Children currently on ART aged 1-4  on First line regimen by regimen type',u4_first_line FROM tx_curr_agg
-- 4.1.1 4d - AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 1','4d = AZT+3TC+EFV',u4_first_line_4d FROM tx_curr_agg
-- 4.1.2 4f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 2','4f=AZT + 3TC + LPV/r',u4_first_line_4f FROM tx_curr_agg
-- 4.1.3 4g - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 3','4g=ABC + 3TC + LPV/r',u4_first_line_4g FROM tx_curr_agg
-- 4.1.4 4g - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 4','4j= ABC +3TC + DTG',u4_first_line_4j FROM tx_curr_agg
-- 4.1.5 4K - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 5','4k= AZT + 3TC + DTG',u4_first_line_4k FROM tx_curr_agg
-- 4.1.6 4L - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 6','4L= ABC + 3TC + EFV',u4_first_line_4l FROM tx_curr_agg
-- 4.1.7 4L - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.1. 7','4h=other first line',u4_first_line_other FROM tx_curr_agg
-- 4.2 Children currently on ART aged 1-4  on Second line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2','Children currently on ART aged 1-4  on Second line regimen by regimen type',u4_second_line FROM tx_curr_agg
-- 4.2.1 5e - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 1','5e=ABC + 3TC + LPV/r',u4_second_line_5e FROM tx_curr_agg
-- 4.2.2 5f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 2','5f=AZT + 3TC + LPV/r',u4_second_line_5f FROM tx_curr_agg
-- 4.2.3 5h - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 3','5h=ABC + 3TC + EFV',u4_second_line_5h FROM tx_curr_agg
-- 4.2.4 5m - ABC+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 4','5m = ABC + 3TC + DTG',u4_second_line_5m FROM tx_curr_agg
-- 4.2.5 5n - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 5','5n= AZT + 3TC + DTG',u4_second_line_5n FROM tx_curr_agg
-- 4.2.6 5j - Other Child 2nd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.2. 6','5j=other secondline',u4_second_line_other FROM tx_curr_agg
-- 4.3 Children currently on ART aged 1-4  on Third Line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3','Children currently on ART aged 1-4  on Third Line regimen by regimen type',u4_third_line FROM tx_curr_agg
-- 4.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3. 1','6c=DRV/r+DTG+AZT+3TC',u4_third_line_6c FROM tx_curr_agg
-- 4.3.2 6f - DRV/r+DTG+ABC+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3. 2','6f= DRV/r + DTG + ABC + 3TC',u4_third_line_6f FROM tx_curr_agg
-- 4.3.3 6g - DRV/r+ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3. 3','6g= DRV/r +ABC+3TC+ EFV',u4_third_line_6g FROM tx_curr_agg
-- 4.3.4 6h - DRV/r+AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3. 4','6h= DRV/r +AZT+3TC+EFV',u4_third_line_6h FROM tx_curr_agg
-- 4.3.5 6e - Other Child 3rd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U4.3. 5','6e=Other thirdline regimen',u4_third_line_other FROM tx_curr_agg
-- 9 Children currently on ART aged 5-9  on by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9','Children currently on ART aged 5-9  on by regimen type',u9 FROM tx_curr_agg
-- 9.1 Children currently on ART aged 5-9  on First line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1','Children currently on ART aged 5-9  on First line regimen by regimen type',u9_first_line FROM tx_curr_agg
-- 9.1.1 4d - AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 1','4d = AZT+3TC+EFV',u9_first_line_4d FROM tx_curr_agg
-- 9.1.2 4e - TDF+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 2','4e= TDF+3TC+EFV',u9_first_line_4e FROM tx_curr_agg
-- 9.1.3 4f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 3','4f=AZT + 3TC + LPV/r',u9_first_line_4f FROM tx_curr_agg
-- 9.1.4 4g - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 4','4g=ABC + 3TC + LPV/r',u9_first_line_4g FROM tx_curr_agg
-- 9.1.5 4i - TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 5','4i= TDF + 3TC + DTG',u9_first_line_4i FROM tx_curr_agg
-- 9.1.6 4j - ABC+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 6','4j= ABC +3TC + DTG',u9_first_line_4j FROM tx_curr_agg
-- 9.1.7 4K - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 7','4k= AZT + 3TC + DTG',u9_first_line_4k FROM tx_curr_agg
-- 9.1.8 4L - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 8','4L= ABC + 3TC + EFV',u9_first_line_4l FROM tx_curr_agg
-- 9.1.9 4h - Other Child 1st line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.1. 9','4h=other first line',u9_first_line_other FROM tx_curr_agg
-- 9.2 Children currently on ART aged 5-9  on Second line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2','Children currently on ART aged 5-9  on Second line regimen by regimen type',u9_second_line FROM tx_curr_agg
-- 9.2.1 5e - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 1','5e=ABC + 3TC + LPV/r',u9_second_line_5e FROM tx_curr_agg
-- 9.2.2 5f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 2','5f=AZT + 3TC + LPV/r',u9_second_line_5f FROM tx_curr_agg
-- 9.2.3 5g - TDF+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 3','5g= TDF + 3TC + EFV',u9_second_line_5g FROM tx_curr_agg
-- 9.2.4 5h - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 4','5h=ABC + 3TC + EFV',u9_second_line_5h FROM tx_curr_agg
-- 9.2.5 5i - TDF+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 5','5i=TDF + 3TC+ LPV/r',u9_second_line_5i FROM tx_curr_agg
-- 9.2.6 5m - ABC+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 6','5m = ABC + 3TC + DTG',u9_second_line_5m FROM tx_curr_agg
-- 9.2.7 5n - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 7','5n= AZT + 3TC + DTG',u9_second_line_5n FROM tx_curr_agg
-- 9.2.8 5o - TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 8','5o= TDF + 3TC + DTG',u9_second_line_5o FROM tx_curr_agg
-- 9.2.9 5j - Other Child 2nd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.2. 9','5j=other secondline',u9_second_line_other FROM tx_curr_agg
-- 9.3 Children currently on ART aged 5-9  on Third Line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3','Children currently on ART aged 5-9  on Third Line regimen by regimen type',u9_third_line FROM tx_curr_agg
-- 9.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 1','6c=DRV/r+DTG+AZT+3TC' ,u9_third_line_6c FROM tx_curr_agg
-- 9.3.2 6d - DRVr+DTG+TDF+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 2','6d=DRV/r+DTG+TDF+3TC',u9_third_line_6d FROM tx_curr_agg
-- 9.3.3 6f - DRV/r+DTG+ABC+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 3','6f= DRV/r + DTG + ABC + 3TC',u9_third_line_6f FROM tx_curr_agg
-- 9.3.4 6g - DRV/r+ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 4','6g= DRV/r +ABC+3TC+ EFV',u9_third_line_6g FROM tx_curr_agg
-- 9.3.5 6h - DRV/r+AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 5','6h= DRV/r +AZT+3TC+EFV',u9_third_line_6h FROM tx_curr_agg
-- 9.3.6 6e - Other Child 3rd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U9.3. 6','6e=Other thirdline regimen',u9_third_line_other FROM tx_curr_agg
-- 14 Children currently on ART aged 10-14 year by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14','Children currently on ART aged 10-14 year by regimen type',u14 FROM tx_curr_agg
-- 14.1 Children currently on ART aged 10-14 year on First line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1','Children currently on ART aged 10-14 year on First line regimen by regimen type',u14_first_line FROM tx_curr_agg
-- 14.1.1 4d - AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 1','4d = AZT+3TC+EFV',u14_first_line_4d FROM tx_curr_agg
-- 14.1.2 4e - TDF+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 2','4e= TDF+3TC+EFV',u14_first_line_4e FROM tx_curr_agg
-- 14.1.3 4f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 3','4f=AZT + 3TC + LPV/r',u14_first_line_4f FROM tx_curr_agg
-- 14.1.4 4g - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 4','4g=ABC + 3TC + LPV/r',u14_first_line_4g FROM tx_curr_agg
-- 14.1.5 4i - TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 5','4i= TDF + 3TC + DTG',u14_first_line_4i FROM tx_curr_agg
-- 14.1.6 4j - ABC+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 6','4j= ABC +3TC + DTG',u14_first_line_4j FROM tx_curr_agg
-- 14.1.7 4K - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 7', '4k= AZT + 3TC + DTG',u14_first_line_4k FROM tx_curr_agg
-- 14.1.8 4L - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 8','4L= ABC + 3TC + EFV',u14_first_line_4L FROM tx_curr_agg
-- 14.1.9 4h - Other Child 1st line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.1. 9','4h=other first line',u14_first_line_other FROM tx_curr_agg
-- 14.2 Children currently on ART aged 10-14 year on Second line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2','Children currently on ART aged 10-14 year on Second line regimen by regimen type',u14_second_line FROM tx_curr_agg
-- 14.2.1 5e - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 1','5e=ABC + 3TC + LPV/r',u14_second_line_5e FROM tx_curr_agg
-- 14.2.2 5f - AZT+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 2','5f=AZT + 3TC + LPV/r',u14_second_line_5f FROM tx_curr_agg
-- 14.2.3 5g - TDF+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 3','5g= TDF + 3TC + EFV',u14_second_line_5g FROM tx_curr_agg
-- 14.2.4 5h - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 4','5h=ABC + 3TC + EFV',u14_second_line_5h FROM tx_curr_agg
-- 14.2.5 5h - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 5','5i=TDF + 3TC+ LPV/r',u14_second_line_5i FROM tx_curr_agg
-- 14.2.6 5m - ABC+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 6','5m = ABC + 3TC + DTG',u14_second_line_5m FROM tx_curr_agg
-- 14.2.7 5n - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 7','5n= AZT + 3TC + DTG',u14_second_line_5n FROM tx_curr_agg
-- 14.2.8 5o - TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 8','5o= TDF + 3TC + DTG',u14_second_line_5o FROM tx_curr_agg
-- 14.2.9 5j - Other Child 2nd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.2. 9','5j=other secondline',u14_second_line_other FROM tx_curr_agg
-- 14.3 Children currently on ART aged 10-14 year on Third Line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3','Children currently on ART aged 10-14 year on Third Line regimen by regimen type',u14_third_line FROM tx_curr_agg
-- 14.3.1 6c - DRVr+DTG+AZT+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 1','6c=DRV/r+DTG+AZT+3TC',u14_third_line_6c FROM tx_curr_agg
-- 14.3.2 6d - DRVr+DTG+TDF+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 2','6d=DRV/r+DTG+TDF+3TC',u14_third_line_6d FROM tx_curr_agg
-- 14.3.3 6f - DRV/r+DTG+ABC+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 3','6f= DRV/r + DTG + ABC + 3TC',u14_third_line_6f FROM tx_curr_agg
-- 14.3.4 6g - DRV/r+ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 4','6g= DRV/r +ABC+3TC+ EFV',u14_third_line_6g FROM tx_curr_agg
-- 14.3.5 6h - DRV/r+AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 5','6h= DRV/r +AZT+3TC+EFV',u14_third_line_6h FROM tx_curr_agg
-- 14.3.6 6e - Other Child 3rd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U14.3. 6','6e=Other thirdline regimen',u14_third_line_other FROM tx_curr_agg
-- 19 Adult currently on ART aged 15-19 by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19','Adult currently on ART aged 15-19 by regimen type',u19 FROM tx_curr_agg
-- 19.1 Adult currently on ART aged 15-19 on First line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1','Adult currently on ART aged 15-19 on First line regimen by regimen type',u19_first_line FROM tx_curr_agg
-- 19.1.1 1d - AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 1','1d = AZT-3TC-EFV',u19_first_line_1d FROM tx_curr_agg
-- 19.1.2 1e - TDF+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 2','1e = TDF-3TC-EFV',u19_first_line_1e FROM tx_curr_agg
-- 19.1.3 1g - ABC+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 3','1g= ABC + 3TC + EFV',u19_first_line_1g FROM tx_curr_agg
-- 19.1.4 1j - TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 4','1j=TDF + 3TC + DTG',u19_first_line_1j FROM tx_curr_agg
-- 19.1.5 1k - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 5','1k= AZT +3TC +DTG',u19_first_line_1k FROM tx_curr_agg
-- 19.1.6 1i - Other Adult 1st line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.1. 6','1i = Other specify',u19_first_line_other FROM tx_curr_agg
-- 19.2 Adult currently on ART aged 15-19 on Second line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2','Adult currently on ART aged 15-19 on Second line regimen by regimen type',u19_second_line FROM tx_curr_agg
-- 19.2.1 1d - AZT+3TC+EFV
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 1','2e= AZT+3TC+LPV/r',u19_second_line_2e FROM tx_curr_agg
-- 19.2.2 2f - AZT+3TC+ATVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 2','2f=AZT+3TC+ATV/r',u19_second_line_2f FROM tx_curr_agg
-- 19.2.3 2f - AZT+3TC+ATVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 3','2g= TDF-3TC-LPV/r',u19_second_line_2g FROM tx_curr_agg
-- 19.2.4 2f - AZT+3TC+ATVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 4','2h =TDF-3TC-ATV/r',u19_second_line_2h FROM tx_curr_agg
-- 19.2.5 2i - ABC+3TC+LPVr
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 5','2i = ABC + 3TC + LPV/r',u19_second_line_2i FROM tx_curr_agg
-- 19.2.6 2j -TDF+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 6','2j = TDF + 3TC + DTG',u19_second_line_2j FROM tx_curr_agg
-- 19.2.7 2k - AZT+3TC+DTG
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 7','2k = AZT + 3TC + DTG', u19_second_line_2k FROM tx_curr_agg
-- 19.2.8 2L - Other Adult 2nd line regimen
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.2. 8','2l=other secondline',u19_second_line_other FROM tx_curr_agg
-- 19.3 Adult currently on ART aged 15-19 on Third Line regimen by regimen type
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.3','Adult currently on ART aged 15-19 on Third Line regimen by regimen type',u19_third_line FROM tx_curr_agg
-- 19.3.1 3a -  DRV/r+DTG+AZT+3TC
    UNION ALL SELECT 'HIV_TX_CURR_REG_U19.3. 1','3a=DRV/r+DTG+AZT/3TC',u19_third_lin FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
-- 19.3.2 3b - DRV/r+DTG+TDF+3TC
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3. 2',
           '3b=DRV/r+DTG+TDF/3TC' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
-- 19.3.3 3c - DRV/r+ABC+3TC+DTG
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3. 3',
           '3c=DRV/r+ABC+3TC+DTG' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
-- 19.3.4 3e - DRV/r+TDF+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3. 4',
           '3e= DRV/r+TDF+3TC+EFV' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
-- 19.3.5 3f - DRV/r+AZT+3TC+EFV
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3. 5',
           '3f= DRV/r+AZT+3TC +EFV' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
-- 19.3.6 3d - Other Adult 3rd line regimen
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_U19.3. 6'          ,
           '3d=Other thirdline',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 19
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
-- 20 Adults >=20 years currently on ART by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20'                               ,
           'Adults >=20 years currently on ART by regimen type',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
-- 20.1 Adults >=20 years currently on First line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1'                                            ,
           'Adults >=20 years currently on First line regimen by regimen type',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '1' or regimen_line = '4')
-- 20.1.1 1d - AZT+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 1',
           '1d = AZT-3TC-EFV, Male' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Male'
-- 20.1.2 1d - AZT+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 2'            ,
           '1d = AZT-3TC-EFV, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.3 1d - AZT+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 3'                ,
           '1d = AZT-3TC-EFV, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1d - AZT+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.4 1e - TDF+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 4',
           '1e = TDF-3TC-EFV, Male' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Male'
-- 20.1.5 1e - TDF+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 5'            ,
           '1e = TDF-3TC-EFV, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.6 1e - TDF+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 6'                ,
           '1e = TDF-3TC-EFV, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1e - TDF+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.7 1g - ABC+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 7',
           '1g= ABC + 3TC + EFV, Male' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Male'
-- 20.1.8 1g - ABC+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 8'            ,
           '1g= ABC + 3TC + EFV, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.9 1g - ABC+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 9'                ,
           '1g= ABC + 3TC + EFV, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1g - ABC+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.10 1j - TDF+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 10',
           '1j=TDF + 3TC + DTG, Male'  ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Male'
-- 20.1.11 1j - TDF+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 11'           ,
           '1j=TDF + 3TC + DTG, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.12 1j - TDF+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 12'               ,
           '1j=TDF + 3TC + DTG, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1j - TDF+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.13 1k - AZT+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 13',
           '1k= AZT +3TC +DTG, Male'  ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Male'
-- 20.1.14 1k - AZT+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 14'           ,
           '1k= AZT +3TC +DTG, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.15 1k - AZT+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 15'               ,
           '1k= AZT +3TC +DTG, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '1k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.1.16 1i - Other Adult 1st line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 16'               ,
           '1i = Other specify, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Male'
-- 20.1.17 1i - Other Adult 1st line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 17'                            ,
           '1i = Other specify, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.1.18 1i - Other Adult 1st line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.1. 18'                                ,
           '1i = Other specify, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in
          ('1d - AZT+3TC+EFV', '1e - TDF+3TC+EFV', '1g - ABC+3TC+EFV', '1j - TDF+3TC+DTG', '1k - AZT+3TC+DTG')
      AND (regimen_line = '1' or regimen_line = '4')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2  Adults >=20 years currently on Second line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2'                                             ,
           'Adults >=20 years currently on Second line regimen by regimen type',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '2' or regimen_line = '5')
-- 20.2.1 2e - AZT+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 1',
           '2e= AZT+3TC+LPV/r, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.2 2e - AZT+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 2'             ,
           '2e= AZT+3TC+LPV/r, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.3 2e - AZT+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 3'                 ,
           '2e= AZT+3TC+LPV/r, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2e - AZT+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.4 2f - AZT+3TC+ATVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 4',
           '2f=AZT+3TC+ATV/r, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Male'
-- 20.2.5 2f - AZT+3TC+ATVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 5'             ,
           '2f=AZT+3TC+ATV/r, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.6 2f - AZT+3TC+ATVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 6'                 ,
           '2f=AZT+3TC+ATV/r, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2f - AZT+3TC+ATVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.7 2g - TDF+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 7',
           '2g= TDF-3TC-LPV/r, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.8 2g - TDF+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 8'             ,
           '2g= TDF-3TC-LPV/r, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.9 2g - TDF+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 9'                 ,
           '2g= TDF-3TC-LPV/r, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2g - TDF+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.10 2h - TDF+3TC+ATVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 10',
           '2h =TDF-3TC-ATV/r, Male' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Male'
-- 20.2.11 2h - TDF+3TC+ATVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 11'            ,
           '2h =TDF-3TC-ATV/r, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.12 2h - TDF+3TC+ATVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 12'                ,
           '2h =TDF-3TC-ATV/r, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2h - TDF+3TC+ATVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.13 2i - ABC+3TC+LPVr, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 13',
           '2i = ABC + 3TC + LPV/r, Male' ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Male'
-- 20.2.14 2i - ABC+3TC+LPVr, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 14'            ,
           '2i = ABC + 3TC + LPV/r, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.15 2i - ABC+3TC+LPVr, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 15'                ,
           '2i = ABC + 3TC + LPV/r, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2i - ABC+3TC+LPVr'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.16 2j -TDF+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 16',
           '2j = TDF + 3TC + DTG, Male'   ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Male'
-- 20.2.17 2j -TDF+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 17'          ,
           '2j = TDF + 3TC + DTG, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.18 2j -TDF+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 18'              ,
           '2j = TDF + 3TC + DTG, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2j -TDF+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.19 2k - AZT+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 19',
           '2k = AZT + 3TC + DTG, Male'  ,
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Male'
-- 20.2.20 2k - AZT+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 20'           ,
           '2k = AZT + 3TC + DTG, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.21 2k - AZT+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 21'               ,
           '2k = AZT + 3TC + DTG, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '2k - AZT+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.2.22 2L - Other Adult 2nd line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 22'               ,
           '2l=other secondline, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Male'
-- 20.2.23 2L - Other Adult 2nd line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 23'                            ,
           '2l=other secondline, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.2.24 2L - Other Adult 2nd line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.2. 24'                                ,
           '2l=other secondline, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('2e - AZT+3TC+LPVr', '2f - AZT+3TC+ATVr', '2g - TDF+3TC+LPVr', '2h - TDF+3TC+ATVr',
                          '2i - ABC+3TC+LPVr', '2j -TDF+3TC+DTG', '2k - AZT+3TC+DTG')
      AND (regimen_line = '2' or regimen_line = '5')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)

-- 20.3 Adults >=20 years currently on Third Line regimen by regimen type
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3'                                            ,
           'Adults >=20 years currently on Third Line regimen by regimen type',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND (regimen_line = '3' or regimen_line = '6')
-- 20.3.1 3a -  DRV/r+DTG+AZT+3TC, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 1'      ,
           '3a=DRV/r+DTG+AZT/3TC, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Male'
-- 20.3.2 3a -  DRV/r+DTG+AZT+3TC, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 2'                   ,
           '3a=DRV/r+DTG+AZT/3TC, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.3 3a -  DRV/r+DTG+AZT+3TC, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 3'                       ,
           '3a=DRV/r+DTG+AZT/3TC, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3a -  DRV/r+DTG+AZT+3TC'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.4 3b - DRV/r+DTG+TDF+3TC, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 4'     ,
           '3b=DRV/r+DTG+TDF/3TC, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Male'
-- 20.3.5 3b - DRV/r+DTG+TDF+3TC, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 5'                  ,
           '3b=DRV/r+DTG+TDF/3TC, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.6 3b - DRV/r+DTG+TDF+3TC, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 6'                      ,
           '3b=DRV/r+DTG+TDF/3TC, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3b - DRV/r+DTG+TDF+3TC'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.7 3c - DRV/r+ABC+3TC+DTG, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 7'     ,
           '3c=DRV/r+ABC+3TC+DTG, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Male'
-- 20.3.8 3c - DRV/r+ABC+3TC+DTG, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 8'                  ,
           '3c=DRV/r+ABC+3TC+DTG, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.9 3c - DRV/r+ABC+3TC+DTG, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 9'                      ,
           '3c=DRV/r+ABC+3TC+DTG, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3c - DRV/r+ABC+3TC+DTG'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.10 3e - DRV/r+TDF+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 10'    ,
           '3e= DRV/r+TDF+3TC+EFV, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Male'
-- 20.3.11 3e - DRV/r+TDF+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 11'                 ,
           '3e= DRV/r+TDF+3TC+EFV, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.12 3e - DRV/r+TDF+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 12'                     ,
           '3e= DRV/r+TDF+3TC+EFV, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3e - DRV/r+TDF+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)
-- 20.3.13 3f - DRV/r+AZT+3TC+EFV, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 13'    ,
           '3f= DRV/r+AZT+3TC +EFV, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Male'
-- 20.3.14 3f - DRV/r+AZT+3TC+EFV, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 14'                 ,
           '3f= DRV/r+AZT+3TC +EFV, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.15 3f - DRV/r+AZT+3TC+EFV, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 15'                     ,
           '3f= DRV/r+AZT+3TC +EFV, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen = '3f - DRV/r+AZT+3TC+EFV'
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null)

-- 20.3.16 3d - Other Adult 3rd line regimen, Male
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 16'               ,
           '3d=Other thirdline, Male',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Male'
-- 20.3.17 3d - Other Adult 3rd line regimen, Female - pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 17'                            ,
           '3d=Other thirdline, Female - pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'
      AND pregnancy_status = 'Yes'
-- 20.3.18 3d - Other Adult 3rd line regimen, Female - non-pregnant
    UNION ALL
    SELECT 'HIV_TX_CURR_REG_20.3. 18'                                ,
           '3d=Other thirdline, Female - non-pregnant',
           COUNT(*)
    FROM tx_curr_agg
    WHERE TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) >= 20
      AND regimen not in ('3a -  DRV/r+DTG+AZT+3TC', '3b - DRV/r+DTG+TDF+3TC', '3c - DRV/r+ABC+3TC+DTG',
                          '3e - DRV/r+TDF+3TC+EFV', '3f - DRV/r+AZT+3TC+EFV')
      AND (regimen_line = '3' or regimen_line = '6')
      AND sex = 'Female'
      AND (pregnancy_status = 'No' or pregnancy_status is null);
END //

DELIMITER ;




