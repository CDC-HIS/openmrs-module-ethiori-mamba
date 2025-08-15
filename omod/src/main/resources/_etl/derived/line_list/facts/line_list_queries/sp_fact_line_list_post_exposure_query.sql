DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_line_list_post_exposure_query;
CREATE PROCEDURE sp_fact_line_list_post_exposure_query(IN REPORT_START_DATE DATE,
                                                       IN REPORT_END_DATE DATE)
BEGIN
    WITH post_information AS (select encounter_id,
                                     ps.reporting_date,
                                     ps.client_id,
                                     occupation_retired,
                                     exposure_duration,
                                     exposure_type,
                                     exposure_code,
                                     exposed_person,
                                     source_of_exposure,
                                     source_person,
                                     eligible,
                                     ps.postexposure_prophylaxis_regimen,
                                     time_between_exposure_and_pep_in_hours,
                                     case_team
                              FROM mamba_flat_encounter_exposed_person_information as ps
                              WHERE ps.reporting_date >= COALESCE(REPORT_START_DATE, CURDATE())
                                AND ps.reporting_date <= COALESCE(REPORT_END_DATE, CURDATE()))
            ,
         tmp_latest_reported AS (SELECT p.*,
                                        ROW_NUMBER() OVER (PARTITION BY p.client_id ORDER BY reporting_date DESC,
                                            encounter_id DESC) AS row_num
                                 FROM post_information as p
                                          join mamba_dim_client client on p.client_id = client.client_id),


         latest_peported AS (select *
                             from tmp_latest_reported
                             where row_num = 1),

         2_weeks_follow_up as (select f.client_id,
                                      f.anitiretroviral_adherence_level,
                                      f.medication_side_effect,
                                      f.final_post_pep_hiv_status,
                                      f.visit_period
                               from mamba_flat_encounter_post_exposure_foll as f
                                        inner join latest_peported as p on p.client_id = f.client_id
                                   and f.reporting_date = p.reporting_date
                               where f.visit_period in ('2 weeks'))
            ,
         4_weeks_follow_up as (select f.client_id,
                                      f.anitiretroviral_adherence_level,
                                      f.medication_side_effect,
                                      f.final_post_pep_hiv_status,
                                      f.visit_period
                               from mamba_flat_encounter_post_exposure_foll as f
                                        inner join latest_peported as p on p.client_id = f.client_id
                                   and f.reporting_date = p.reporting_date
                               where f.visit_period in ('4 weeks')),
         6_weeks_follow_up as (select f.client_id,
                                      f.anitiretroviral_adherence_level,
                                      f.medication_side_effect,
                                      f.final_post_pep_hiv_status,
                                      f.visit_period
                               from mamba_flat_encounter_post_exposure_foll as f
                                        inner join latest_peported as p on p.client_id = f.client_id
                                   and f.reporting_date = p.reporting_date
                               where f.visit_period in ('6 weeks')),
         3_months_follow_up as (select f.client_id,
                                       f.anitiretroviral_adherence_level,
                                       f.medication_side_effect,
                                       f.final_post_pep_hiv_status,
                                       f.visit_period
                                from mamba_flat_encounter_post_exposure_foll as f
                                         inner join latest_peported as p on p.client_id = f.client_id
                                    and f.reporting_date = p.reporting_date
                                where f.visit_period in ('3 months')),
         6_months_follow_up as (select f.client_id,
                                       f.anitiretroviral_adherence_level,
                                       f.medication_side_effect,
                                       f.final_post_pep_hiv_status,
                                       f.visit_period
                                from mamba_flat_encounter_post_exposure_foll as f
                                         inner join latest_peported as p on p.client_id = f.client_id
                                    and f.reporting_date = p.reporting_date
                                where f.visit_period in ('6 months'))


    SELECT ROW_NUMBER() OVER (ORDER BY patient_name)           as `#`,
           client.patient_uuid                                 as `Patient GUID`,
           client.patient_name                                 as `Patient Name`,
           CAST(client.mrn AS CHAR(20))                        as MRN,
           client.uan                                          AS UAN,

           TIMESTAMPDIFF(YEAR, client.date_of_birth,
                         COALESCE(REPORT_END_DATE, CURDATE())) as `Age`,
           client.sex                                          as Sex,
           f_case.occupation_retired                           as `Occupation Retired`,
           f_case.case_team                                    as `Department/Case Team`,
           f_case.reporting_date                               as `Reporting Date EC.`,
           f_case.reporting_date                               as `Reporting Date GC.`,
           f_case.exposure_duration                            as `Exposure Duration (in hours)`,
           f_case.exposure_type                                as `Exposure Type`,
           f_case.exposure_code                                as `Exposure Code`,
           f_case.source_person                                as `Source Person HIV Status`,
           f_case.exposed_person                               as `Exposed Person HIV Status`,
           f_case.source_of_exposure                           as `Source of Exposure`,
           f_case.eligible                                     as `Eligible for PEP?`,
           f_case.postexposure_prophylaxis_regimen             as `ARV (PEP) Regimen`,
           f_case.time_between_exposure_and_pep_in_hours       as `Time Between Exposure and PEP (in hours)`,

           two_weeks.visit_period                              as `Follow-Up Visit Date (at 2WK) EC.`,
           two_weeks.visit_period                              as `Follow-Up Visit Date (at 2WK) GC.`,
           two_weeks.anitiretroviral_adherence_level           as `Adherence  (at 2WK)`,
           two_weeks.medication_side_effect                    as ` Side Effect (at 2WK)`,
           two_weeks.final_post_pep_hiv_status                 as `Exposed client HIV Status (at 2WK)`,

           four_weeks.visit_period                              as `Follow-Up Visit Date (at 4WK) EC.`,
           four_weeks.visit_period                              as `Follow-Up Visit Date (at 4WK) GC.`,
           four_weeks.anitiretroviral_adherence_level           as `Adherence  (at 4WK)`,
           four_weeks.medication_side_effect                    as ` Side Effect (at 4WK)`,
           four_weeks.final_post_pep_hiv_status                 as `Exposed client HIV Status (at 4WK)`,

           six_weeks.visit_period                              as `Follow-Up Visit Date (at 6WK) EC.`,
           six_weeks.visit_period                              as `Follow-Up Visit Date (at 6WK) GC.`,
           six_weeks.anitiretroviral_adherence_level           as `Adherence  (at 6WK)`,
           six_weeks.medication_side_effect                    as ` Side Effect (at 6WK)`,
           six_weeks.final_post_pep_hiv_status                 as `Exposed client HIV Status (at 6WK)`,

           three_months.visit_period                              as `Follow-Up Visit Date (at 3M) EC.`,
           three_months.visit_period                              as `Follow-Up Visit Date (at 3M) GC.`,
           three_months.anitiretroviral_adherence_level           as `Adherence  (at 3M)`,
           three_months.medication_side_effect                    as ` Side Effect (at 3M)`,
           three_months.final_post_pep_hiv_status                 as `Exposed client HIV Status (at 3M)`,

           six_months.visit_period                              as `Follow-Up Visit Date (at 6M) EC.`,
           six_months.visit_period                              as `Follow-Up Visit Date (at 6M) GC.`,
           six_months.anitiretroviral_adherence_level           as `Adherence  (at 6M)`,
           six_months.medication_side_effect                    as ` Side Effect (at 6M)`,
           six_months.final_post_pep_hiv_status                 as `Exposed client HIV Status (at 6M)`

    FROM latest_peported AS f_case
             INNER JOIN mamba_dim_client client on f_case.client_id = client.client_id
             Left JOIN 2_weeks_follow_up as two_weeks
                       ON f_case.client_id = two_weeks.client_id
             Left JOIN 4_weeks_follow_up as four_weeks
                       ON f_case.client_id = four_weeks.client_id
             Left JOIN 6_weeks_follow_up as six_weeks
                       ON f_case.client_id = six_weeks.client_id
             Left JOIN 3_months_follow_up as three_months
                       ON f_case.client_id = three_months.client_id
             Left JOIN 6_months_follow_up as six_months
                       ON f_case.client_id = six_months.client_id

    ORDER BY client.patient_name;

END //

DELIMITER ;
