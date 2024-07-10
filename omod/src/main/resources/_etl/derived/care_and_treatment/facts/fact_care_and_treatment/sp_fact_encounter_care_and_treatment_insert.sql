-- $BEGIN
INSERT INTO mamba_fact_care_and_treatment (
                                            client_id,
                                            return_visit_date,
                                            hiv_confirmed_date,
                                            art_start_date,
                                            age_at_enrollment,
                                            days_difference,
                                            enrollment_date,
                                            pregnancy_status,
                                            regimen_at_enrollment,
                                            arv_dose_at_enrollment,
                                            last_follow_up_status,
                                            next_visit_date
                                             )
SELECT      DISTINCT
    enc_follow_up.client_id,
    max_table.return_visit_date,
    min_table.date_of_hiv_diagnosis,
    min_table.art_start_date,
    DATEDIFF(enc_int_a.date_enrolled_in_care, dim_client.date_of_birth) / 365 AS age_at_enrollment,
    DATEDIFF(enc_int_a.date_enrolled_in_care,min_table.date_of_hiv_diagnosis) / 365 AS days_difference,
    enc_int_a.date_enrolled_in_care,
    max_table.pregnancy_status,
    min_table.regimen,
    min_table.antiretroviral_art_dispensed_dose_i,
    max_table.follow_up_status,
    max_table.return_visit_date
FROM mamba_flat_encounter_follow_up enc_follow_up
         LEFT JOIN (SELECT client_id,
                           MIN(art_antiretroviral_start_date) AS arv_start_date
                    FROM mamba_flat_encounter_follow_up
                    GROUP BY client_id) art_date ON enc_follow_up.client_id = art_date.client_id
         LEFT JOIN (SELECT client_id,
                           return_visit_date,
                           follow_up_status,
                           pregnancy_status,
                           follow_up_date_followup_
                    FROM (SELECT *,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS row_num
                          FROM mamba_flat_encounter_follow_up) max_table
                    WHERE row_num = 1
                      and follow_up_status = 'Alive') max_table ON enc_follow_up.client_id = max_table.client_id
         LEFT JOIN (SELECT client_id,
                           date_of_hiv_diagnosis,
                           art_antiretroviral_start_date AS art_start_date,
                           antiretroviral_art_dispensed_dose_i,
                           regimen
                    FROM (SELECT *,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_) AS row_num
                          FROM mamba_flat_encounter_follow_up) max_table
                    WHERE row_num = 1
                      and follow_up_status = 'Alive') min_table ON enc_follow_up.client_id = min_table.client_id
         LEFT JOIN mamba_flat_encounter_intake_a enc_int_a on enc_follow_up.client_id=enc_int_a.encounter_id
         LEFT JOIN mamba_dim_client_care_and_treatment dim_client on dim_client.client_id = enc_follow_up.client_id;
-- $END
