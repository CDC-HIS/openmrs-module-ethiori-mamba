DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_vl_eligibility_linelist_data_extraction_query;

CREATE PROCEDURE  sp_fact_vl_eligibility_linelist_data_extraction_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH LatestFollowUp AS (
            SELECT *
            FROM (SELECT
                    f.client_id,
                    f.encounter_id,
                    per.uuid,
                    weight_text_,
                    date_of_event,
                    art_antiretroviral_start_date,
                    follow_up_date_followup_,
                    pregnancy_status,
                    regimen,
                    antiretroviral_art_dispensed_dose_i,
                    next_visit_date,
                    follow_up_status,
                    treatment_end_date,
                    routine_viral_load_test_indication,
                    currently_breastfeeding_child,
                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
                FROM mamba_flat_encounter_follow_up as f
                    LEFT JOIN mamba_flat_encounter_follow_up_1 f1 ON f.encounter_id = f1.encounter_id
                    LEFT JOIN mamba_flat_encounter_follow_up_2 f2 ON f2.encounter_id = f.encounter_id
                    LEFT JOIN mamba_dim_person per on per.person_id = f.client_id
                WHERE follow_up_date_followup_ <= REPORT_END_DATE
                ) sub
            WHERE sub.rn_desc = 1 
        ),
        VLSentDate AS (
            select *
            FROM (
                SELECT client_id
                        ,encounter_id
                        ,date_of_reported_hiv_viral_load AS ViralLoadSentDate
                        ,ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_of_reported_hiv_viral_load DESC) AS rn
                FROM mamba_flat_encounter_follow_up_2
                WHERE date_of_reported_hiv_viral_load <= REPORT_END_DATE) sub 
            WHERE sub.rn = 1
        ),
        SwitchSubdate AS (
            SELECT *
            FROM (
                    SELECT f.client_id,
                            follow_up_date_followup_ as FollowupDate,
                            regimen_change,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS rn
                    FROM mamba_flat_encounter_follow_up f 
                        INNER JOIN mamba_flat_encounter_follow_up_1 f1 ON f.client_id = f1.client_id
                    WHERE regimen_change = 'Regimen switch type' 
                        AND follow_up_date_followup_ <= REPORT_END_DATE
                    ) sub
            WHERE sub.rn = 1
        ),
        VLPerformedDate1 AS (
            SELECT * from (
                SELECT f.client_id
                        ,f.encounter_id
                        ,date_viral_load_results_received
                        ,ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_viral_load_results_received DESC) AS rn
                FROM mamba_flat_encounter_follow_up f 
                    INNER JOIN mamba_flat_encounter_follow_up_2 as f2 ON f.client_id = f2.client_id and f.encounter_id = f2.encounter_id
                WHERE follow_up_status IS NOT NULL
                    AND (date_viral_load_results_received <= REPORT_END_DATE OR date_viral_load_results_received IS NULL)
                ) sub
                where sub.rn = 1
        ),
        VLPerformedDate2 AS (
            SELECT vlp1.client_id,
                    vlp1.encounter_id,
                    CASE WHEN vlp1.date_viral_load_results_received < vls.ViralLoadSentDate THEN NULL
                                                    ELSE vlp1.date_viral_load_results_received
                                                END AS ViralLoadPerformedDate,
                    CASE WHEN vlp1.date_viral_load_results_received < vls.ViralLoadSentDate THEN NULL
                                            ELSE viral_load_test_status
                                        END AS ViralLoadStatus,
                    CASE WHEN f.hiv_viral_load REGEXP '^[0-9]+$'
                                                    AND f.hiv_viral_load > 0
                                                    AND vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                THEN CAST(f.hiv_viral_load AS DECIMAL(12, 2))
                                            ELSE NULL
                                        END AS ViralLoadCount,
                    CASE WHEN viral_load_test_status IS NULL
                                                            AND vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                        THEN NULL
                                                    WHEN vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                            AND (viral_load_test_status LIKE 'Unsuppressed%'
                                                                    OR viral_load_test_status LIKE 'HIV infection with high viral load%'
                                                                    OR viral_load_test_status LIKE 'Low-level viremia%')
                                                        THEN 'U'
                                                    WHEN vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                            AND (viral_load_test_status LIKE 'Suppressed%')
                                                        THEN 'S'
                                                    WHEN vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                            AND IFNULL(hiv_viral_load, 0) > CAST(50 AS DECIMAL)
                                                        THEN 'U'
                                                    WHEN vlp1.date_viral_load_results_received >= vls.ViralLoadSentDate
                                                            AND IFNULL(hiv_viral_load, 0) <= CAST(50 AS DECIMAL)
                                                        THEN 'S'
                                                    ELSE NULL
                                                END AS ViralLoadStatusInferred,
                    CASE WHEN vls.ViralLoadSentDate IS NOT NULL THEN vls.ViralLoadSentDate
                                                    WHEN vlp1.date_viral_load_results_received IS NOT NULL THEN 
                                                        vlp1.date_viral_load_results_received
                                                    ELSE NULL
                                                END AS ViralLoadReferenceDate     
            FROM VLPerformedDate1 vlp1 
                    LEFT JOIN VLSentDate vls ON vls.encounter_id = vlp1.encounter_id 
                    LEFT JOIN mamba_flat_encounter_follow_up f ON f.encounter_id = vlp1.encounter_id
                    LEFT JOIN mamba_flat_encounter_follow_up_1 f1 ON f1.encounter_id = vlp1.encounter_id
        )
    SELECT dim.sex
            ,lf.weight_text_ as 'Weight'
            ,dim.current_age AS 'Age'
            ,lf.date_of_event as 'date_hiv_confirmed'
            ,lf.art_antiretroviral_start_date AS 'art_start_date'
            ,lf.follow_up_date_followup_ As 'FollowUpDate'
            ,lf.pregnancy_status AS 'IsPregnant'
            ,lf.regimen AS 'ARVDispendsedDose'
            ,lf.antiretroviral_art_dispensed_dose_i as 'art_dose'
            ,lf.next_visit_date as 'next_visit_date'
            ,lf.follow_up_status AS 'follow_up_status'
            ,lf.treatment_end_date AS 'art_dose_End'
            ,vlp2.ViralLoadPerformedDate AS 'viral_load_perform_date'
            ,vlp2.ViralLoadStatus AS 'viral_load_status'
            ,vlp2.ViralLoadCount AS 'viral_load_count'
            ,vls.ViralLoadSentDate AS 'viral_load_sent_date'
            ,CASE WHEN vls.ViralLoadSentDate IS NOT NULL THEN vls.ViralLoadSentDate
                WHEN vlp2.ViralLoadPerformedDate IS NOT NULL THEN 
                    vlp2.ViralLoadPerformedDate
                ELSE NULL
            END AS 'viral_load_ref_date'
            ,switch.FollowupDate as 'date_regimen_change'
            ,CASE WHEN vlp2.ViralLoadReferenceDate IS NULL
                            AND lf.follow_up_status = 'Restart medication'
                        THEN DATE_ADD(lf.follow_up_date_followup_, INTERVAL 91 DAY) -- client restarted ART
                    WHEN vlp2.ViralLoadReferenceDate IS NULL
                            AND switch.FollowupDate IS NOT NULL
                        THEN DATE_ADD(switch.FollowupDate, INTERVAL 181 DAY) -- Regimen Change
                    WHEN vlp2.ViralLoadReferenceDate IS NULL
                            AND lf.pregnancy_status = 'Yes'
                            AND (DATEDIFF(REPORT_END_DATE, lf.art_antiretroviral_start_date) > 90 )
                        THEN DATE_ADD(lf.art_antiretroviral_start_date, INTERVAL 91 DAY) -- First VL for Pregnant
                    WHEN vlp2.ViralLoadReferenceDate IS NULL
                            AND (DATEDIFF(REPORT_END_DATE, lf.art_antiretroviral_start_date) <= 180)
                        THEN NULL -- not applicable
                    WHEN vlp2.ViralLoadReferenceDate IS NULL
                            AND (DATEDIFF(REPORT_END_DATE, lf.art_antiretroviral_start_date) > 180)
                        THEN DATE_ADD(lf.art_antiretroviral_start_date, INTERVAL 181 DAY) -- First VL 
                    WHEN (vlp2.ViralLoadReferenceDate IS NOT NULL
                            AND vlp2.ViralLoadReferenceDate < lf.follow_up_date_followup_)
                            AND lf.follow_up_status = 'Restart medication'
                        THEN DATE_ADD(lf.follow_up_date_followup_, INTERVAL 91 DAY)  -- client restarted ART
                    WHEN vlp2.ViralLoadReferenceDate IS NOT NULL
                            AND vlp2.ViralLoadReferenceDate < switch.FollowupDate
                            AND switch.FollowupDate IS NOT NULL 
                        THEN DATE_ADD(switch.FollowupDate, INTERVAL 181 DAY) -- Regimen Change
                    WHEN vlp2.ViralLoadReferenceDate IS NOT NULL
                            AND vlp2.ViralLoadStatusInferred = 'U'
                        THEN DATE_ADD(vlp2.ViralLoadReferenceDate, INTERVAL 91 DAY) -- unsupressed  Repeat/Confirmatory Viral Load test
                    WHEN vlp2.ViralLoadReferenceDate IS NOT NULL
                            AND (lf.pregnancy_status = 'Yes' OR lf.currently_breastfeeding_child = 'Yes')
                            AND lf.routine_viral_load_test_indication 
                                IN ('First viral load test at 6 months',
                                    'VL > 50 and <= 1000'
                                    'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml')
                        THEN DATE_ADD(vlp2.ViralLoadReferenceDate, INTERVAL 91 DAY) -- Pregnant/Breastfeeding and needs retesting
                    WHEN vlp2.ViralLoadReferenceDate IS NOT NULL
                            AND (lf.pregnancy_status = 'Yes' OR lf.currently_breastfeeding_child = 'Yes')
                            AND lf.routine_viral_load_test_indication IS NOT NULL
                            AND lf.routine_viral_load_test_indication 
                                NOT IN ('First viral load test at 6 months',
                                        'VL > 50 and <= 1000'
                                        'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml')
                        THEN DATE_ADD(vlp2.ViralLoadReferenceDate, INTERVAL 181 DAY) -- Pregnant/Breastfeeding and needs retesting
                    WHEN vlp2.ViralLoadReferenceDate IS NOT NULL
                        THEN DATE_ADD(vlp2.ViralLoadReferenceDate, INTERVAL 365 DAY) -- Annual Viral Load Test
                    ELSE '12-31-9999'
            END AS 'eligiblityDate'
            ,mdp.uuid as 'PatientGUID'
            ,lf.currently_breastfeeding_child as 'breast_feeding_status'
            ,CASE WHEN lf.pregnancy_status = 'Yes' OR lf.currently_breastfeeding_child = 'Yes' THEN 'Yes' END AS 'PMTCT_ART'
    FROM LatestFollowUp AS lf 
        INNER JOIN mamba_dim_client_art_follow_up AS dim ON lf.client_id = dim.client_id 
        INNER JOIN mamba_dim_person mdp ON mdp.person_id = lf.client_id
        LEFT JOIN VLPerformedDate2 AS vlp2 ON vlp2.client_id = lf.client_id
        LEFT JOIN VLSentDate AS vls ON vls.client_id = lf.client_id 
        LEFT JOIN SwitchSubdate switch ON switch.client_id = lf.client_id 
        WHERE follow_up_status = 'Alive' or follow_up_status = 'Restart medication';
END //
DELIMITER ;    