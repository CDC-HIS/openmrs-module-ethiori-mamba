DELIMITER //
DROP PROCEDURE IF EXISTS sp_fact_cxca_linelist_data_extraction_query;

CREATE PROCEDURE  sp_fact_cxca_linelist_data_extraction_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH Followup AS (
            SELECT *
            FROM (
                SELECT f.client_id
                        ,f.encounter_id
                        ,follow_up_date_followup_
                        ,follow_up_status
                        ,hpv_dna_result_received_date
                        ,hpv_dna_screening_result
                        ,date_visual_inspection_of_the_cervi
                        ,via_screening_result
                        ,date_cytology_result_received
                        ,colposcopy_of_cervix_findings
                        ,cytology_result
                        ,treatment_start_date
                        ,next_follow_up_screening_date
                        ,art_antiretroviral_start_date as art_start_date
                        ,ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
                FROM mamba_flat_encounter_follow_up as f
                        LEFT JOIN mamba_flat_encounter_follow_up_1 as f1 ON f1.encounter_id = f.encounter_id
                        LEFT JOIN mamba_flat_encounter_follow_up_2 as f2 ON f2.encounter_id = f.encounter_id
                    WHERE follow_up_date_followup_ <= REPORT_END_DATE 
                    and cervical_cancer_screening_status like 'Cervical cancer screening performed'
                ) as sub
            WHERE sub.rn_desc = 1
        ),
        Lateset_Followup AS (
            SELECT *
            FROM (
                SELECT f.client_id
                        ,f.encounter_id
                        ,f.follow_up_status
                        ,sex
                        ,weight_text_
                        ,art_antiretroviral_start_date as `art_start_date`
                        ,follow_up_date_followup_ as `FollowUpDate`
                        ,mdp.uuid as `PatientGUID`
                        ,antiretroviral_art_dispensed_dose_i as `ARTDoseDays`
                        ,CEILING(TIMESTAMPDIFF(MONTH , date_of_birth, REPORT_END_DATE) / 12) AS age
                        ,ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
                FROM mamba_flat_encounter_follow_up f
                        INNER JOIN mamba_flat_encounter_follow_up_1 as f1 ON f1.encounter_id = f.encounter_id
                        LEFT JOIN mamba_flat_encounter_follow_up_2 f2 ON f2.encounter_id = f.encounter_id
                        LEFT JOIN mamba_dim_client_art_follow_up AS dim ON dim.client_id = f.client_id
                        LEFT JOIN mamba_dim_person mdp ON mdp.person_id = f.client_id
                ) as sub
            WHERE rn_desc = 1 AND follow_up_status IS NOT NULL AND follow_up_status NOT IN ('Dead','Transferred out')
        )
        ,Why_eligible AS (
            SELECT client_id
                    ,encounter_id
                    ,art_start_date
                    ,CASE WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, REPORT_END_DATE) > 1095
                                AND hpv_dna_screening_result = 'Negative result'
                            THEN 'HPV Screened Negative-Need Re-screening'
                        WHEN TIMESTAMPDIFF(DAY, date_visual_inspection_of_the_cervi, REPORT_END_DATE) > 730
                                AND via_screening_result = 'VIA negative' 
                            THEN 'VIA Screened Negative-Need Re-screening'
                        WHEN TIMESTAMPDIFF(DAY, date_cytology_result_received, REPORT_END_DATE) > 1095
                                AND cytology_result = 'Negative result' 
                            THEN 'Cytology Screened Negative-Need Re-screening'
                        WHEN (colposcopy_of_cervix_findings IN ('Low Grade', 'High Grade')
                                OR cytology_result like 'High Grade'
                                OR via_screening_result IN ('VIA positive: eligible for cryo/thermo-coagula',
                                                        'VIA positive: non-eligible for cryo/thermo-coagula'))
                            THEN 'Positive Result with No Treatment Date'
                        WHEN TIMESTAMPDIFF(DAY, treatment_start_date, REPORT_END_DATE) > 181
                            THEN 'Treated with Cryotherapy/Thermocoagulation/LEEP'
                        WHEN TIMESTAMPDIFF(DAY, hpv_dna_result_received_date, REPORT_END_DATE) > 365
                                AND hpv_dna_screening_result = 'Positive'
                                AND via_screening_result = 'VIA negative'
                            THEN 'HPV Positive but VIA Negative- Need Re-screening'
                        WHEN next_follow_up_screening_date <= REPORT_END_DATE THEN 'Other' END AS WhyEligible
            FROM Followup
        )
        ,Why_elligible_null AS (
            SELECT *
            FROM Why_eligible w
            WHERE w.WhyEligible IS NULL
        )
        SELECT sex
                ,weight_text_ as `Weight`
                ,Age
                ,lf.art_start_date
                ,lf.FollowUpDate
                ,lf.ARTDoseDays
                ,lf.follow_up_status
                ,lf.PatientGUID
        FROM Lateset_Followup lf 
        LEFT JOIN `Why_eligible` AS why ON lf.encounter_id = why.encounter_id
        LEFT JOIN `Why_elligible_null` as why_null on lf.client_id = why_null.client_id
        WHERE sex = 'FEMALE'
        AND age > 15
        AND lf.art_start_date IS NOT NULL
        AND why_null.client_id IS NULL;
END //
DELIMITER ;