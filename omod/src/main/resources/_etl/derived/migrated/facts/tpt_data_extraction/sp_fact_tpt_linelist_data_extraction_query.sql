DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_tpt_linelist_data_extraction_query;

CREATE PROCEDURE  sp_fact_tpt_linelist_data_extraction_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN
    WITH LatestFollowUp AS (
        SELECT
            client_id,
            hiv_confirmed_date,
            art_start_date,
            followup_date,
            enrollment_date,
            weight_in_kg,
            pregnancy_status,
            regimen,
            arv_dose_days,
            follow_up_status,
            anitiretroviral_adherence_level,
            next_visit_date,
            dsd_category,
            tpt_start_date,
            tpt_completed_date,
            tpt_discontinued_date,
            tuberculosis_treatment_end_date,
            date_viral_load_results_received,
            viral_load_test_status,
            ti,
            treatment_end_date,
            tb_prophylaxis_type,
            current_who_hiv_stage,
            cd4_count,
            cotrimoxazole_prophylaxis_start_dat,
            cotrimoxazole_prophylaxis_stop_date,
            patient_diagnosed_with_active_tuber,
            diagnosis_date,
            tuberculosis_drug_treatment_start_d,
            date_active_tbrx_completed,
            fluconazole_start_date,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY followup_date DESC) AS rn_desc
        FROM mamba_fact_art_follow_up
        WHERE followup_date <= CAST('2024-08-31' AS DATE)
          AND art_start_date <= CAST('2024-08-31' AS DATE)
          AND regimen IS NOT NULL
    )
    SELECT
        dim.sex as Sex, 
        lf.weight_in_kg as Weight,
        dim.current_age as Age,
        lf.tpt_start_date as TPT_Start_Date,
        lf.tpt_completed_date as TPT_Completed_Date,
        lf.tb_prophylaxis_type,
        lf.hiv_confirmed_date as HIV_Confirmed_Date,
        lf.art_start_date as ART_Start_Date,
        lf.followup_date as Followup_Date,
        lf.ti,
        lf.arv_dose_days as ART_Dose_Days,
        lf.next_visit_date as Next_Visit_Date,
        lf.follow_up_status as Followup_Status,
        lf.treatment_end_date as Dose_End_Date,
        -- PatientGUID (Do we need this field)
        lf.current_who_hiv_stage as WHO_Stage,
        lf.cd4_count,
        lf.cotrimoxazole_prophylaxis_start_dat,
        lf.cotrimoxazole_prophylaxis_stop_date,
        -- tb_specimen_type (Doesn't find in Ethiohri)
        lf.patient_diagnosed_with_active_tuber,
        lf.diagnosis_date,
        lf.tuberculosis_drug_treatment_start_d,
        lf.date_active_tbrx_completed as TbTx_CompletedDate,
        lf.fluconazole_start_date as FluconazoleStartDate,
       -- fluco completed date
        NOW()
    FROM LatestFollowUp lf
             JOIN mamba_dim_client_art_follow_up dim ON lf.client_id = dim.client_id
    WHERE lf.rn_desc = 1
      AND lf.follow_up_status = 'Alive';
END //

DELIMITER ;