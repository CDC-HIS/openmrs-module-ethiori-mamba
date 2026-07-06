DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_dqi_cached_query;

-- Fast DQI&U Line List backed by the ETL-materialized mamba_fact_client table
-- (populated as of the latest ETL run). Unlike sp_fact_line_list_dqi_query
-- (which recomputes everything live from the flat encounter tables for an
-- arbitrary REPORT_END_DATE), this procedure returns the full cached,
-- pre-computed snapshot with no filtering or parameters.
CREATE PROCEDURE sp_fact_line_list_dqi_cached_query()
BEGIN
    SELECT client_id,
           patient_uuid,
           mrn,
           patient_name,
           sex,
           birthdate,
           age,

           uan,
           phrh_code,
           ncd_code,
           icd_number,
           ict_number,

           registration_date,
           art_start_date,
           months_on_art,
           next_appointment_date,
           hiv_confirmed_date,
           transfer_in_date,
           transfer_in_date_ec,

           region,
           zone,
           woreda,
           kebele,
           house_number,
           mobile_phone,
           address_completeness,

           current_status,
           current_regimen,
           regimen_dose,
           regimen_line,
           tx_curr_end_date,
           nutritional_status,
           pregnancy_status,
           pmtct_status,
           family_planning_method,
           who_stage,

           last_visit_date,
           days_overdue,

           last_vl_date,
           last_vl_result,
           is_suppressed,
           vl_status,
           vl_eligibility_date,
           vl_sent_date,
           vl_sent_date_ec,
           vl_received_date,
           vl_received_date_ec,

           tpt_status,
           tpt_start_date,
           tpt_start_date_ec,
           tpt_completed_date,
           tpt_completed_date_ec,
           tpt_discontinued_date,
           tpt_discontinued_date_ec,

           active_tb_diagnosis_date,
           active_tb_diagnosis_date_ec,
           tb_treatment_start_date,
           tb_treatment_start_date_ec,
           tb_treatment_discontinued_date,
           tb_treatment_discontinued_date_ec,
           tb_treatment_completed_date,
           tb_treatment_completed_date_ec,
           tb_treatment_rx_status,

           dsd_category,
           ict_screening_status,
           ncd_screening_status,
           ncd_last_screening_date,
           next_ncd_screening_date,
           ncd_screening_eligibility_status,
           ncd_screening_eligibility_date,
           ncd_screening_reason,
           cxca_screening_status,
           next_cca_screening_date,
           systolic_blood_pressure,
           diastolic_blood_pressure,
           target_population,

           breast_feeding_status,
           disclosure_stage,
           cd4_result,
           visitect_cd4_result,

           pmtct_booking_date,
           pmtct_booking_date_ec,
           pmtct_discharge_date,
           pmtct_discharge_date_ec,
           pmtct_enrollment_status,

           advanced_hiv_disease,

           -- DQI corrected status columns
           vl_status_dqi,
           tpt_status_dqi,
           ict_screening_status_dqi,
           ncd_screening_status_dqi,
           cxca_screening_status_dqi

    FROM mamba_fact_client
    ORDER BY patient_name;

END //

DELIMITER ;
