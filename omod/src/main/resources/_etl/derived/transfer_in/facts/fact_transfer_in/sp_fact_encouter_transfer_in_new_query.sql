DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_new_query;

CREATE PROCEDURE sp_fact_encounter_transfer_in_new_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN
    SELECT
        dim.client_id,
        dim.patient_name,
        dim.mrn,
        dim.uan,
        dim.current_age,
        dim.mobile_no,
        dim.date_of_birth,
        dim.sex,
        dim.state_province AS region,
        dim.county_district AS zone,
        dim.city_village AS woreda,
        fact.ti_status,
        fact.ti_date,
        fact.art_start_date,
        fact.treatment_end_date,
        fact.next_visit_date,
        fact.latest_followup_date,
        fact.latest_followup_status,
        fact.latest_regimen,
        fact.adherence
    FROM mamba_dim_client_transfer_in dim
             INNER JOIN mamba_fact_client_tranfer_in fact ON fact.client_id = dim.client_id
    WHERE fact.ti_date BETWEEN  CAST(REPORT_START_DATE as DATE) AND CAST(REPORT_END_DATE as DATE)
      and dim.mrn is not null and fact.latest_followup_status in ('Alive', 'Restart');
END //

DELIMITER ;