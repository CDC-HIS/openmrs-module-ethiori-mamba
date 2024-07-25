DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_encounter_transfer_in_new_query;

CREATE Procedure sp_fact_encounter_transfer_in_new_query(IN start_date Date, IN end_date Date)
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
WHERE fact.ti_date BETWEEN  CAST(start_date as DATE) AND CAST(end_date as DATE)
  and dim.mrn is not null and fact.latest_followup_status in ('Alive', 'Restart');
END //
DELIMITER ;