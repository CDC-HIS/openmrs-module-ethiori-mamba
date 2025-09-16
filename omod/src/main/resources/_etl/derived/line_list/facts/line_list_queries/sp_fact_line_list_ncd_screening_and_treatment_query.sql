DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_ncd_screening_and_treatment_query;

CREATE PROCEDURE sp_fact_line_list_ncd_screening_and_treatment_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE, IN PHRH_CODE VARCHAR(255))
BEGIN
	WITH provided_coupons AS (
        SELECT
            mps.client_id,
            mps.followup_date,
            mps.coupon_1  AS coupon_id,
            mpf.final_hiv_test_result AS conventional_result
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_flat_encounter_phrh_followup mpf
			ON mps.client_id = mpf.client_id
		WHERE mps.coupon_1 is not null

        UNION

        SELECT
            mps.client_id,
            mps.followup_date,
            mps.coupon_2  AS coupon_id,
            mpf.final_hiv_test_result AS conventional_result
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_flat_encounter_phrh_followup mpf
			ON mps.client_id = mpf.client_id
		WHERE mps.coupon_2 != ''

        UNION

        SELECT
            mps.client_id,
            mps.followup_date,
            mps.coupon_3  AS coupon_id,
            mpf.final_hiv_test_result AS conventional_result
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_flat_encounter_phrh_followup mpf
			ON mps.client_id = mpf.client_id
		WHERE mps.coupon_3 != ''

        UNION

        SELECT
            mps.client_id,
            mps.followup_date,
            mps.coupon_4  AS coupon_id,
            mpf.final_hiv_test_result AS conventional_result
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_flat_encounter_phrh_followup mpf
			ON mps.client_id = mpf.client_id
		WHERE mps.coupon_4 != ''

        UNION

        SELECT
            mps.client_id,
            mps.followup_date,
            mps.coupon_5  AS coupon_id,
            mpf.final_hiv_test_result AS conventional_result
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_flat_encounter_phrh_followup mpf
			ON mps.client_id = mpf.client_id
		WHERE mps.coupon_5 != ''
    ),
    provided_date AS (
		SELECT p.*
			FROM provided_coupons p
            WHERE REPORT_START_DATE is not null and REPORT_END_DATE is not null and (p.followup_date BETWEEN REPORT_START_DATE and REPORT_END_DATE) or
			(REPORT_START_DATE is not null and REPORT_END_DATE is null and (p.followup_date >= REPORT_START_DATE)) or
			(REPORT_START_DATE is null and REPORT_END_DATE is not null and (p.followup_date <= REPORT_END_DATE)) or
            (REPORT_START_DATE is null and REPORT_END_DATE is null)
    ),
    provided_and_retuned AS (
		SELECT
			pc.client_id,
            pc.followup_date,
            pc.coupon_id,
            pc.conventional_result,
            mpi.identifier AS phrh_code_returning_coupon,
            fn_gregorian_to_ethiopian_calendar(mpf.followup_date, 'Y-M-D') AS date_returned_ec,
            mpf.followup_date as date_returned,
            mpf.etb_paid_for_recruiter,
            mpf.final_hiv_test_result AS conventional_result_returning
		FROM provided_date pc
        LEFT JOIN mamba_flat_encounter_phrh_followup mpf
			ON mpf.coupon_id=pc.coupon_id
		LEFT JOIN mamba_dim_patient_identifier mpi
			ON mpf.client_id = mpi.patient_id and
            mpi.identifier_type = (SELECT patient_identifier_type_id FROM mamba_dim_patient_identifier_type WHERE name='PHRH')
    ),
    provided_and_retuned_with_phrh AS (
        SELECT
            mps.*,
            mpi.identifier AS phrh,
            fn_gregorian_to_ethiopian_calendar(mps.followup_date, 'Y-M-D') AS date_provided_ec,
            mps.followup_date AS date_provided
        FROM provided_and_retuned mps
        INNER JOIN mamba_dim_patient_identifier mpi
            ON mps.client_id = mpi.patient_id
        INNER JOIN mamba_dim_patient_identifier_type mpit
            ON mpi.identifier_type = mpit.patient_identifier_type_id  and mpit.name = 'PHRH'
		WHERE (PHRH_CODE is not null and mpi.identifier = PHRH_CODE) or PHRH_CODE is null
    )
    SELECT
		prp.phrh AS 'PHRH Code Provided Coupon',
        prp.coupon_id AS 'Coupon ID',
        prp.date_provided_ec AS 'Date Provided (EC)',
        prp.date_provided AS 'Date Provided (GC)',
        prp.conventional_result AS 'Con Result Provided Coupon',
        prp.phrh_code_returning_coupon AS 'PHRH Code Returning Coupon',
        prp.date_returned_ec AS 'Date Returned (EC)',
        prp.date_returned AS 'Date Returned (GC)',
        prp.etb_paid_for_recruiter AS 'ETB paid for Recruiter',
        prp.conventional_result_returning AS 'Con Test Result Returning Coupon'
    FROM provided_and_retuned_with_phrh prp;
END //

DELIMITER ;