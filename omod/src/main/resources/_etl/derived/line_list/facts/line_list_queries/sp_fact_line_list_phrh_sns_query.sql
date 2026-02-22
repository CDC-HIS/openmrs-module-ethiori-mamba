DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_phrh_sns_query;

CREATE PROCEDURE sp_fact_line_list_phrh_sns_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE, IN PHRH_CODE VARCHAR(255))
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
		WHERE mps.coupon_1 != ''

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
            pc.coupon_id,
            pc.conventional_result,
            mpi.identifier AS phrh_code_returning_coupon,
            mpf.followup_date AS date_returned_ec,
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
    sns AS (
        SELECT
            mps.client_id AS client_id,
            mpi.identifier AS phrh,
            mps.followup_date AS date_provided_ec,
            mps.followup_date AS date_provided
        FROM mamba_flat_encounter_phrh_sns mps
        INNER JOIN mamba_dim_patient_identifier mpi
            ON mps.client_id = mpi.patient_id
        INNER JOIN mamba_dim_patient_identifier_type mpit
            ON mpi.identifier_type = mpit.patient_identifier_type_id  and mpit.name = 'PHRH'
		WHERE (PHRH_CODE is not null and mpi.identifier = PHRH_CODE) or PHRH_CODE is null
    )
    SELECT
		s.phrh AS 'PHRH Code Provided Coupon',
        p.coupon_id AS 'Coupon ID',
        s.date_provided_ec AS 'Date Provided EC.',
        s.date_provided AS 'Date Provided (GC)',
        p.conventional_result AS 'Contact Result Provided Coupon',
        p.phrh_code_returning_coupon AS 'PHRH Code Returning Coupon',
        p.date_returned_ec AS 'Date Returned EC.',
        p.date_returned AS 'Date Returned (GC)',
        p.etb_paid_for_recruiter AS 'ETB paid for Recruiter',
        p.conventional_result_returning AS 'Contact Test Result Returning Coupon'
    FROM provided_and_retuned p
    JOIN sns s ON p.client_id = s.client_id;
END //

DELIMITER ;
