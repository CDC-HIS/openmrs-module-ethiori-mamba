DROP FUNCTION IF EXISTS fn_get_ti_status;

DELIMITER //

CREATE FUNCTION fn_get_ti_status(TI_CLIENT_ID INT, START_DATE DATE, END_DATE DATE) RETURNS NVARCHAR(10)
    DETERMINISTIC
BEGIN

    DECLARE TI_RESULT NVARCHAR(10);
    DECLARE min_date_val DATE;

    SET min_date_val = (
        SELECT MIN(follow_up_date_followup_) AS min_follow_up_date
        FROM mamba_flat_encounter_follow_up follow_up
                 LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                           ON follow_up.encounter_id = follow_up_1.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                           ON follow_up.encounter_id = follow_up_2.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                           ON follow_up.encounter_id = follow_up_3.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                           ON follow_up.encounter_id = follow_up_4.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5
                           ON follow_up.encounter_id = follow_up_5.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6
                           ON follow_up.encounter_id = follow_up_6.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7
                           ON follow_up.encounter_id = follow_up_7.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8
                           ON follow_up.encounter_id = follow_up_8.encounter_id
                 LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9
                           ON follow_up.encounter_id = follow_up_9.encounter_id
        WHERE follow_up_status IS NOT NULL
          AND art_antiretroviral_start_date IS NOT NULL
          AND transferred_in_check_this_for_all_t = 'Yes'
          AND follow_up.client_id = TI_CLIENT_ID
    );

    IF min_date_val BETWEEN START_DATE AND END_DATE THEN
        SET TI_RESULT = 'TI';
    ELSE
        SET TI_RESULT = 'NTI';
    END IF;

    RETURN TI_RESULT;

END //

DELIMITER ;