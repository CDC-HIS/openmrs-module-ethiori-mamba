DROP FUNCTION IF EXISTS fn_get_ti_status;

DELIMITER //

CREATE FUNCTION fn_get_ti_status(TI_CLIENT_ID INT, START_DATE DATE,END_DATE DATE) RETURNS NVARCHAR(10)
                                                                                                  DETERMINISTIC
BEGIN
    DECLARE TI_STATUS NVARCHAR(10);
    DECLARE TI_RESULT NVARCHAR(10);

WITH TIFollowUp AS (
    SELECT follow_up.client_id, MIN(follow_up_date_followup_) AS FollowupDate
    FROM mamba_flat_encounter_follow_up follow_up
             JOIN mamba_flat_encounter_follow_up_1 follow_up_1 ON follow_up.encounter_id = follow_up_1.encounter_id
             JOIN mamba_flat_encounter_follow_up_2 follow_up_2 ON follow_up.encounter_id = follow_up_2.encounter_id
             JOIN mamba_flat_encounter_follow_up_3 follow_up_3 ON follow_up.encounter_id = follow_up_3.encounter_id
    WHERE follow_up_status IS NOT NULL
      AND art_antiretroviral_start_date IS NOT NULL
      AND transferred_in_check_this_for_all_t IS NOT NULL
      AND follow_up.client_id = TI_CLIENT_ID
    GROUP BY follow_up.client_id
)
SELECT * INTO TI_STATUS
FROM TIFollowUp
WHERE FollowupDate BETWEEN START_DATE AND END_DATE;


IF TI_STATUS IS NULL THEN
        SET TI_RESULT = 'NTI';
ELSE
        SET TI_RESULT = 'TI';
END IF;

RETURN TI_RESULT;
END //

DELIMITER ;