DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_curr_analysis_query;

CREATE PROCEDURE sp_fact_line_list_tx_curr_analysis_query(
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE,
    IN REPORT_TYPE VARCHAR(50)
)
BEGIN

    DECLARE tx_curr_base_query TEXT;
    DECLARE final_select_query TEXT;
    DECLARE filter_condition TEXT;
    DECLARE columns_list TEXT;
SET session group_concat_max_len = 20000;

-- Determine the WHERE clause filter condition based on REPORT_TYPE.
IF REPORT_TYPE = 'TX_CURR_THIS_MONTH' THEN
        -- Corresponds to factors:'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK', 'STILL ON CARE', 'TO/TI'
        SET filter_condition =
                ' factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'',''TO/TI'' ,''TI'', ''TRACED BACK'')';

        SET columns_list = '
            patient_name as `Patient Name`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as `Current_Age`,
            TIMESTAMPDIFF(YEAR, date_of_birth, FollowUpDate_prev) as `Previous_Age`,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            CASE
                WHEN in_prev_period THEN ''Counted''
                ELSE ''''
            END AS `Previous status`,
            pregnancy_status as `Pregnancy status`,
            nutritional_status_of_adult as `Nutritional status`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`,
            dsd_category as `Dsd category`
        ';

    ELSEIF REPORT_TYPE = 'TX_CURR_LAST_MONTH' THEN

        -- Clients who were in TX_CURR in the previous reporting period.
        SET filter_condition = ' in_prev_period = 1';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_prev as `follow up status`,
            art_start_date_prev as `art start date`,
            art_start_date_prev as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_prev as `Follow up date`,
            FollowUpDate_prev as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`
        ';

    ELSEIF REPORT_TYPE = 'TX_CURR_NEWLY_INCLUDED' THEN

        -- Corresponds to factors:
        -- 'TRACED BACK', 'RESTART', 'TI', 'TO/TI', 'NEWLY STARTED'
        SET filter_condition =
                ' factor in (''TRACED BACK'', ''RESTART'', ''TI'', ''TO/TI'', ''NEWLY STARTED'')';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`,
            factor as `Added status`
        ';

    ELSEIF REPORT_TYPE = 'TX_CURR_EXCLUDED_THIS_MONTH' THEN

        -- Corresponds to factors:
        -- 'TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED'
        SET filter_condition =
                ' factor in (''Transferred out'', ''Dead'', ''Loss to follow-up (LTFU)'', ''Ran away'', ''Stop all'', ''NOT UPDATED'')';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`,
            factor as `Deduct follow up status`
        ';

    ELSEIF REPORT_TYPE = 'OTHER_OUTCOME' THEN

        -- Corresponds to factors:
        -- 'TO', 'DEAD', 'LOST', 'DROP', 'STOP'
        SET filter_condition =
                ' in_prev_period = 1
                  AND factor in (
                      ''Transferred out'',
                      ''Dead'',
                      ''Loss to follow-up (LTFU)'',
                      ''Ran away'',
                      ''Stop all''
                  )';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date_prev as `Appointment date`,
            next_visit_date_prev as `Appointment date EC.`
        ';

    ELSEIF REPORT_TYPE = 'NOT_UPDATED' THEN

        -- Corresponds to factor:
        -- 'NOT UPDATED'
        SET filter_condition =
                ' factor in (''NOT UPDATED'') ';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            age_out as `Age Out`,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`
        ';

    ELSEIF REPORT_TYPE = 'SUMMARY' THEN

        SET columns_list = '
            ''Previous Month Tx_Current'' as `Label`,
            COUNT(*) as `Count`
            FROM tx_curr_analysis
            WHERE in_prev_period = 1

            UNION ALL

            SELECT ''TO(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor in (''Transferred out'')

            UNION ALL

            SELECT ''LOST(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''Loss to follow-up (LTFU)'')

            UNION ALL

            SELECT ''DROP(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''Ran away'')

            UNION ALL

            SELECT ''DEAD(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''Dead'')

            UNION ALL

            SELECT ''STOP(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''Stop all'')

            UNION ALL

            SELECT ''NOT UPDATED(-)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''NOT UPDATED'')

            UNION ALL

            SELECT ''TRACED BACK(+)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''TRACED BACK'')

            UNION ALL

            SELECT ''TO/TI(+)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''TO/TI'')

            UNION ALL

            SELECT ''RESTART(+)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''RESTART'')

            UNION ALL

            SELECT ''TI(+)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''TI'')

            UNION ALL

            SELECT ''NEWLY INITIATED(+)'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor = (''NEWLY STARTED'')

            UNION ALL

            SELECT ''Current Month Tx_Current'', COUNT(*)
            FROM tx_curr_analysis
            WHERE factor in (
                ''NEWLY STARTED'',
                ''STILL ON CARE'',
                ''RESTART'',
                ''TI'',
                ''TRACED BACK'',
                ''TO/TI''
            )

            UNION ALL

            SELECT
                ''Tx_Current net current increment'',
                (
                    SELECT COUNT(*)
                    FROM tx_curr_analysis
                    WHERE factor IN (
                        ''NEWLY STARTED'',
                        ''STILL ON CARE'',
                        ''RESTART'',
                        ''TI'',
                        ''TRACED BACK'',
                        ''TO/TI''
                    )
                ) -
                (
                    SELECT COUNT(*)
                    FROM tx_curr_analysis
                    WHERE in_prev_period = 1
                )
        ';

    ELSEIF REPORT_TYPE = 'ON_DSD' THEN

        SET filter_condition = ' dsd_category is not null ';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            sex,
            regimen,
            follow_up_status_curr as `follow up status`,
            art_start_date_curr as `art start date`,
            art_start_date_curr as `art start date EC.`,
            adherence,
            visit_type `schedule type`,
            FollowUpDate_curr as `Follow up date`,
            FollowUpDate_curr as `Follow up date EC.`,
            next_visit_date as `Appointment date`,
            next_visit_date as `Appointment date EC.`,
            assessment_date as `enrollment date`,
            assessment_date as `enrollment date EC.`,
            dsd_category as `latest DSD category`
        ';

ELSE

        SET filter_condition = ' 1 = 1';

        SET columns_list = '
            patient_name as `Patient Name`,
            patient_uuid as `UUID`,
            MRN,
            UAN,
            TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
            sex,
            FollowUpDate_curr,
            art_dose_End_curr,
            follow_up_status_curr,
            art_start_date_curr,
            TIStatus_curr,
            in_prev_period,
            FollowUpDate_prev,
            art_dose_End_prev,
            follow_up_status_prev,
            in_prev_period,
            factor
        ';

END IF;


    SET tx_curr_base_query = '
        WITH followup AS
        (
            SELECT
                follow_up.encounter_id,
                follow_up_date_followup_ AS follow_up_date,
                follow_up.client_id,
                follow_up_status,
                art_antiretroviral_start_date,
                treatment_end_date,
                regimen,
                adherence,
                next_visit_date,
                dsd_category,
                assessment_date,
                pregnancy_status,
                nutritional_status_of_adult,
                cd4_count,
                visit_type

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
        ),

        tmp_latest_follow_up AS
        (
            SELECT
                client_id,
                encounter_id,
                follow_up_date,
                treatment_end_date,
                follow_up_status,
                art_antiretroviral_start_date,
                regimen,
                adherence,
                pregnancy_status,
                nutritional_status_of_adult,
                next_visit_date,
                dsd_category,
                assessment_date,
                cd4_count,
                visit_type,

                ROW_NUMBER() OVER (
                    PARTITION BY client_id
                    ORDER BY follow_up_date DESC, encounter_id DESC
                ) AS row_num

            FROM followup

            WHERE follow_up_date <= ?
        ),

        -- Latest follow up before reporting period
        latest_follow_up AS
        (
            SELECT
                *,
                fn_get_ti_status(client_id, ?, ?) AS TIStatus

            FROM tmp_latest_follow_up

            WHERE row_num = 1
              AND (
                    art_antiretroviral_start_date IS NOT NULL
                    AND art_antiretroviral_start_date <= ?
              )
        ),

        -- Latest curr follow up
        latest_curr_follow_up AS
        (
            SELECT *
            FROM latest_follow_up

            WHERE follow_up_status IN (
                ''Alive'',
                ''Restart medication''
            )
            AND treatment_end_date >= ?
        ),

        -- Previous follow up before reporting period
        tmp_previous_follow_up AS
        (
            SELECT
                client_id,
                encounter_id,
                follow_up_date,
                treatment_end_date,
                follow_up_status,
                art_antiretroviral_start_date,
                regimen,
                adherence,
                pregnancy_status,
                nutritional_status_of_adult,
                next_visit_date,
                dsd_category,
                assessment_date,
                cd4_count,
                visit_type,

                ROW_NUMBER() OVER (
                    PARTITION BY client_id
                    ORDER BY follow_up_date DESC, encounter_id DESC
                ) AS row_num

            FROM followup

            WHERE follow_up_date <= DATE_ADD(?, INTERVAL -1 DAY)
              AND follow_up_status IS NOT NULL
              AND art_antiretroviral_start_date IS NOT NULL
        ),

        previous_follow_up AS
        (
            SELECT *
            FROM tmp_previous_follow_up
            WHERE row_num = 1
        ),

        -- Previous curr follow up before reporting period
        previous_curr_follow_up AS
        (
            SELECT *
            FROM tmp_previous_follow_up

            WHERE row_num = 1
              AND follow_up_status IN (
                    ''Alive'',
                    ''Restart medication''
              )
              AND art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY)
              AND treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)
        ),

        tx_curr_factor AS
        (
            SELECT
                latest_all.client_id,
                latest_curr.client_id AS curr_client,

                latest_all.follow_up_date AS FollowUpDate_curr,
                latest_all.treatment_end_date AS art_dose_End_curr,

                CASE latest_all.follow_up_status
                    WHEN ''Alive'' THEN ''Alive on ART''
                    WHEN ''Restart medication'' THEN ''Restart''
                    WHEN ''Transferred out'' THEN ''TO''
                    WHEN ''Stop all'' THEN ''Stop''
                    WHEN ''Loss to follow-up (LTFU)'' THEN ''Lost''
                    WHEN ''Ran away'' THEN ''Drop''
                END AS follow_up_status_curr,

                latest_all.art_antiretroviral_start_date AS art_start_date_curr,
                latest_all.TIStatus AS TIStatus_curr,

                previous_curr.client_id IS NOT NULL AS in_prev_period,

                previous_curr.follow_up_date AS FollowUpDate_prev,
                previous_curr.treatment_end_date AS art_dose_End_prev,

                CASE previous_curr.follow_up_status
                    WHEN ''Alive'' THEN ''Alive on ART''
                    WHEN ''Restart medication'' THEN ''Restart''
                    WHEN ''Transferred out'' THEN ''TO''
                    WHEN ''Stop all'' THEN ''Stop''
                    WHEN ''Loss to follow-up (LTFU)'' THEN ''Lost''
                    WHEN ''Ran away'' THEN ''Drop''
                END AS follow_up_status_prev,

                previous_curr.next_visit_date AS next_visit_date_prev,
                previous_curr.art_antiretroviral_start_date AS art_start_date_prev,

                latest_all.regimen,
                latest_all.adherence,
                latest_all.pregnancy_status,
                latest_all.nutritional_status_of_adult,
                latest_all.next_visit_date,

                latest_curr.dsd_category,
                latest_all.assessment_date,
                latest_all.cd4_count,
                latest_all.visit_type,

                CASE
                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_curr.art_antiretroviral_start_date BETWEEN ? AND ?
                         AND latest_curr.TIStatus = ''NTI''
                         AND latest_curr.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                    THEN ''NEWLY STARTED''

                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_curr.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                         AND previous_curr.client_id IS NOT NULL
                    THEN ''STILL ON CARE''

                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_all.follow_up_status = ''Restart medication''
                         AND previous_curr.client_id IS NULL
                    THEN ''RESTART''

                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_curr.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                         AND latest_curr.TIStatus = ''TI''
                    THEN ''TI''

                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_curr.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                         AND previous_all.follow_up_status = ''Transferred out''
                    THEN ''TO/TI''

                    WHEN latest_curr.client_id IS NOT NULL
                         AND latest_curr.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                    THEN ''TRACED BACK''

                    -- subtract factor
                    WHEN previous_curr.client_id IS NOT NULL
                         AND latest_all.treatment_end_date < ?
                         AND latest_all.follow_up_status IN (
                             ''Alive'',
                             ''Restart medication''
                         )
                    THEN ''NOT UPDATED''

                    WHEN previous_curr.client_id IS NOT NULL
                    THEN latest_all.follow_up_status

                END AS factor

            FROM latest_follow_up latest_all

            LEFT JOIN previous_curr_follow_up previous_curr
                ON latest_all.client_id = previous_curr.client_id

            LEFT JOIN latest_curr_follow_up latest_curr
                ON latest_all.client_id = latest_curr.client_id

            LEFT JOIN previous_follow_up previous_all
                ON latest_all.client_id = previous_all.client_id
        ),

        tx_curr_analysis AS
        (
            SELECT
                CAST(dim_client.mrn AS CHAR(20)) AS MRN,
                dim_client.uan AS UAN,
                dim_client.patient_name,
                dim_client.date_of_birth,
                patient_uuid,
                dim_client.sex,

                CASE
                    WHEN
                        TIMESTAMPDIFF(
                            YEAR,
                            date_of_birth,
                            art_start_date_curr
                        ) <= 15
                        AND TIMESTAMPDIFF(
                            YEAR,
                            date_of_birth,
                            COALESCE(?, CURDATE())
                        ) >= 15
                        AND DATE_ADD(
                            date_of_birth,
                            INTERVAL 15 YEAR
                        ) BETWEEN ? AND ?
                    THEN ''YES''
                    ELSE ''NO''
                END AS age_out,

                r.*

            FROM tx_curr_factor r

            INNER JOIN mamba_dim_client dim_client
                ON r.client_id = dim_client.client_id

            WHERE r.factor IS NOT NULL
        )
    ';


    IF REPORT_TYPE = 'SUMMARY' THEN

        SET final_select_query = CONCAT(
            ' SELECT ',
            columns_list
        );

ELSE

        SET final_select_query = CONCAT(
            ' SELECT ',
            columns_list,
            ' FROM tx_curr_analysis WHERE ',
            filter_condition
        );

END IF;


    SET @sql = CONCAT(
        tx_curr_base_query,
        final_select_query
    );

PREPARE stmt FROM @sql;

SET @end_date = REPORT_END_DATE;
    SET @start_date = REPORT_START_DATE;


    IF REPORT_TYPE = 'SUMMARY' THEN

        EXECUTE stmt USING
            @end_date,
            @start_date,
            @end_date,
            @end_date,
            @end_date,
            @start_date,
            @start_date,
            @start_date,
            @start_date,
            @end_date,
            @end_date,
            @end_date,
            @start_date,
            @end_date;

ELSE

        EXECUTE stmt USING
            @end_date,
            @start_date,
            @end_date,
            @end_date,
            @end_date,
            @start_date,
            @start_date,
            @start_date,
            @start_date,
            @end_date,
            @end_date,
            @end_date,
            @start_date,
            @end_date,
            @end_date;

END IF;


DEALLOCATE PREPARE stmt;

END //

DELIMITER ;