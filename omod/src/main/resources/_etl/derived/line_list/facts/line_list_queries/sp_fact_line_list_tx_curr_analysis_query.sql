DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_curr_analysis_query;

CREATE PROCEDURE sp_fact_line_list_tx_curr_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE, IN REPORT_TYPE VARCHAR(50))
BEGIN

    DECLARE tx_curr_base_query TEXT;
    DECLARE final_select_query TEXT;
    DECLARE filter_condition TEXT;
SET session group_concat_max_len = 20000;

-- Determine the WHERE clause filter condition based on REPORT_TYPE.
-- The conditions directly use columns from the f_result CTE instead of relying on a pre-calculated 'factor'.
IF REPORT_TYPE = 'TX_CURR_THIS_MONTH' THEN
        -- Corresponds to factors: 'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK' , 'STILL ON CARE'
        SET filter_condition = ' factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'')
                               ';
    ELSEIF REPORT_TYPE = 'TX_CURR_LAST_MONTH' THEN
        -- Clients who were in TX_CURR in the previous reporting period.
        SET filter_condition = ' in_prev_period = 1';
    ELSEIF REPORT_TYPE = 'TX_CURR_NEWLY_INCLUDED' THEN
        -- Corresponds to factors: 'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK' , 'TO/TI'
        SET filter_condition = ' factor in (''TRACED BACK'', ''RESTART'', ''TI'', ''TO/TI'', ''NEWLY STARTED'')
                               ';
    ELSEIF REPORT_TYPE = 'TX_CURR_EXCLUDED_THIS_MONTH' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED'
        SET filter_condition = ' factor in (''TO'', ''DEAD'', ''LOST'', ''DROP'', ''STOP'', ''NOT UPDATED'')
                               ';
    ELSEIF REPORT_TYPE = 'OTHER_OUTCOME' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP'
        SET filter_condition = ' in_prev_period = 1 AND factor in (''TO'', ''DEAD'', ''LOST'', ''DROP'', ''STOP'')';
    ELSEIF REPORT_TYPE = 'NOT_UPDATED' THEN
        -- Corresponds to factor: 'NOT UPDATED'
        SET filter_condition = ' factor in (''NOT UPDATED'') ';
    ELSEIF REPORT_TYPE = 'SUMMARY' THEN
        SET filter_condition = ' 1 = 1';
    ELSEIF REPORT_TYPE = 'ON_DSD' THEN
        SET filter_condition = ' 1 = 1';
ELSE
        SET filter_condition = ' 1 = 0';
END IF;

    SET tx_curr_base_query = '
        with followup as
             (select follow_up.encounter_id,
                     follow_up_date_followup_ as follow_up_date,
                     follow_up.client_id,
                     follow_up_status,
                     art_antiretroviral_start_date,
                     treatment_end_date,
                     regimen,
                     adherence,
                     next_visit_date,
                     dsd_category,
                     assessment_date
              from mamba_flat_encounter_follow_up follow_up
                       left join mamba_flat_encounter_follow_up_1 follow_up_1
                                 on follow_up.encounter_id = follow_up_1.encounter_id
                       left join mamba_flat_encounter_follow_up_2 follow_up_2
                                 on follow_up.encounter_id = follow_up_2.encounter_id
                       left join mamba_flat_encounter_follow_up_3 follow_up_3
                                 on follow_up.encounter_id = follow_up_3.encounter_id
                       left join mamba_flat_encounter_follow_up_4 follow_up_4
                                 on follow_up.encounter_id = follow_up_4.encounter_id),
         -- all followups before end_date
         tmp_latest_follow_up as
             (select client_id,
                     encounter_id,
                     follow_up_date,
                     treatment_end_date,
                     follow_up_status,
                     art_antiretroviral_start_date,
                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
              from followup
              where follow_up_date <= ?), -- param 1: @end_date
         -- latest follow up is alive or restart and art start date is < report end date
         latest_follow_up as
             (select *, fn_get_ti_status(client_id, ?, ?) AS TIStatus -- param 2: @start_date, param 3: @end_date
              from tmp_latest_follow_up
              where row_num = 1
                and follow_up_status in (''Alive'', ''Restart medication'')
                and (art_antiretroviral_start_date is not null and art_antiretroviral_start_date <= ?)), -- param 4: @end_date
         -- Latest follow up before reporting period
         tmp_previous_follow_up as
             (select followup.encounter_id,
                     followup.client_id,
                     follow_up_date,
                     follow_up_status,
                     treatment_end_date,
                     art_antiretroviral_start_date,
                     fn_get_ti_status(followup.client_id, ?, -- param 6: @end_date, param 7: @start_date (assuming original mapping)
                                      ?)                                                             AS TIStatus,
                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
              from followup
              where follow_up_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 5: @start_date
                AND follow_up_status IS NOT NULL
                and art_antiretroviral_start_date is not null),

         previous_follow_up as
             (select *
              from tmp_previous_follow_up
              where row_num = 1
                and follow_up_status in (''Alive'', ''Restart medication'')
                and art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 8: @start_date
                and treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)), -- param 9: @start_date

         -- f_result now includes columns needed for filtering instead of the ''factor'' calculation
         f_result as
             (select latest.client_id,
                     latest.follow_up_date                                      as FollowUpDate_curr,
                     latest.treatment_end_date                                  as art_dose_End_curr,
                     latest.follow_up_status                                    as follow_up_status_curr,
                     latest.art_antiretroviral_start_date                       as art_start_date_curr,
                     latest.TIStatus                                            as TIStatus_curr,
                     previous.client_id is not null                             as in_prev_period,
                     previous.follow_up_date                                    as FollowUpDate_prev,
                     previous.treatment_end_date                                as art_dose_End_prev,
                     previous.follow_up_status                                     follow_up_status_prev,
                     case
                     when
                         latest.art_antiretroviral_start_date between ? and ? and
                         latest.follow_up_date <= ? and
                         latest.treatment_end_date >= ? and
                         latest.TIStatus = ''NTI'' and latest.follow_up_status in (''Alive'', ''Restart medication'')
                         then ''NEWLY STARTED''
                     when
                         latest.follow_up_date <= ? and
                         latest.treatment_end_date >= ? and
                         latest.follow_up_status in (''Alive'', ''Restart medication'') AND
                         previous.client_id is not null -- also in prev curr
                         then ''STILL ON CARE''

                     when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
                         and latest.follow_up_status = ''Restart medication'' and
                          previous.client_id is null -- not in prev
                         then ''RESTART''
                     when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
                         and latest.follow_up_status in (''Alive'', ''Restart medication'')
                         and previous.client_id is null and -- not in prev
                          latest.TIStatus = ''TI'' then ''TI''

                     when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
                         and latest.TIStatus = ''NTI'' and latest.follow_up_status in (''Alive'')
                         and previous.client_id is null -- not in prev
                         then ''TRACED BACK'' --

                     when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
                         and latest.follow_up_status in (''Alive'', ''Restart medication'')
                         and latest.follow_up_status = ''Transferred out''
                         and previous.client_id is not null and -- TO in prev
                          latest.TIStatus = ''TI'' then ''TO/TI'' -- TI in curr

                 -- subtract factor--

                     when
                         previous.client_id is not null And
                         not (latest.follow_up_date <= ? and
                              latest.treatment_end_date >= ?) AND
                         latest.follow_up_status in (''Alive'', ''Restart medication'')
                         then ''NOT UPDATED''


                     when previous.client_id is not null then latest.follow_up_status
                     end                                                    as factor
              from latest_follow_up latest
                       left join previous_follow_up previous on latest.client_id = previous.client_id),
        -- Join with dim_client and select all relevant columns from f_result
        tx_curr_analysis as (
            select dim_client.mrn as MRN,
                   dim_client.uan as UAN,
                   dim_client.patient_name,
                   dim_client.date_of_birth,
                   dim_client.sex,
                   r.* -- Select all columns from the modified f_result
            from f_result r
                     inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id
        ) ';

    -- Construct the final select query by concatenating the base query and the
    -- dynamically built SELECT and WHERE clause based on REPORT_TYPE.
    -- We select all relevant columns from tx_curr_analysis and apply the filter.
    SET final_select_query = CONCAT(' SELECT
                                       patient_name,
                                       MRN,
                                       UAN,
                                       date_of_birth,
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
                                FROM tx_curr_analysis WHERE ', filter_condition);


    SET @sql = CONCAT(tx_curr_base_query, final_select_query);

    PREPARE stmt FROM @sql;
    SET @end_date = REPORT_END_DATE;
    SET @start_date = REPORT_START_DATE;
    EXECUTE stmt USING @end_date, @start_date, @end_date, @end_date, @start_date, @end_date, @start_date, @start_date, @start_date, @start_date
    , @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date;

    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
