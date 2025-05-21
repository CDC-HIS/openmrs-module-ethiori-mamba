DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_curr_analysis_query;

CREATE PROCEDURE sp_fact_line_list_tx_curr_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE,
                                                          IN REPORT_TYPE VARCHAR(50))
BEGIN

    DECLARE tx_curr_base_query TEXT;
    DECLARE final_select_query TEXT;
    DECLARE filter_condition TEXT;
    DECLARE columns_list TEXT;
    SET session group_concat_max_len = 20000;

    -- Determine the WHERE clause filter condition based on REPORT_TYPE.
-- The conditions directly use columns from the f_result CTE instead of relying on a pre-calculated 'factor'.
    IF REPORT_TYPE = 'TX_CURR_THIS_MONTH' THEN
        -- Corresponds to factors: 'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK' , 'STILL ON CARE'
        SET filter_condition =
                ' factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'')';
        SET columns_list = 'patient_name as `patient name`, MRN , UAN , TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
       sex, regimen , follow_up_status_curr as `follow up status` , art_start_date_curr as `art start date` , adherence ,
       CASE WHEN in_prev_period THEN ''Counted'' ELSE '''' END   AS `Previous status`, pregnancy_status as `Pregnancy status`,
       nutritional_status_of_adult as `Nutritional status`, FollowUpDate_curr as `Follow up date`, next_visit_date as `Appointment date`,
       dsd_category as `Dsd category`';
    ELSEIF REPORT_TYPE = 'TX_CURR_LAST_MONTH' THEN
        -- Clients who were in TX_CURR in the previous reporting period.
        SET filter_condition = ' in_prev_period = 1';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_prev                                 as `follow up status`,
           art_start_date_prev                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_prev                                     as `Follow up date`,
           next_visit_date                                       as `Appointment date` ';
    ELSEIF REPORT_TYPE = 'TX_CURR_NEWLY_INCLUDED' THEN
        -- Corresponds to factors: 'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK' , 'TO/TI'
        SET filter_condition = ' factor in (''TRACED BACK'', ''RESTART'', ''TI'', ''TO/TI'', ''NEWLY STARTED'')';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           next_visit_date                                       as `Appointment date`,
           factor as `Added status` ';
    ELSEIF REPORT_TYPE = 'TX_CURR_EXCLUDED_THIS_MONTH' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED'
        SET filter_condition = ' factor in (''Transferred out'', ''Dead'', ''Loss to follow-up (LTFU)'', ''Ran away'', ''Stop all'', ''NOT UPDATED'')';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           next_visit_date                                       as `Appointment date`,
           factor as `Deduct follow up status` ';
    ELSEIF REPORT_TYPE = 'OTHER_OUTCOME' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP'
        SET filter_condition = ' in_prev_period = 1 AND factor in (''Transferred out'', ''Dead'', ''Loss to follow-up (LTFU)'', ''Ran away'', ''Stop all'')';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           next_visit_date_prev                                       as `Appointment date` ';
    ELSEIF REPORT_TYPE = 'NOT_UPDATED' THEN
        -- Corresponds to factor: 'NOT UPDATED'
        SET filter_condition = ' factor in (''NOT UPDATED'') ';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           next_visit_date                                       as `Appointment date` ';
    ELSEIF REPORT_TYPE = 'SUMMARY' THEN
        SET columns_list = ' ''Previous Month Tx_Current'' as `Label`,COUNT(*) as `Count` from tx_curr_analysis where in_prev_period=1
            UNION ALL
            select ''TO(-)'',COUNT(*) from tx_curr_analysis where factor in (''Transferred out'')
            UNION ALL
            select ''LOST(-)'',COUNT(*) from tx_curr_analysis where factor = (''Loss to follow-up (LTFU)'')
            UNION ALL
            select ''DROP(-)'',COUNT(*) from tx_curr_analysis where factor = (''Ran away'')
            UNION ALL
            select ''DEAD(-)'',COUNT(*) from tx_curr_analysis where factor = (''Dead'')
            UNION ALL
            select ''STOP(-)'',COUNT(*) from tx_curr_analysis where factor = (''Stop all'')
            UNION ALL
            select ''NOT UPDATED(-)'',COUNT(*) from tx_curr_analysis where factor = (''NOT UPDATED'')
            UNION ALL
            select ''TRACED BACK(+)'',COUNT(*) from tx_curr_analysis where factor = (''TRACED BACK'')
            UNION ALL
            select ''RESTART(+)'',COUNT(*) from tx_curr_analysis where factor = (''RESTART'')
            UNION ALL
            select ''TI(+)'',COUNT(*) from tx_curr_analysis where factor = (''TI'')
            UNION ALL
            select ''NEWLY INITIATED(+)'',COUNT(*) from tx_curr_analysis where factor = (''NEWLY STARTED'')
            UNION ALL
            select ''Current Month Tx_Current'',COUNT(*) from tx_curr_analysis where factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'')
            UNION ALL
            select ''Tx_Current net current increment'', (SELECT COUNT(*) FROM tx_curr_analysis WHERE factor IN (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'')) -
                (SELECT COUNT(*) FROM tx_curr_analysis WHERE in_prev_period = 1) ';
    ELSEIF REPORT_TYPE = 'ON_DSD' THEN
        SET filter_condition = ' dsd_category is not null ';
        SET columns_list = ' patient_name as `patient name`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           next_visit_date                                       as `Appointment date`,
           assessment_date as `enrollment date GC`,
           fn_gregorian_to_ethiopian_calendar(assessment_date,''D/M/Y'') as `enrollment date EC`,
           dsd_category as `latest DSD category` ';
    ELSE
        SET filter_condition = ' 1 = 1';
        SET columns_list = '   patient_name,
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
                               factor ';
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
                 assessment_date,
                 pregnancy_status,
                 nutritional_status_of_adult,
                 cd4_count,
                 visit_type
          from mamba_flat_encounter_follow_up follow_up
                   left join mamba_flat_encounter_follow_up_1 follow_up_1
                             on follow_up.encounter_id = follow_up_1.encounter_id
                   left join mamba_flat_encounter_follow_up_2 follow_up_2
                             on follow_up.encounter_id = follow_up_2.encounter_id
                   left join mamba_flat_encounter_follow_up_3 follow_up_3
                             on follow_up.encounter_id = follow_up_3.encounter_id
                   left join mamba_flat_encounter_follow_up_4 follow_up_4
                             on follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_latest_follow_up as
         (select client_id,
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
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
          from followup
          where follow_up_date <= ?),                                -- param 1: @end_date
     -- Latest follow up before reporting period
     latest_follow_up as
         (select *, fn_get_ti_status(client_id, ?, ?) AS TIStatus -- param 2: @start_date, param 3: @end_date
          from tmp_latest_follow_up
          where row_num = 1
            and (art_antiretroviral_start_date is not null and
                 art_antiretroviral_start_date <= ?)),               -- param 4: @end_date
     -- Latest curr follow up
     latest_curr_follow_up as (select *
                               from latest_follow_up
                               where follow_up_status in (''Alive'', ''Restart medication'')
                                 and treatment_end_date >= ? -- param 5: @end_date
     ),
     -- Previous follow up before reporting period
     tmp_previous_follow_up as
         (select client_id,
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
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
          from followup
          where follow_up_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 6: @start_date
            AND follow_up_status IS NOT NULL
            and art_antiretroviral_start_date is not null),
     -- Previous curr follow up before reporting period
     previous_curr_follow_up as
         (select *, fn_get_ti_status(client_id, ?, ?) AS TIStatus -- param 7: @start_date, param 8: @end_date
          from tmp_previous_follow_up
          where row_num = 1
            and follow_up_status in (''Alive'', ''Restart medication'')
            and art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 9: @start_date
            and treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)), -- param 10: @start_date
     tx_curr_factor as (select latest.client_id,
                                 latest.follow_up_date                as FollowUpDate_curr,
                                 latest.treatment_end_date            as art_dose_End_curr,
                                 latest.follow_up_status              as follow_up_status_curr,
                                 latest.art_antiretroviral_start_date as art_start_date_curr,
                                 latest.TIStatus                      as TIStatus_curr,
                                 previous.client_id is not null       as in_prev_period,
                                 previous.follow_up_date              as FollowUpDate_prev,
                                 previous.treatment_end_date          as art_dose_End_prev,
                                 previous.follow_up_status            as follow_up_status_prev,
                                 previous.next_visit_date             as next_visit_date_prev,
                                 previous.art_antiretroviral_start_date as art_start_date_prev,
                                 latest.regimen,
                                 latest.adherence,
                                 latest.pregnancy_status,
                                 latest.nutritional_status_of_adult,
                                 latest.next_visit_date,
                                 latest.dsd_category,
                                 latest.assessment_date,
                                 latest.cd4_count,
                                 latest.visit_type,
                                 case
                                     when
                                         latest.art_antiretroviral_start_date between ? and ? and -- param 11: @start_date, param 12: @end_date
                                         latest.follow_up_date <= ? and -- param 13: @end_date
                                         latest.treatment_end_date >= ? and -- param 14: @end_date
                                         latest.TIStatus = ''NTI'' and
                                         latest.follow_up_status in (''Alive'', ''Restart medication'')
                                         then ''NEWLY STARTED''
                                     when
                                         latest.follow_up_date <= ? and -- param 15: @end_date
                                         latest.treatment_end_date >= ? and -- param 16: @end_date
                                         latest.follow_up_status in (''Alive'', ''Restart medication'') AND
                                         previous.client_id is not null -- also in prev curr
                                         then ''STILL ON CARE''

                                     when latest.follow_up_date <= ? and
                                          latest.treatment_end_date >= ? -- param 17: @end_date, param 18: @end_date
                                         and latest.follow_up_status = ''Restart medication'' and
                                          previous.client_id is null -- not in prev
                                         then ''RESTART''
                                     when latest.follow_up_date <= ? and
                                          latest.treatment_end_date >= ? -- param 19: @end_date, param 20: @end_date
                                         and latest.follow_up_status in (''Alive'', ''Restart medication'')
                                         and previous.client_id is null and -- not in prev
                                          latest.TIStatus = ''TI'' then ''TI''

                                     when latest.follow_up_date <= ? and
                                          latest.treatment_end_date >= ? -- param 21: @end_date, param 22: @end_date
                                         and latest.TIStatus = ''NTI'' and latest.follow_up_status in (''Alive'')
                                         and previous.client_id is null -- not in prev
                                         then ''TRACED BACK'' --

                                     when latest.follow_up_date <= ? and
                                          latest.treatment_end_date >= ? -- param 23: @end_date, param 24: @end_date
                                         and latest.follow_up_status in (''Alive'', ''Restart medication'')
                                         and latest.follow_up_status = ''Transferred out''
                                         and previous.client_id is not null and -- TO in prev
                                          latest.TIStatus = ''TI'' then ''TO/TI'' -- TI in curr

-- subtract factor--
                                     when
                                         previous.client_id is not null And
                                         not (latest.follow_up_date <= ? and
                                              latest.treatment_end_date >=
                                              ?) AND -- param 25: @end_date, param 26: @end_date
                                         latest.follow_up_status in (''Alive'', ''Restart medication'')
                                         then ''NOT UPDATED''
                                     when previous.client_id is not null then latest.follow_up_status
                                     end                              as factor
                          from latest_follow_up latest
                             left join previous_curr_follow_up previous on latest.client_id = previous.client_id
                          ),
     tx_curr_analysis as (select dim_client.mrn as MRN,
                                 dim_client.uan as UAN,
                                 dim_client.patient_name,
                                 dim_client.date_of_birth,
                                 dim_client.sex,
                                 r.* -- Select all columns from the modified f_result
                          from tx_curr_factor r
                                   inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id
                          where r.factor is not null
                          ) ';

    -- Construct the final select query by concatenating the base query and the
    -- dynamically built SELECT and WHERE clause based on REPORT_TYPE.
    -- We select all relevant columns from tx_curr_analysis and apply the filter.
    IF REPORT_TYPE = 'SUMMARY' THEN
        SET final_select_query = CONCAT(' SELECT ',
                                        columns_list);
    ELSE
        SET final_select_query = CONCAT(' SELECT ',
                                        columns_list,
                                        ' FROM tx_curr_analysis WHERE ', filter_condition);
    END IF;


    SET @sql = CONCAT(tx_curr_base_query, final_select_query);

    PREPARE stmt FROM @sql;
    SET @end_date = REPORT_END_DATE;
    SET @start_date = REPORT_START_DATE;
    IF REPORT_TYPE = 'SUMMARY' THEN
        EXECUTE stmt USING @end_date, @start_date, @end_date, @end_date, @end_date, @start_date, @start_date, @end_date, @start_date, @start_date, @start_date, @end_date,
@end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date;
    ELSE
        EXECUTE stmt USING @end_date, @start_date, @end_date, @end_date, @end_date, @start_date, @start_date, @end_date, @start_date, @start_date, @start_date, @end_date,
@end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date, @end_date;
    END IF;

    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;

-- param 1: @end_date
-- param 2: @start_date, param 3: @end_date
-- param 4: @end_date
-- param 5: @end_date
-- param 6: @start_date
-- param 7: @start_date, param 8: @end_date
-- param 9: @start_date
-- param 10: @start_date
-- param 11: @start_date, param 12: @end_date
-- param 13: @end_date
-- param 14: @end_date
-- param 15: @end_date
-- param 16: @end_date
-- param 17: @end_date, param 18: @end_date
-- param 19: @end_date, param 20: @end_date
-- param 21: @end_date, param 22: @end_date
-- param 23: @end_date, param 24: @end_date
-- param 25: @end_date, param 26: @end_date
