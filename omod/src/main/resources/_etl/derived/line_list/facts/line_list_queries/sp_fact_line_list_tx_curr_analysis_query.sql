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
                ' factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'',''TO/TI'' ,''TI'', ''TRACED BACK'')';
        SET columns_list = 'patient_name as `Patient Name`, MRN , UAN , TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age, age_out as `Age Out`,
       sex, regimen , follow_up_status_curr as `follow up status` , art_start_date_curr as `art start date`, art_start_date_curr as `art start date EC.` , adherence ,
       CASE WHEN in_prev_period THEN ''Counted'' ELSE '''' END   AS `Previous status`, pregnancy_status as `Pregnancy status`,
       nutritional_status_of_adult as `Nutritional status`, FollowUpDate_curr as `Follow up date`, FollowUpDate_curr as `Follow up date EC.`, next_visit_date as `Appointment date`, next_visit_date as `Appointment date EC.`,
       dsd_category as `Dsd category`';
    ELSEIF REPORT_TYPE = 'TX_CURR_LAST_MONTH' THEN
        -- Clients who were in TX_CURR in the previous reporting period.
        SET filter_condition = ' in_prev_period = 1';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           age_out as `Age Out`,
           sex,
           regimen,
           follow_up_status_prev                                 as `follow up status`,
           art_start_date_prev                                   as `art start date`,
           art_start_date_prev                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_prev                                     as `Follow up date`,
           FollowUpDate_prev                                     as `Follow up date EC.`,
           next_visit_date                                       as `Appointment date`,
           next_visit_date                                       as `Appointment date EC.` ';
    ELSEIF REPORT_TYPE = 'TX_CURR_NEWLY_INCLUDED' THEN
        -- Corresponds to factors: 'NEWLY STARTED', 'RESTART', 'TI', 'TRACED BACK' , 'TO/TI'
        SET filter_condition = ' factor in (''TRACED BACK'', ''RESTART'', ''TI'', ''TO/TI'', ''NEWLY STARTED'')';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           age_out as `Age Out`,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           art_start_date_curr                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           FollowUpDate_curr                                     as `Follow up date EC.`,
           next_visit_date                                       as `Appointment date`,
           next_visit_date                                       as `Appointment date EC.`,
           factor as `Added status` ';
    ELSEIF REPORT_TYPE = 'TX_CURR_EXCLUDED_THIS_MONTH' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED'
        SET filter_condition = ' factor in (''Transferred out'', ''Dead'', ''Loss to follow-up (LTFU)'', ''Ran away'', ''Stop all'', ''NOT UPDATED'')';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           age_out as `Age Out`,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           art_start_date_curr                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           FollowUpDate_curr                                     as `Follow up date EC.`,
           next_visit_date                                       as `Appointment date`,
           next_visit_date                                       as `Appointment date EC.`,
           factor as `Deduct follow up status` ';
    ELSEIF REPORT_TYPE = 'OTHER_OUTCOME' THEN
        -- Corresponds to factors: 'TO', 'DEAD', 'LOST', 'DROP', 'STOP'
        SET filter_condition = ' in_prev_period = 1 AND factor in (''Transferred out'', ''Dead'', ''Loss to follow-up (LTFU)'', ''Ran away'', ''Stop all'')';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           age_out as `Age Out`,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           art_start_date_curr                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           FollowUpDate_curr                                     as `Follow up date EC.`,
           next_visit_date_prev                                       as `Appointment date`,
           next_visit_date_prev                                       as `Appointment date EC.` ';
    ELSEIF REPORT_TYPE = 'NOT_UPDATED' THEN
        -- Corresponds to factor: 'NOT UPDATED'
        SET filter_condition = ' factor in (''NOT UPDATED'') ';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           age_out as `Age Out`,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           art_start_date_curr                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           FollowUpDate_curr                                     as `Follow up date EC.`,
           next_visit_date                                       as `Appointment date`,
           next_visit_date                                       as `Appointment date EC.` ';
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
            select ''TO/TI(+)'',COUNT(*) from tx_curr_analysis where factor = (''TO/TI'')
            UNION ALL
            select ''RESTART(+)'',COUNT(*) from tx_curr_analysis where factor = (''RESTART'')
            UNION ALL
            select ''TI(+)'',COUNT(*) from tx_curr_analysis where factor = (''TI'')
            UNION ALL
            select ''NEWLY INITIATED(+)'',COUNT(*) from tx_curr_analysis where factor = (''NEWLY STARTED'')
            UNION ALL
            select ''Current Month Tx_Current'',COUNT(*) from tx_curr_analysis where factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'',''TO/TI'')
            UNION ALL
            select ''Tx_Current net current increment'', (SELECT COUNT(*) FROM tx_curr_analysis WHERE factor IN (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'',''TO/TI'')) -
                (SELECT COUNT(*) FROM tx_curr_analysis WHERE in_prev_period = 1) ';
    ELSEIF REPORT_TYPE = 'ON_DSD' THEN
        SET filter_condition = ' dsd_category is not null ';
        SET columns_list = ' patient_name as `Patient Name`,
           patient_uuid                             as `UUID`,
           MRN,
           UAN,
           TIMESTAMPDIFF(YEAR, date_of_birth, ?) as age,
           sex,
           regimen,
           follow_up_status_curr                                 as `follow up status`,
           art_start_date_curr                                   as `art start date`,
           art_start_date_curr                                   as `art start date EC.`,
           adherence,
           visit_type `schedule type`,
           FollowUpDate_curr                                     as `Follow up date`,
           FollowUpDate_curr                                     as `Follow up date EC.`,
           next_visit_date                                       as `Appointment date`,
           next_visit_date                                       as `Appointment date EC.`,
           assessment_date as `enrollment date`,
           assessment_date as `enrollment date EC.`,
           dsd_category as `latest DSD category` ';
    ELSE
        SET filter_condition = ' 1 = 1';
        SET columns_list = '   patient_name as `Patient Name`,
                               patient_uuid                             as `UUID`,
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
                                         ON follow_up.encounter_id = follow_up_9.encounter_id),
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
         (select *,
                 fn_get_ti_status(client_id, ?, ?) AS TIStatus -- param 2: @start_date, param 3: @end_date
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
     previous_follow_up as ( select * from tmp_previous_follow_up where row_num=1),
     -- Previous curr follow up before reporting period
     previous_curr_follow_up as
         (select *
          from tmp_previous_follow_up
          where row_num = 1
            and follow_up_status in (''Alive'', ''Restart medication'')
            and art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 7: @start_date
            and treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)), -- param 8: @start_date
     tx_curr_factor as (select latest_all.client_id,
                               latest_curr.client_id as curr_client,
                               latest_all.follow_up_date                   as FollowUpDate_curr,
                               latest_all.treatment_end_date               as art_dose_End_curr,
                                CASE latest_all.follow_up_status
                               WHEN ''Alive'' THEN ''Alive on ART''
                               WHEN ''Restart medication'' THEN ''Restart''
                               WHEN ''Transferred out'' THEN ''TO''
                               WHEN ''Stop all'' THEN ''Stop''
                               WHEN ''Loss to follow-up (LTFU)'' THEN ''Lost''
                               WHEN ''Ran away'' THEN ''Drop''
                               END                as follow_up_status_curr,
                               latest_all.art_antiretroviral_start_date    as art_start_date_curr,
                               latest_all.TIStatus                         as TIStatus_curr,
                               previous_curr.client_id is not null         as in_prev_period,
                               previous_curr.follow_up_date                as FollowUpDate_prev,
                               previous_curr.treatment_end_date            as art_dose_End_prev,
                               CASE previous_curr.follow_up_status
                               WHEN ''Alive'' THEN ''Alive on ART''
                               WHEN ''Restart medication'' THEN ''Restart''
                               WHEN ''Transferred out'' THEN ''TO''
                               WHEN ''Stop all'' THEN ''Stop''
                               WHEN ''Loss to follow-up (LTFU)'' THEN ''Lost''
                               WHEN ''Ran away'' THEN ''Drop''
                               END             as follow_up_status_prev,
                               previous_curr.next_visit_date               as next_visit_date_prev,
                               previous_curr.art_antiretroviral_start_date as art_start_date_prev,
                               latest_all.regimen,
                               latest_all.adherence,
                               latest_all.pregnancy_status,
                               latest_all.nutritional_status_of_adult,
                               latest_all.next_visit_date,
                               latest_all.dsd_category,
                               latest_all.assessment_date,
                               latest_all.cd4_count,
                               latest_all.visit_type,
                               case
                                   when
                                       latest_curr.client_id is not null
                                           and
                                       latest_curr.art_antiretroviral_start_date between ? and ? -- param 9: @start_date, param 10: @end_date
                                           and latest_curr.TIStatus = ''NTI''
                                           and latest_curr.follow_up_status in (''Alive'', ''Restart medication'')
                                       then ''NEWLY STARTED''
                                   when
                                       latest_curr.client_id is not null
                                           and latest_curr.follow_up_status in (''Alive'', ''Restart medication'')
                                           and previous_curr.client_id is not null -- also in prev curr
                                       then ''STILL ON CARE''

                                   when latest_curr.client_id is not null
                                       and latest_all.follow_up_status = ''Restart medication''
                                       and previous_curr.client_id is null -- not in prev
                                       then ''RESTART''
                                   when latest_curr.client_id is not null
                                       and latest_curr.follow_up_status in (''Alive'', ''Restart medication'')
                                       and latest_curr.TIStatus = ''TI'' then ''TI''

                                   when latest_curr.client_id is not null
                                       and latest_curr.follow_up_status in (''Alive'', ''Restart medication'')
                                       and previous_all.follow_up_status = ''Transferred out''
                                       then ''TO/TI'' -- TI in curr

                                   when latest_curr.client_id is not null
                                       and latest_curr.follow_up_status in (''Alive'', ''Restart medication'')
                                       then ''TRACED BACK''
-- subtract factor--
                                   when
                                       previous_curr.client_id is not null
                                           and latest_all.treatment_end_date <
                                               ? -- param 11: @end_date
                                           and latest_all.follow_up_status in (''Alive'', ''Restart medication'')
                                       then ''NOT UPDATED''
                                   when previous_curr.client_id is not null then latest_all.follow_up_status
                                   end                                     as factor
                        from latest_follow_up latest_all
                                 left join previous_curr_follow_up previous_curr
                                           on latest_all.client_id = previous_curr.client_id
                                 left join latest_curr_follow_up latest_curr
                                           on latest_all.client_id = latest_curr.client_id
                                 left join previous_follow_up previous_all
                                           on latest_all.client_id = previous_all.client_id
                        ),
     tx_curr_analysis as (select CAST(dim_client.mrn AS CHAR(20)) as MRN,
                                 dim_client.uan as UAN,
                                 dim_client.patient_name,
                                 dim_client.date_of_birth,
                                 patient_uuid,
                                 dim_client.sex,
                                 CASE WHEN (TIMESTAMPDIFF(YEAR, date_of_birth, art_start_date_curr) <= 15) AND
                                           TIMESTAMPDIFF(YEAR, date_of_birth, COALESCE(?,CURDATE())) >= 15 AND
                                           DATE_ADD(date_of_birth, INTERVAL 15 YEAR) BETWEEN ? AND ? THEN ''YES''
                                      ELSE ''NO'' END AS age_out,
                                 r.*
                          from tx_curr_factor r
                                   inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id
                          where r.factor is not null
                          ) ';

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
        EXECUTE stmt USING @end_date, @start_date, @end_date, @end_date, @end_date, @start_date, @start_date, @start_date, @start_date, @end_date, @end_date, @end_date, @start_date, @end_date;
    ELSE
        EXECUTE stmt USING @end_date, @start_date, @end_date, @end_date, @end_date, @start_date, @start_date, @start_date, @start_date, @end_date, @end_date, @end_date, @start_date, @end_date, @end_date;
    END IF;

    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;

