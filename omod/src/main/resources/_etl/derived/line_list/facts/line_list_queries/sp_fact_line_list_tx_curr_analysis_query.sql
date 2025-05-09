DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_tx_curr_analysis_query;

CREATE PROCEDURE sp_fact_line_list_tx_curr_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE, IN REPORT_TYPE BOOLEAN)
BEGIN

    DECLARE tx_curr_analysis_query TEXT;
    DECLARE group_query TEXT;
    DECLARE group_condition TEXT;
    SET session group_concat_max_len = 20000;

    IF REPORT_TYPE = 'TX_CURR_THIS_MONTH' THEN
        SET group_condition = ' current_month_tx_Curr = 1';
    ELSEIF REPORT_TYPE = 'TX_CURR_LAST_MONTH' THEN
        SET group_condition = ' last_month_tx_Curr = 1';
    ELSEIF REPORT_TYPE = 'TX_CURR_NEWLY_INCLUDED' THEN
        SET group_condition = ' newly_included_this_month = 1';
    ELSEIF REPORT_TYPE = 'TX_CURR_EXCLUDED_THIS_MONTH' THEN
        SET group_condition = ' excluded_from_tx_curr_this_month = 1';
    ELSEIF REPORT_TYPE = 'OTHER_OUTCOME' THEN
        SET group_condition = ' other_outcome_this_month = 1';
    ELSEIF REPORT_TYPE = 'NOT_UPDATED' THEN
        SET group_condition = ' not_updated = 1';
    ELSEIF REPORT_TYPE = 'SUMMARY' THEN
        SET group_condition = ' 1 = 1';
    ELSEIF REPORT_TYPE = 'ON_DSD' THEN
        SET group_condition = ' 1 = 1';
    END IF;

    SET tx_curr_analysis_query = 'with followup as
         (select follow_up.encounter_id,
                 follow_up_date_followup_ as follow_up_date,
                 follow_up.client_id,
                 follow_up_status,
                 art_antiretroviral_start_date,
                 treatment_end_date
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
          where follow_up_date <= ?),
     -- latest follow up is alive or restart and art start date is < report end date
     latest_follow_up as
         (select *, fn_get_ti_status(client_id, ?, ?) AS TIStatus
          from tmp_latest_follow_up
          where row_num = 1
            and follow_up_status in (''Alive'', ''Restart medication'')
            and (art_antiretroviral_start_date is not null and art_antiretroviral_start_date <= ?)),
     -- Latest follow up before reporting period
     tmp_previous_follow_up as
         (select followup.encounter_id,
                 followup.client_id,
                 follow_up_date,
                 follow_up_status,
                 treatment_end_date,
                 art_antiretroviral_start_date,
                 fn_get_ti_status(followup.client_id, ?,
                                  ?)                                                             AS TIStatus,
                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
          from followup
          where follow_up_date <= DATE_ADD(?, INTERVAL -1 DAY)
            AND follow_up_status IS NOT NULL
            and art_antiretroviral_start_date is not null),


     previous_follow_up as
         (select *
          from tmp_previous_follow_up
          where row_num = 1
            and follow_up_status in (''Alive'', ''Restart medication'')
            and art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY)
            and treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)),

     f_result as
         (select latest.client_id,
                 latest.follow_up_date                                      as FollowUpDate_curr,
                 latest.treatment_end_date                                  as art_dose_End_curr,
                 latest.follow_up_status                                    as follow_up_status_curr,
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
                     end                                                    as factor,

                 case when previous.client_id is not null then 1 else 0 end as prev_curr


          from latest_follow_up latest
                   left join previous_follow_up previous on latest.client_id = previous.client_id),
tx_curr_analysis as (
    select dim_client.mrn as MRN,
           dim_client.patient_name,
           dim_client.date_of_birth,
           dim_client.sex,
           r.FollowUpDate_curr,
           r.art_dose_End_curr,
           r.follow_up_status_curr,
           r.FollowUpDate_prev,
           r.art_dose_End_prev,
           r.follow_up_status_prev,
           r.factor,
           case
               when
                   r.factor in (''NEWLY STARTED'', ''STILL ON CARE'', ''RESTART'', ''TI'', ''TRACED BACK'')
                   then 1
               else 0
               end        as current_month_tx_Curr,
           r.prev_curr    as last_month_tx_Curr,
           case
               when
                   r.factor in (''TRACED BACK'', ''RESTART'', ''TI'', ''TO/TI'', ''NEWLY STARTED'')
                   then 1
               else 0
               end        as newly_included_this_month,
           case
               when
                   r.factor in (''TO'', ''DEAD'', ''LOST'', ''DROP'', ''STOP'', ''NOT UPDATED'')
                   then 1
               else 0
               end        as excluded_from_tx_curr_this_month,
           case
               when
                   r.factor in (''TO'', ''DEAD'', ''LOST'', ''DROP'', ''STOP'')
                   then 1
               else 0
               end        as other_outcome_this_month,
           case
               when
                   r.factor in (''NOT UPDATED'')
                   then 1
               else 0
               end        as not_updated

    from f_result r
             inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id
) ';
    SET group_query = CONCAT(tx_curr_analysis_query,
                             ' select * from tx_curr_analysis WHERE ',
                             group_condition);
    SET @sql = group_query;
    PREPARE stmt FROM @sql;
    SET @start_date = REPORT_START_DATE;
    SET @end_date = REPORT_END_DATE;
    EXECUTE stmt USING @end_date, @end_date, @end_date;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;