DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_tx_curr_analysis_query;

CREATE PROCEDURE  sp_fact_tx_curr_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    with followup as
             (select follow_up.encounter_id,
                     follow_up_date_followup_ as follow_up_date,
                     follow_up.client_id,
                     follow_up_status,
                     art_antiretroviral_start_date,
                     treatment_end_date
              from mamba_flat_encounter_follow_up follow_up
                       join mamba_flat_encounter_follow_up_1 follow_up_1
                            on follow_up.encounter_id = follow_up_1.encounter_id
                       join mamba_flat_encounter_follow_up_2 follow_up_2
                            on follow_up.encounter_id = follow_up_2.encounter_id),
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
              where follow_up_date <= REPORT_END_DATE),

         latest_follow_up as
             (select *, fn_get_ti_status(client_id, REPORT_END_DATE, REPORT_START_DATE) AS TIStatus
              from tmp_latest_follow_up
              where row_num = 1),

         tmp_previous_follow_up as
             (select client_id,
                     encounter_id,
                     follow_up_date                                                                             as followupdate,
                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
              from followup ff
              where follow_up_date <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY)
                AND follow_up_status IS NOT NULL
                and art_antiretroviral_start_date is not null),


         previous_follow_up as
             (select * from tmp_previous_follow_up where row_num = 1),

         followup_curr_prev as
             (select followup.encounter_id,
                     followup.client_id,
                     follow_up_date,
                     follow_up_status,
                     treatment_end_date,
                     art_antiretroviral_start_date,
                     fn_get_ti_status(followup.client_id, REPORT_END_DATE, REPORT_START_DATE) AS TIStatus
              from followup
                       inner join previous_follow_up on
                  previous_follow_up.encounter_id = followup.encounter_id and
                  followup.treatment_end_date >= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY) and
                  follow_up_status in ('Alive', 'Restart medication') and
                  art_antiretroviral_start_date <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY))
            ,
         f_result as
             (select latest.client_id,
                     latest.follow_up_date                                    as FollowUpDate_curr,
                     latest.treatment_end_date                                as art_dose_End_curr,
                     latest.follow_up_status                                  as follow_up_status_curr,
                     previous.follow_up_date                                    as FollowUpDate_prev,
                     previous.treatment_end_date                                as art_dose_End_prev,
                     previous.follow_up_status                                     follow_up_status_prev,
                     case
                         when
                             latest.art_antiretroviral_start_date between REPORT_START_DATE and REPORT_END_DATE and
                             latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE and
                             latest.TIStatus = 'NTI' and latest.follow_up_status in ('Alive', 'Restart medication')
                             then 'NEWLY STARTED'
                         when
                             latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE and
                             latest.follow_up_status in ('Alive', 'Restart medication') AND
                             previous.client_id is not null -- also in prev curr
                             then 'STILL ON CARE'

                         when latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE
                             and latest.follow_up_status = 'Restart medication' and
                              previous.client_id is null -- not in prev
                             then 'RESTART'
                         when latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE
                             and latest.follow_up_status in ('Alive', 'Restart medication')
                             and previous.client_id is null and -- not in prev
                              latest.TIStatus = 'TI' then 'TI'

                         when latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE
                             and latest.TIStatus = 'NTI' and latest.follow_up_status in ('Alive')
                             and previous.client_id is null -- not in prev
                             then 'TRACED BACK' --

                         when latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE
                             and latest.follow_up_status in ('Alive', 'Restart medication')
                             and latest.follow_up_status = 'Transferred out'
                             and previous.client_id is not null and -- TO in prev
                              latest.TIStatus = 'TI' then 'TO/TI' -- TI in curr

                     -- subtract factor--

                         when
                             previous.client_id is not null And
                             not (latest.follow_up_date <= REPORT_END_DATE and latest.treatment_end_date >= REPORT_END_DATE) AND
                             latest.follow_up_status in ('Alive', 'Restart medication')
                             then 'NOT UPDATED'


                         when previous.client_id is not null then latest.follow_up_status
                         end                                              as factor,

                     case when previous.client_id is not null then 1 else 0 end as prev_curr


              from latest_follow_up latest
                       left join followup_curr_prev previous on latest.client_id = previous.client_id)


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
                   r.factor in ('NEWLY STARTED', 'STILL ON CARE', 'RESTART', 'TI', 'TRACED BACK')
                   then 1
               else 0
               end        as current_month_tx_Curr,
           case
               when
                   r.factor in ('TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED')
                   then 1
               else 0
               end        as excluded_from_tx_curr_this_month,
           case
               when
                   r.factor in ('TRACED BACK', 'RESTART', 'TI', 'TO/TI', 'NEWLY STARTED')
                   then 1
               else 0
               end        as newly_included_this_month,
           r.prev_curr

    from f_result r
             inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id
    where r.prev_curr = 1;

END //

DELIMITER ;