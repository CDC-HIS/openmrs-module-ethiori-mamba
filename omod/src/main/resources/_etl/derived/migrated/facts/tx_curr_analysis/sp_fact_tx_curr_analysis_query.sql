DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_vl_eligibility_query;

CREATE PROCEDURE  sp_fact_tx_curr_analysis_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN

    with followup as
             (select follow_up.encounter_id,
                     follow_up_1.follow_up_date_followup_,
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
         followup_all as
             (select *, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date_followup_ DESC) AS RN
              from followup
              where follow_up_date_followup_ <= REPORT_END_DATE),

         followup_all_dedup as
             (select *, fn_get_ti_status(client_id, REPORT_END_DATE, REPORT_START_DATE) AS TIStatus from followup_all where RN = 1),

         followup_not_null_prev as
             (select client_id, max(follow_up_date_followup_) as followupdate
              from followup ff
              where follow_up_date_followup_ <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY)
                AND (NOT (follow_up_status IS NULL))
                and art_antiretroviral_start_date = 1
              group by client_id),


         followup_not_null_dedup_prev as
             (select max(encounter_id) as maxid
              from followup ff
                       inner join followup_not_null_prev ff_nn on ff.follow_up_date_followup_ = ff_nn.followupdate
                  and ff.client_id = ff_nn.client_id
              where ff.follow_up_date_followup_ <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY)
                AND (NOT (follow_up_status IS NULL))
              group by ff.client_id, ff.follow_up_date_followup_),

         followup_curr_prev as
             (select encounter_id,
                     client_id,
                     follow_up_date_followup_,
                     follow_up_status,
                     treatment_end_date,
                     art_antiretroviral_start_date,
                     fn_get_ti_status(client_id, REPORT_END_DATE, REPORT_START_DATE) AS TIStatus
              from followup ff
                       inner join followup_not_null_dedup_prev ff_curr on
                  ff_curr.maxid = ff.encounter_id and ff.treatment_end_date >= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY) and
                  follow_up_status in (5, 6) and art_antiretroviral_start_date <= DATE_ADD(REPORT_START_DATE, INTERVAL -1 DAY))
            ,
         f_result as
             (select ff.client_id,
                     ff.follow_up_date_followup_                                      as FollowUpDate_curr,
                     ff.treatment_end_date                                      as art_dose_End_curr,
                     ff.follow_up_status                                  as follow_up_status_curr,
                     fp.follow_up_date_followup_                                      as FollowUpDate_prev,
                     fp.treatment_end_date                                      as art_dose_End_prev,
                     fp.follow_up_status                                     follow_up_status_prev,
                     case
                         when
                             ff.art_antiretroviral_start_date between REPORT_START_DATE and REPORT_END_DATE and
                             ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE and
                             ff.TIStatus = 'NTI' and ff.follow_up_status in (5, 6)
                             then 'NEWLY STARTED'
                         when
                             ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE and
                             ff.follow_up_status in (5, 6) AND
                             fp.client_id is not null -- also in prev curr
                             then 'STILL ON CARE'

                         when ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE
                             and ff.follow_up_status = 6 and
                              fp.client_id is null -- not in prev
                             then 'RESTART'
                         when ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE
                             and ff.follow_up_status in (5, 6)
                             and fp.client_id is null and -- not in prev
                              ff.TIStatus = 'TI' then 'TI'

                         when ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE
                             and ff.TIStatus = 'NTI' and ff.follow_up_status in (5)
                             and fp.client_id is null -- not in prev
                             then 'TRACED BACK' --

                         when ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE
                             and ff.follow_up_status in (5, 6)
                             and ff.follow_up_status = 0
                             and fp.client_id is not null and -- TO in prev
                              ff.TIStatus = 'TI' then 'TO/TI' -- TI in curr

                     -- subtract factor--

                         when
                             fp.client_id is not null And
                             not (ff.follow_up_date_followup_ <= REPORT_END_DATE and ff.treatment_end_date >= REPORT_END_DATE) AND
                             ff.follow_up_status in (5, 6)
                             then 'NOT UPDATED'


                         when ff.follow_up_status = 0 and fp.client_id is not null then 'TO'
                         when ff.follow_up_status = 1 and fp.client_id is not null then 'Stop'
                         when ff.follow_up_status = 2 and fp.client_id is not null then 'Lost'
                         when ff.follow_up_status = 3 and fp.client_id is not null then 'Droppped'
                         when ff.follow_up_status = 4 and fp.client_id is not null then 'Dead'
                         end                                              as factor,

                     case when fp.client_id is not null then 1 else 0 end as prev_curr


              from followup_all_dedup ff
                       left join followup_curr_prev fp on ff.client_id = fp.client_id)


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
               end       as current_month_tx_Curr,
           case
               when
                   r.factor in ('TO', 'DEAD', 'LOST', 'DROP', 'STOP', 'NOT UPDATED')
                   then 1
               else 0
               end       as excluded_from_tx_curr_this_month,
           case
               when
                   r.factor in ('TRACED BACK', 'RESTART', 'TI', 'TO/TI', 'NEWLY STARTED')
                   then 1
               else 0
               end       as newly_included_this_month,
           r.prev_curr

    from f_result r
             inner join mamba_dim_client_art_follow_up dim_client on r.client_id = dim_client.client_id
    where r.factor is not null
       or r.prev_curr = 1;

END //

DELIMITER ;