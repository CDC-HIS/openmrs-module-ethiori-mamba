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
on follow_up.encounter_id = follow_up_4.encounter_id) ,
-- all followups before end_date
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
-- latest follow up is alive or restart and art start date is < report end date
latest_follow_up as
(select *, fn_get_ti_status(client_id, ?, ?) AS TIStatus -- param 2: @start_date, param 3: @end_date
from tmp_latest_follow_up
where row_num = 1
--  and follow_up_status in ('Alive', 'Restart medication')
and (art_antiretroviral_start_date is not null and
art_antiretroviral_start_date <= ?)),               -- param 4: @end_date
latest_curr_follow_up as ( select * from latest_follow_up where follow_up_status in ('Alive', 'Restart medication')),
-- Latest follow up before reporting period
tmp_previous_follow_up as
(select followup.encounter_id,
    followup.client_id,
    follow_up_date,
    follow_up_status,
    treatment_end_date,
    art_antiretroviral_start_date,
    fn_get_ti_status(followup.client_id,
            ?, -- param 6: @end_date, param 7: @start_date (assuming original mapping)
    ?)                                                                        AS TIStatus,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC,encounter_id DESC ) AS row_num
from followup
where follow_up_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 5: @start_date
AND follow_up_status IS NOT NULL
and art_antiretroviral_start_date is not null
),

previous_follow_up as
(select *
    from tmp_previous_follow_up
where row_num = 1
and follow_up_status in ('Alive', 'Restart medication')
and art_antiretroviral_start_date <= DATE_ADD(?, INTERVAL -1 DAY) -- param 8: @start_date
and treatment_end_date >= DATE_ADD(?, INTERVAL -1 DAY)), -- param 9: @start_date

-- f_result now includes columns needed for filtering instead of the 'factor' calculation
f_result as
(select latest.client_id,
    latest.follow_up_date                as FollowUpDate_curr,
    latest.treatment_end_date            as art_dose_End_curr,
    latest.follow_up_status              as follow_up_status_curr,
    latest.art_antiretroviral_start_date as art_start_date_curr,
    latest.TIStatus                      as TIStatus_curr,
    previous.client_id is not null       as in_prev_period,
    previous.follow_up_date              as FollowUpDate_prev,
    previous.treatment_end_date          as art_dose_End_prev,
    previous.follow_up_status               follow_up_status_prev,
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
latest.art_antiretroviral_start_date between ? and ? and
    latest.follow_up_date <= ? and
        latest.treatment_end_date >= ? and
            latest.TIStatus = 'NTI' and latest.follow_up_status in ('Alive', 'Restart medication')
then 'NEWLY STARTED'
when
latest.follow_up_date <= ? and
    latest.treatment_end_date >= ? and
        latest.follow_up_status in ('Alive', 'Restart medication') AND
previous.client_id is not null -- also in prev curr
then 'STILL ON CARE'

when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
    and latest.follow_up_status = 'Restart medication' and
previous.client_id is null -- not in prev
then 'RESTART'
when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
    and latest.follow_up_status in ('Alive', 'Restart medication')
and previous.client_id is null and -- not in prev
latest.TIStatus = 'TI' then 'TI'

when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
    and latest.TIStatus = 'NTI' and latest.follow_up_status in ('Alive')
and previous.client_id is null -- not in prev
then 'TRACED BACK' --

when latest.follow_up_date <= ? and latest.treatment_end_date >= ?
    and latest.follow_up_status in ('Alive', 'Restart medication')
and latest.follow_up_status = 'Transferred out'
and previous.client_id is not null and -- TO in prev
latest.TIStatus = 'TI' then 'TO/TI' -- TI in curr

-- subtract factor--

when
previous.client_id is not null And
not (latest.follow_up_date <= ? and
    latest.treatment_end_date >= ?) AND
latest.follow_up_status in ('Alive', 'Restart medication')
then 'NOT UPDATED'


when previous.client_id is not null then latest.follow_up_status
end                              as factor
from latest_curr_follow_up latest
left join previous_follow_up previous on latest.client_id = previous.client_id
join latest_follow_up latest_status on latest_status.client_id = previous.client_id
),
-- Join with dim_client and select all relevant columns from f_result
tx_curr_analysis as (select dim_client.mrn as MRN,
    dim_client.uan as UAN,
    dim_client.patient_name,
    dim_client.date_of_birth,
    dim_client.sex,
r.* -- Select all columns from the modified f_result
from f_result r
inner join mamba_dim_client dim_client on r.client_id = dim_client.client_id)

select * from tx_curr_analysis;