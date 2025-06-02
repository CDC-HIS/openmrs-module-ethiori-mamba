WITH FollowUp as (
    select follow_up.client_id,
           follow_up.encounter_id,
           date_viral_load_results_received AS viral_load_perform_date,
           viral_load_received_,
           follow_up_status,
           follow_up_date_followup_         AS follow_up_date,
           art_antiretroviral_start_date       art_start_date,
           viral_load_test_status,
           hiv_viral_load                   AS viral_load_count,
           COALESCE(
                   at_3436_weeks_of_gestation,
                   viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                   viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                   every_six_months_until_mtct_ends,
                   six_months_after_the_first_viral_load_test_at_postnatal_peri,
                   three_months_after_delivery,
                   at_the_first_antenatal_care_visit,
                   annual_viral_load_test,
                   second_viral_load_test_at_12_months_post_art,
                   first_viral_load_test_at_6_months_or_longer_post_art,
                   first_viral_load_test_at_3_months_or_longer_post_art
           )                                AS routine_viral_load_test_indication,
           COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                    suspected_antiretroviral_failure
           )                                AS targeted_viral_load_test_indication,
           viral_load_test_indication,
           pregnancy_status,
           currently_breastfeeding_child    AS breastfeeding_status,
           antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
           regimen,
           next_visit_date,
           treatment_end_date,
           date_of_event                       date_hiv_confirmed,
           weight_text_                     as weight,
           date_of_reported_hiv_viral_load  as viral_load_sent_date,
           regimen_change,
           operation_triple_zero_enrollment_da as otz_date
    FROM mamba_flat_encounter_follow_up follow_up
             left JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                       ON follow_up.encounter_id = follow_up_1.encounter_id
             left JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                       ON follow_up.encounter_id = follow_up_2.encounter_id
             left JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                       ON follow_up.encounter_id = follow_up_3.encounter_id
             left JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                       ON follow_up.encounter_id = follow_up_4.encounter_id
),
    tmp_latest_follow_up as (
        SELECT encounter_id,
               client_id,
               follow_up_date,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
        FROM FollowUp
        WHERE follow_up_status IS NOT NULL
          AND art_start_date IS NOT NULL
          AND follow_up_date <= REPORT_END_DATE
    ),
     latest_follow_up as (
         select * from tmp_latest_follow_up where row_num=1 -- temp3
     ),
     tmp_vl_performed_date as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                viral_load_perform_date,
                viral_load_sent_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
     ),
     vl_performed_date as (
         select * from tmp_vl_performed_date where row_num=1  -- temp6
     ),
     tmp_otz_vl_performed_date as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                viral_load_perform_date,
                viral_load_sent_date,
                otz_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
           AND otz_date BETWEEN DATE_ADD(viral_load_perform_date, INTERVAL -3 MONTH) AND DATE_ADD(viral_load_perform_date, INTERVAL 2 MONTH)
     ),
     otz_vl_performed_date as (
         select * from tmp_otz_vl_performed_date where row_num=1 -- temp9
     ),
     tmp_oldest_follow_up as (
         SELECT encounter_id,
                client_id,
                follow_up_date,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date , encounter_id) AS row_num
         FROM FollowUp
         WHERE follow_up_status IS NOT NULL
           AND art_start_date IS NOT NULL
           AND follow_up_date <= REPORT_END_DATE
     ),
     oldest_follow_up as (
         select * from tmp_oldest_follow_up where row_num=1 -- temp13
     ),


     SELECT CASE
    WHEN otz.enrollement_date is null THEN NULL
    ELSE[dbo].[Fn_gregoriantoethiopiandate](otz.enrollement_date)
END                                                   AS EnrollementDate,
    CASE
    WHEN otz.isenrolledyes = 1 THEN 'Yes'
    WHEN otz.isenrolledno = 1 THEN 'No'
    ELSE ''
END                                                   AS
    EnrollementStatus,
#temp10.fullname,
#temp10.age,
#temp10.sex,
#temp10.phonenumber,
#temp10.mobilephonenumber,
latest_follow_up.[weight],
#temp10.mrn,
latest_follow_up.uniqueartnumber,
    CASE
    WHEN latest_follow_up.date_hiv_confirmed is null THEN NULL
    ELSE[dbo].[Fn_gregoriantoethiopiandate](latest_follow_up.date_hiv_confirmed)
END                                                   AS confirmeddate,
    CASE
    WHEN latest_follow_up.art_start_date is null THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](latest_follow_up.art_start_date)
END                                                   AS startedDate,
    CASE
    WHEN latest_follow_up.followupdate is null THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](latest_follow_up.followupdate)
END                                                   AS FollowUpDate,
    CASE
    WHEN latest_follow_up.scheduletype = 0 THEN 'S'
    WHEN latest_follow_up.scheduletype = 1 THEN 'US'
    ELSE ''
END                                                   AS scheduletype,
    CASE latest_follow_up.nutrition_resultsnew
    WHEN 0 THEN 'Normal'
    WHEN 1 THEN 'UnderNourished'
    WHEN 2 THEN 'OverWeight'
    ELSE ''
END                                                   AS
    NutritionalStatus,
    CASE
    WHEN latest_follow_up.adherance = '0' THEN 'Good'
    WHEN latest_follow_up.adherance = '1' THEN 'Fair'
    WHEN latest_follow_up.adherance = '2' THEN 'Poor'
    ELSE ''
END                                                   AS Adherance,
    CASE
    WHEN latest_follow_up.next_visit_date is null THEN NULL
    ELSE[dbo].[Fn_gregoriantoethiopiandate](latest_follow_up.next_visit_date)
END                                                   AS nextvisitdate,
    CASE
    WHEN latest_follow_up.art_dose = '0' THEN '30'
    WHEN latest_follow_up.art_dose = '1' THEN '60'
    WHEN latest_follow_up.art_dose = '2' THEN '90'
    WHEN latest_follow_up.art_dose = '3' THEN '120'
    WHEN latest_follow_up.art_dose = '4' THEN '150'
    WHEN latest_follow_up.art_dose = '5' THEN '180'
    ELSE ''
END                                                   AS Dosedays,
    dbo.Fn_getartregimencode(latest_follow_up.arvdispendseddose)    AS Regimen,
    Cast(CASE
    WHEN latest_follow_up.[follow_up_status] = 0 THEN 'TO'
    WHEN latest_follow_up.[follow_up_status] = 1 THEN 'STOP'
    WHEN [follow_up_status] = 2 THEN 'Lost'
    WHEN latest_follow_up.[follow_up_status] = 3 THEN 'Dropped'
    WHEN [follow_up_status] = 4 THEN 'Dead'
    WHEN latest_follow_up.[follow_up_status] = 5 THEN 'Alive on ART'
    WHEN latest_follow_up.[follow_up_status] = 6 THEN 'Restart'
    END AS VARCHAR(50))                              AS FollowUpStatus,
    CASE
    WHEN otz_vl_performed_date.bl_vl_sent_date is null THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](otz_vl_performed_date.bl_vl_sent_date)
END                                                   AS BaselineVLsent,
    CASE
    WHEN otz_vl_performed_date.bl_viral_load_perform_date is null THEN NULL
    ELSE
    [dbo].[Fn_gregoriantoethiopiandate](otz_vl_performed_date.bl_viral_load_perform_date)
END                                                   AS
    BaselineVLReceived,
otz_vl_performed_date.bl_viral_load_count                            AS BaselineVLCount,
otz_vl_performed_date.bl_viral_load_status                           AS BaselineVLStatus
    ,
    CASE
    WHEN vl_performed_date.vl_sent_date is null THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](vl_performed_date.vl_sent_date)
END                                                   AS VLsent,
    CASE
    WHEN vl_performed_date.viral_load_perform_date is null THEN NULL
    ELSE
    [dbo].[Fn_gregoriantoethiopiandate](vl_performed_date.viral_load_perform_date)
END                                                   AS VLReceived,
vl_performed_date.viral_load_count                               AS VLCount,
vl_performed_date.viral_load_status                              AS VLStatus,
    dbo.Fn_getartregimencode(oldest_follow_up.o_arvdispendseddose) AS OriginalRegimen,
    CASE
    WHEN #temp15.crg_followupdate is null THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp15.crg_followupdate)
END                                                   AS
    currentRegimenStart

    FROM   latest_follow_up
    LEFT JOIN #temp10
    ON #temp10.patientid = latest_follow_up.patientid
    LEFT JOIN otz_vl_performed_date
    ON otz_vl_performed_date.patientid = latest_follow_up.patientid
    LEFT JOIN vl_performed_date
    ON vl_performed_date.patientid = latest_follow_up.patientid
    LEFT JOIN oldest_follow_up
    ON oldest_follow_up.patientid = latest_follow_up.patientid
    LEFT JOIN #temp15
    ON #temp15.patientid = latest_follow_up.patientid
    LEFT JOIN dbo.icap_track_otz AS otz
    ON otz.patientid = latest_follow_up.patientid
    WHERE  otz.enrollement_date BETWEEN @DateFrom AND @DateTo
  AND ( ( latest_follow_up.[follow_up_status] != 0
  AND latest_follow_up.[follow_up_status] != 4 )
   OR ( otz.enrollement_date > '1900-01-01'
   OR otz.isenrolledyes = 1
   OR otz.isenrolledno = 1 ) )
  AND #temp10.fullname IS NOT NULL
  AND ( ( #temp10.age >= 10
  AND #temp10.age <= 24 )
   OR ( otz.enrollement_date > '1900-01-01'
   OR otz.isenrolledyes = 1
   OR otz.isenrolledno = 1 ) )