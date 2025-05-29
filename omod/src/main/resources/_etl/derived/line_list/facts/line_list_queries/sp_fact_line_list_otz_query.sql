DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_line_list_otz_query;

CREATE PROCEDURE sp_fact_line_list_otz_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE)
BEGIN


INSERT INTO #temp1
    SELECT patientid,
       Max(followupdate) AS FollowupDate
FROM   dbo.crtethiopiaartvisit
WHERE  ( deprecated = 0 )
  AND ( NOT( follow_up_status IS NULL ) )
  AND ( art_start = 1 )
  AND followupdate <= Getdate()
GROUP  BY patientid


INSERT INTO #temp2
    SELECT Max(v.id) AS Id
FROM   dbo.crtethiopiaartvisit AS v
    INNER JOIN #temp1
ON v.patientid = #temp1.patientid
    AND v.followupdate = #temp1.followupdate
GROUP  BY v.patientid,
    v.followupdate,
    v.deprecated
HAVING v.deprecated = 0


INSERT INTO #temp3
    SELECT d.id,
    d.patientid,
    d.uniqueartnumber,
    d.[weight],
    d.date_hiv_confirmed,
    d.art_start_date,
    d.followupdate,
    d.scheduletype,
    d.nutrition_resultsnew,
    d.arvdispendseddose,
    d.art_dose,
    adherance,
    d.next_visit_date,
    d.follow_up_status,
    d.art_dose_end
FROM   dbo.crtethiopiaartvisit AS d
    INNER JOIN #temp2
ON d.id = #temp2.id



INSERT INTO #temp4
    SELECT patientid,
       Max(viral_load_perform_date) AS VLDate
FROM   dbo.crtethiopiaartvisit
WHERE  ( deprecated = 0 )
  AND ( NOT ( follow_up_status IS NULL ) )
  AND ( art_start = 1 )
GROUP  BY patientid



INSERT INTO #temp5
    SELECT Max(vl.id) AS Id
FROM   dbo.crtethiopiaartvisit AS vl
    INNER JOIN #temp4
ON vl.patientid = #temp4.patientid
    AND vl.viral_load_perform_date = #temp4.vldate
GROUP  BY vl.patientid



INSERT INTO #temp6
    SELECT dvl.id,
    dvl.patientid,
    vlsent.vl_sent_date,
    dvl.viral_load_perform_date,
    dvl.viral_load_count,
    dvl.viral_load_status,
    dvl.followupdate
FROM   dbo.crtethiopiaartvisit AS dvl
    INNER JOIN #temp5
ON dvl.id = #temp5.id
    LEFT JOIN dbo.icap_track_vlsentdate AS vlsent
    ON dvl.viral_load_perform_date = vlsent.vl_received_date
    AND dvl.followupdate = vlsent.followupdate
    AND dvl.patientid = vlsent.patientid





INSERT INTO #temp7
    SELECT bl.patientid,
    Max(viral_load_perform_date) AS VLDate
FROM   dbo.crtethiopiaartvisit AS bl
    INNER JOIN dbo.icap_track_otz AS otzbvl
ON bl.patientid = otzbvl.patientid
WHERE  bl.deprecated = 0
  AND bl.follow_up_status IS NOT NULL
  AND bl.art_start = 1
  AND otzbvl.enrollement_date BETWEEN
    Dateadd(month, -3, bl.viral_load_perform_date)
  AND
    Dateadd(month, 2, bl.viral_load_perform_date)
GROUP  BY bl.patientid


INSERT INTO #temp8
    SELECT Max(vlbl.id) AS Id
FROM   dbo.crtethiopiaartvisit AS vlbl
    INNER JOIN #temp7
ON vlbl.patientid = #temp7.patientid
    AND vlbl.viral_load_perform_date = #temp7.vldate
GROUP  BY vlbl.patientid



-----------------------------------------------------------------------------------------------



INSERT INTO #temp9
    SELECT dvl_base.id,
    dvl_base.patientid,
    vlsent_base.vl_sent_date,
    dvl_base.viral_load_perform_date,
    dvl_base.viral_load_count,
    dvl_base.viral_load_status,
    dvl_base.followupdate
FROM   dbo.crtethiopiaartvisit AS dvl_base
    INNER JOIN #temp8
ON dvl_base.id = #temp8.id
    LEFT JOIN dbo.icap_track_vlsentdate AS vlsent_base
    ON dvl_base.viral_load_perform_date = vlsent_base.vl_received_date
    AND dvl_base.followupdate = vlsent_base.followupdate
    AND dvl_base.patientid = vlsent_base.patientid


INSERT INTO #temp10
    SELECT reg.patientid,
    reg.pid,
    reg.fullname,
    dbo.Fn_age(reg.dateofbirth, Getdate()) AS Age,
    sex,
    address.phonenumber,
    address.mobilephonenumber
FROM   dbo.registration AS reg
    LEFT JOIN address
ON reg.patientguid = address.patientguid


INSERT INTO #temp11
    SELECT patientid,
       Min(followupdate) AS FollowupDate
FROM   dbo.crtethiopiaartvisit
WHERE  deprecated = 0
  AND follow_up_status IS NOT NULL
  AND art_start = 1
  AND arvdispendseddose IS NOT NULL
  AND followupdate <= Getdate()
GROUP  BY patientid


INSERT INTO #temp12
    SELECT Max(vo.id) AS Id
FROM   dbo.crtethiopiaartvisit AS vo
    INNER JOIN #temp11
ON vo.patientid = #temp11.patientid
    AND vo.followupdate = #temp11.followupdate
GROUP  BY vo.patientid,
    vo.followupdate,
    vo.deprecated
HAVING vo.deprecated = 0


INSERT INTO #temp13
    SELECT od.id,
    od.patientid,
    od.followupdate,
    od.arvdispendseddose
FROM   dbo.crtethiopiaartvisit AS od
    INNER JOIN #temp12
ON od.id = #temp12.id



INSERT INTO #temp14
    SELECT patientid,
       Min(followupdate) AS FollowupDate,
       arvdispendseddose
FROM   dbo.crtethiopiaartvisit
WHERE  deprecated = 0
  AND follow_up_status IS NOT NULL
  AND art_start = 1
  AND arvdispendseddose IS NOT NULL
  AND followupdate <= Getdate()
GROUP  BY patientid,
    arvdispendseddose



INSERT INTO #temp15
    SELECT creg.patientid,
    Min(creg.followupdate) AS crg_FollowupDate
FROM   dbo.crtethiopiaartvisit AS creg
    INNER JOIN #temp14
ON creg.patientid = #temp14.patientid
    AND creg.followupdate = #temp14.followupdate
WHERE  creg.deprecated = 0
  AND creg.follow_up_status IS NOT NULL
  AND creg.art_start = 1
  AND creg.arvdispendseddose = (SELECT TOP 1 #temp14.arvdispendseddose
    FROM   #temp14
    ORDER  BY #temp14.followupdate DESC)
    GROUP  BY creg.patientid

    SELECT CASE
    WHEN otz.enrollement_date = '1900-01-01' THEN NULL
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
#temp3.[weight],
#temp10.mrn,
#temp3.uniqueartnumber,
    CASE
    WHEN #temp3.date_hiv_confirmed = '1900-01-01' THEN NULL
    ELSE[dbo].[Fn_gregoriantoethiopiandate](#temp3.date_hiv_confirmed)
    END                                                   AS confirmeddate,
    CASE
    WHEN #temp3.art_start_date = '1900-01-01' THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp3.art_start_date)
    END                                                   AS startedDate,
    CASE
    WHEN #temp3.followupdate = '1900-01-01' THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp3.followupdate)
    END                                                   AS FollowUpDate,
    CASE
    WHEN #temp3.scheduletype = 0 THEN 'S'
    WHEN #temp3.scheduletype = 1 THEN 'US'
    ELSE ''
    END                                                   AS scheduletype,
    CASE #temp3.nutrition_resultsnew
    WHEN 0 THEN 'Normal'
    WHEN 1 THEN 'UnderNourished'
    WHEN 2 THEN 'OverWeight'
    ELSE ''
    END                                                   AS
    NutritionalStatus,
    CASE
    WHEN #temp3.adherance = '0' THEN 'Good'
    WHEN #temp3.adherance = '1' THEN 'Fair'
    WHEN #temp3.adherance = '2' THEN 'Poor'
    ELSE ''
    END                                                   AS Adherance,
    CASE
    WHEN #temp3.next_visit_date = '1900-01-01' THEN NULL
    ELSE[dbo].[Fn_gregoriantoethiopiandate](#temp3.next_visit_date)
    END                                                   AS nextvisitdate,
    CASE
    WHEN #temp3.art_dose = '0' THEN '30'
    WHEN #temp3.art_dose = '1' THEN '60'
    WHEN #temp3.art_dose = '2' THEN '90'
    WHEN #temp3.art_dose = '3' THEN '120'
    WHEN #temp3.art_dose = '4' THEN '150'
    WHEN #temp3.art_dose = '5' THEN '180'
    ELSE ''
    END                                                   AS Dosedays,
    dbo.Fn_getartregimencode(#temp3.arvdispendseddose)    AS Regimen,
    Cast(CASE
    WHEN #temp3.[follow_up_status] = 0 THEN 'TO'
    WHEN #temp3.[follow_up_status] = 1 THEN 'STOP'
    WHEN [follow_up_status] = 2 THEN 'Lost'
    WHEN #temp3.[follow_up_status] = 3 THEN 'Dropped'
    WHEN [follow_up_status] = 4 THEN 'Dead'
    WHEN #temp3.[follow_up_status] = 5 THEN 'Alive on ART'
    WHEN #temp3.[follow_up_status] = 6 THEN 'Restart'
    END AS VARCHAR(50))                              AS FollowUpStatus,
    CASE
    WHEN #temp9.bl_vl_sent_date = '1900-01-01' THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp9.bl_vl_sent_date)
    END                                                   AS BaselineVLsent,
    CASE
    WHEN #temp9.bl_viral_load_perform_date = '1900-01-01' THEN NULL
    ELSE
    [dbo].[Fn_gregoriantoethiopiandate](#temp9.bl_viral_load_perform_date)
    END                                                   AS
    BaselineVLReceived,
#temp9.bl_viral_load_count                            AS BaselineVLCount,
#temp9.bl_viral_load_status                           AS BaselineVLStatus
    ,
    CASE
    WHEN #temp6.vl_sent_date = '1900-01-01' THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp6.vl_sent_date)
    END                                                   AS VLsent,
    CASE
    WHEN #temp6.viral_load_perform_date = '1900-01-01' THEN NULL
    ELSE
    [dbo].[Fn_gregoriantoethiopiandate](#temp6.viral_load_perform_date)
    END                                                   AS VLReceived,
#temp6.viral_load_count                               AS VLCount,
#temp6.viral_load_status                              AS VLStatus,
    dbo.Fn_getartregimencode(#temp13.o_arvdispendseddose) AS OriginalRegimen,
    CASE
    WHEN #temp15.crg_followupdate = '1900-01-01' THEN NULL
    ELSE [dbo].[Fn_gregoriantoethiopiandate](#temp15.crg_followupdate)
    END                                                   AS
    currentRegimenStart

    FROM   #temp3
    LEFT JOIN #temp10
    ON #temp10.patientid = #temp3.patientid
    LEFT JOIN #temp9
    ON #temp9.patientid = #temp3.patientid
    LEFT JOIN #temp6
    ON #temp6.patientid = #temp3.patientid
    LEFT JOIN #temp13
    ON #temp13.patientid = #temp3.patientid
    LEFT JOIN #temp15
    ON #temp15.patientid = #temp3.patientid
    LEFT JOIN dbo.icap_track_otz AS otz
    ON otz.patientid = #temp3.patientid
    WHERE  otz.enrollement_date BETWEEN @DateFrom AND @DateTo
  AND ( ( #temp3.[follow_up_status] != 0
  AND #temp3.[follow_up_status] != 4 )
   OR ( otz.enrollement_date > '1900-01-01'
   OR otz.isenrolledyes = 1
   OR otz.isenrolledno = 1 ) )
  AND #temp10.fullname IS NOT NULL
  AND ( ( #temp10.age >= 10
  AND #temp10.age <= 24 )
   OR ( otz.enrollement_date > '1900-01-01'
   OR otz.isenrolledyes = 1
   OR otz.isenrolledno = 1 ) )


    END //

    DELIMITER ;
