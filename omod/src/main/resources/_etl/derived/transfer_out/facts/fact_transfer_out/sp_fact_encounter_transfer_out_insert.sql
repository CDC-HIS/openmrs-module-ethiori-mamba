-- $BEGIN
INSERT INTO mamba_fact_client_tranfer_out (
    client_id,
    latest_followup_date,
    art_start_date,
    adherence,
    regimen,
    follow_up_status,
    next_visit_date,
    treatment_end_date
)
SELECT client_id,
       MAX(CASE WHEN rn_desc = 1 THEN art_antiretroviral_start_date END)        AS art_start_date,
       MAX(CASE WHEN rn_desc = 1 THEN treatment_end_date END)                  AS treatment_end_date,
       MAX(CASE WHEN rn_desc = 1 THEN return_visit_date END)                   AS next_visit_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_date_followup_ END)            AS followup_date,
       MAX(CASE WHEN rn_desc = 1 THEN follow_up_status END)                    AS followup_status,
       MAX(CASE WHEN rn_desc = 1 THEN regimen END)                             AS regimen,
       MAX(CASE WHEN rn_desc = 1 THEN adherence END) AS adherence
FROM (SELECT f1.client_id,
             adherence,
             art_antiretroviral_start_date,
             follow_up_date_followup_,
             regimen,
             follow_up_status,
             treatment_end_date,
             return_visit_date,
             -- ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ )     AS rn_asc,
             ROW_NUMBER() OVER (PARTITION BY f1.client_id ORDER BY follow_up_date_followup_ DESC) AS rn_desc
      FROM mamba_flat_encounter_follow_up f1 INNER JOIN
mamba_flat_encounter_follow_up_1 f2 ON f1.encounter_id=f2.encounter_id AND f1.client_id=f2.client_id
      where art_antiretroviral_start_date is not null AND f1.follow_up_status ='Transferred out') AS subquery
WHERE rn_desc = 1
GROUP BY client_id;
-- $END