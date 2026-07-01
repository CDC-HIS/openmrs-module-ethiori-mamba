# DQI&U Line List & ART Dashboard — Implementation Reference
**Source:** DQIU_LL ART_Dashbaord_Requirement112525.docx  
**Date Captured:** 2026-06-30  
**Target Table:** `mamba_fact_client_staging` (promoted to `mamba_fact_client` after atomic rename)  
**Reporting Scope:** All values reflect data as of **TODAY** (latest ETL run). Date-based historical reporting is a separate future requirement.

---

## Part 1: Patient-Level ART Dashboard

A single-view patient summary card embedded in the EthioHRI clinical search page. Visible only for clients with an **HIV Confirmed Date** (not PrEP/PostEP). Data reflects the patient's situation as of today.

### Patient Registration

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Full Name | `patient_name` | `mamba_dim_client.patient_name` | Direct select, COALESCE to `'-'` |
| Age | `age` | `mamba_dim_client.date_of_birth` | `TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE())` |
| Date of Birth | `birthdate` | `mamba_dim_client.date_of_birth` | Direct select |
| Sex | `sex` | `mamba_dim_client.sex` | Direct select |
| MRN | `mrn` | `mamba_dim_client.mrn` | Direct select, COALESCE to `'-'` |
| UAN | `uan` | `mamba_dim_client.uan` | Direct select, COALESCE to `'-'` |
| PHRH Code | `phrh_code` | `mamba_dim_patient_identifier` | `Identifiers` CTE — MAX where `identifier_type.name = 'PHRH'` |
| NCD Code | `ncd_code` | `mamba_dim_patient_identifier` | `Identifiers` CTE — MAX where `identifier_type.name = 'NCD'` |
| ICD # | `icd_number` | `mamba_dim_patient_identifier` | `Identifiers` CTE — MAX where `identifier_type.name = 'ICD'` |

### ART Enrollment Information

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Registration Date | `registration_date` | `mamba_flat_encounter_registration` | `Registration` CTE — MIN `registration_date` per client |
| HIV Confirmed Date | `hiv_confirmed_date` | `mamba_flat_encounter_follow_up.date_of_event` | `LatestFollowUp` CTE — from most recent follow-up with non-null follow-up date |
| ART Start Date | `art_start_date` | `mamba_flat_encounter_follow_up.art_antiretroviral_start_date` | `Medical_Aggregates` CTE — MIN across all follow-ups |
| Months on ART | `months_on_art` | Computed | `TIMESTAMPDIFF(MONTH, art_start_date, CURDATE())` |
| Transfer-In Date (GC) | `transfer_in_date` | `mamba_flat_encounter_follow_up` | `Medical_Aggregates` CTE — MIN date where `transfer_in = 'Yes'/'True'/'1'` or `follow_up_status IN ('Transfer in', 'TI')` |
| Transfer-In Date (EC) | `transfer_in_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(transfer_in_date, 'Y-M-D')` |

### Client Address

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Region | `region` | `mamba_dim_client.state_province` | Direct select, COALESCE to `'-'` |
| Zone/Sub-City | `zone` | `mamba_dim_client.county_district` | Direct select, COALESCE to `'-'` |
| Woreda | `woreda` | `mamba_dim_client.city_village` | Direct select, COALESCE to `'-'` |
| Kebele | `kebele` | `mamba_dim_client.kebele` | Direct select, COALESCE to `'-'` |
| House Number | `house_number` | `mamba_dim_client.house_number` | Direct select, COALESCE to `'-'` |
| Mobile Phone | `mobile_phone` | `mamba_dim_client.mobile_no` | Direct select, COALESCE to `'-'` |
| Address Completeness | `address_completeness` | Computed | `'GREEN'` if `patient_name`, `state_province`, and `mobile_no` are all non-null; else `'YELLOW'` |

### Previous ART Visit Status

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Follow-Up Date | `last_visit_date` | `mamba_flat_encounter_follow_up.follow_up_date_followup_` | `LatestFollowUp` CTE — `visit_date` from most recent row ranked by `follow_up_date_followup_ DESC, encounter_id DESC` |
| Follow-Up Status | `current_status` | `mamba_flat_encounter_follow_up.follow_up_status` | `LatestFollowUp` CTE — mapped: Dead/Died → `'Dead'`, TO → `'Transferred Out'`, active if `treatment_end_date >= CURDATE()` → `'Active'` |
| ARV Regimen | `current_regimen` | `mamba_flat_encounter_follow_up.regimen` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| ARV Regimen Dose | `regimen_dose` | `mamba_flat_encounter_follow_up.antiretroviral_art_dispensed_dose_i` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| Regimen Line | `regimen_line` | Computed | `'First Line'` if regimen starts with `'1'`; `'Second Line'` if `'2'`; else `'Other'` |
| TX_Curr End Date | `tx_curr_end_date` | `mamba_flat_encounter_follow_up.treatment_end_date` | `LatestFollowUp` CTE |
| Nutritional Status | `nutritional_status` | `mamba_flat_encounter_follow_up` | `LatestFollowUp` — COALESCE of `nutritional_status_of_adult` and `nutritional_status_of_older_child_a`, mapped to simplified labels |
| Pregnancy Status | `pregnancy_status` | `mamba_flat_encounter_follow_up.pregnancy_status` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| PMTCT Status | `pmtct_status` | `mamba_flat_encounter_pmtct_enrollment` / `mamba_flat_encounter_pmtct_discharge` | `'Currently on PMTCT'` if booking date exists and no discharge date (or discharge < booking); else `'Not Applicable'` |
| Family Planning Method | `family_planning_method` | `mamba_flat_encounter_follow_up.method_of_family_planning` | `FamilyPlanning` CTE — most recent non-null, non-empty value |
| WHO Stage | `who_stage` | `mamba_flat_encounter_follow_up.current_who_hiv_stage` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| Next Visit Date | `next_appointment_date` | `mamba_flat_encounter_follow_up.next_visit_date` | `LatestFollowUp` CTE |
| Missed Days | `days_overdue` | Computed | `DATEDIFF(CURDATE(), COALESCE(next_visit_date, visit_date))`, floor at 0 |

### Viral Load Status

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Last VL Date | `last_vl_date` | `mamba_flat_encounter_follow_up.date_viral_load_results_received` | `vl_performed` CTE — most recent result-received date |
| Last VL Result | `last_vl_result` | `mamba_flat_encounter_follow_up.hiv_viral_load` | `vl_performed` CTE |
| Suppressed Flag | `is_suppressed` | Computed | `1` if `viral_load_status_inferred = 'S'`; `0` if `'U'`; NULL otherwise |
| VL Status | `vl_status` | Computed | `vl_eligibility` CTE — `'Eligible for Viral Load'` / `'Viral Load Done (Currently not Eligible)'` / `'Not Applicable'`; see `vl_status_dqi` for corrected ART Not Started logic |
| VL Eligibility Date | `vl_eligibility_date` | Computed | Scenario-based: restart (+91d), switch (+181d), pregnant (+91d), first (+181d), annual (+365d), etc. |
| VL Sent Date (GC) | `vl_sent_date` | `mamba_flat_encounter_follow_up.date_of_reported_hiv_viral_load` | `vl_performed` CTE — `VL_Sent_Date` |
| VL Sent Date (EC) | `vl_sent_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(vl_sent_date, 'Y-M-D')` |
| VL Received Date (GC) | `vl_received_date` | `mamba_flat_encounter_follow_up.date_viral_load_results_received` | `vl_performed` CTE — `viral_load_perform_date` |
| VL Received Date (EC) | `vl_received_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(vl_received_date, 'Y-M-D')` |

### TPT Information

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| TPT Status | `tpt_status` | Computed | `Medical_Aggregates` CTE: Gold if completed; On TPT if started and not completed/discontinued; Contraindicated via MAX flag across all visits; else Not Started. See `tpt_status_dqi` for corrected logic |
| TPT Start Date (GC) | `tpt_start_date` | `mamba_flat_encounter_follow_up.date_started_on_tuberculosis_prophy` | `Medical_Aggregates` CTE — MAX |
| TPT Start Date (EC) | `tpt_start_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tpt_start_date, 'Y-M-D')` |
| TPT Completed Date (GC) | `tpt_completed_date` | `mamba_flat_encounter_follow_up.date_completed_tuberculosis_prophyl` | `Medical_Aggregates` CTE — MAX |
| TPT Completed Date (EC) | `tpt_completed_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tpt_completed_date, 'Y-M-D')` |
| TPT Discontinued Date (GC) | `tpt_discontinued_date` | `mamba_flat_encounter_follow_up.date_discontinued_tuberculosis_prop` | `Medical_Aggregates` CTE — MAX |
| TPT Discontinued Date (EC) | `tpt_discontinued_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tpt_discontinued_date, 'Y-M-D')` |

### TB Treatment Information

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Active TB Diagnosis Date (GC) | `active_tb_diagnosis_date` | `mamba_flat_encounter_follow_up.diagnosis_date` | `Medical_Aggregates` CTE — MAX |
| Active TB Diagnosis Date (EC) | `active_tb_diagnosis_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(active_tb_diagnosis_date, 'Y-M-D')` |
| TB Treatment Start Date (GC) | `tb_treatment_start_date` | `mamba_flat_encounter_follow_up.tuberculosis_drug_treatment_start_d` | `Medical_Aggregates` CTE — MAX |
| TB Treatment Start Date (EC) | `tb_treatment_start_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tb_treatment_start_date, 'Y-M-D')` |
| TB Treatment Discontinued Date (GC) | `tb_treatment_discontinued_date` | `mamba_flat_encounter_follow_up.date_active_tbrx_dc` | `Medical_Aggregates` CTE — MAX |
| TB Treatment Discontinued Date (EC) | `tb_treatment_discontinued_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tb_treatment_discontinued_date, 'Y-M-D')` |
| TB Treatment Completed Date (GC) | `tb_treatment_completed_date` | `mamba_flat_encounter_follow_up.date_active_tbrx_completed` | `Medical_Aggregates` CTE — MAX |
| TB Treatment Completed Date (EC) | `tb_treatment_completed_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(tb_treatment_completed_date, 'Y-M-D')` |

### Other Clinical Statuses & Identifiers

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Current DSD Category | `dsd_category` | `mamba_flat_encounter_follow_up.dsd_category` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| ICT Screening Status | `ict_screening_status` | `mamba_flat_encounter_ict_offer` | `ICT_Events` CTE — most recent offered date; mapped Accepted/Refused/Offered; default `'Not Screened'`. See `ict_screening_status_dqi` |
| ICT Number | `ict_number` | `mamba_flat_encounter_ict_general.ict_serial_number` | `ICT_General_Events` CTE — most recent non-empty serial number |
| NCD Screening Status | `ncd_screening_status` | `mamba_flat_encounter_ncd_screening.baseline_diagnosis` | `NCD_Screening_Events` CTE — most recent screening date. See `ncd_screening_status_dqi` |
| Next NCD Screening Date | `next_ncd_screening_date` | `mamba_flat_encounter_ncd_followup.return_visit_date` | `NCD_FollowUp_Events` CTE — most recent follow-up |
| Systolic BP | `systolic_blood_pressure` | `mamba_flat_encounter_ncd_followup.systolic_blood_pressure` | `NCD_FollowUp_Events` CTE |
| Diastolic BP | `diastolic_blood_pressure` | `mamba_flat_encounter_ncd_followup.diastolic_blood_pressure` | `NCD_FollowUp_Events` CTE |
| CXCA Screening Status | `cxca_screening_status` | Computed | `CXCA_Status` CTE — eligibility-based label. See `cxca_screening_status_dqi` for corrected labels |
| Next CCA Screening Date | `next_cca_screening_date` | `mamba_flat_encounter_follow_up.next_follow_up_screening_date` | `CXCA_PrevScreening` CTE |
| Target Population | `target_population` | `mamba_flat_encounter_phrh_followup.target_population` | `PHRH_Target_Population` CTE — most recent non-empty value; fallback to `mamba_dim_client.key_population` |

---

## Part 2: DQI&U Line List Additions

### New Clinical Fields

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| Breastfeeding Status | `breast_feeding_status` | `mamba_flat_encounter_follow_up.currently_breastfeeding_child` | `LatestFollowUp` CTE, COALESCE to `'-'` |
| Disclosure Stage | `disclosure_stage` | `mamba_flat_encounter_follow_up_5.stages_of_disclosure` | `Latest_Disclosure` CTE — most recent non-null value from `mamba_fact_follow_up` |
| CD4 Result | `cd4_result` | `mamba_flat_encounter_follow_up.cd4_count` | `Latest_CD4` CTE — most recent non-null cd4 or visitect row from `mamba_fact_follow_up` |
| VISITECT CD4 Result | `visitect_cd4_result` | `mamba_flat_encounter_follow_up.visitect_cd4_result` | `Latest_CD4` CTE — same row as `cd4_result` |

### PMTCT Dates

| Requirement | Column | Source | Implementation |
|---|---|---|---|
| PMTCT Booking Date (GC) | `pmtct_booking_date` | `mamba_flat_encounter_pmtct_enrollment.date_of_enrollment_or_booking` | `PMTCT_CTE` — MAX booking date |
| PMTCT Booking Date (EC) | `pmtct_booking_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(pmtct_booking_date, 'Y-M-D')` |
| PMTCT Discharge Date (GC) | `pmtct_discharge_date` | `mamba_flat_encounter_pmtct_discharge.discharge_date` | `PMTCT_Discharge_CTE` — MAX discharge date |
| PMTCT Discharge Date (EC) | `pmtct_discharge_date_ec` | Computed | `fn_gregorian_to_ethiopian_calendar(pmtct_discharge_date, 'Y-M-D')` |

### New Computed Status Columns

#### TB Treatment (RX) Status — `tb_treatment_rx_status`

Source: `Medical_Aggregates` CTE

| Label | Rule |
|---|---|
| `NULL` | `active_tb_diagnosis_date IS NULL` |
| `Completed TB Treatment` | `tb_treatment_completed_date IS NOT NULL` |
| `On TB Treatment` | Start date exists; `TIMESTAMPDIFF(YEAR, start, TODAY) < 2`; no complete date |
| `Unknown TB Treatment Completion` | Start date exists; `TIMESTAMPDIFF(YEAR, start, TODAY) >= 2`; no complete date |
| `Active TB Diagnosed & No RX` | Diagnosis date exists; no start and no complete date |

#### PMTCT Enrollment Status — `pmtct_enrollment_status`

Source: `PMTCT_CTE` + `PMTCT_Discharge_CTE`

| Label | Rule |
|---|---|
| `NULL` | No PMTCT booking date |
| `Currently on PMTCT` | Booking date ≤ TODAY; discharge date is NULL or > TODAY |
| `Enrolled & Currently Discharged` | Booking date ≤ TODAY; discharge date ≤ TODAY |

#### Advanced HIV Disease — `advanced_hiv_disease`

Source: `Latest_CD4` CTE + `LatestFollowUp` CTE

| Label | Rule |
|---|---|
| `Yes` | Age < 5 years |
| `Yes` | Age ≥ 5 and (`visitect_cd4_result IS NULL` and `cd4_count < 200`) OR `visitect_cd4_result = 'VISITECT <200 copies/ml'` |
| `Yes` | Age ≥ 5 and `who_stage IN ('WHO stage 3 adult', 'WHO stage 3 peds', 'WHO stage 4 peds', 'WHO stage 4 adult')` |
| `No` | All other cases |

#### NCD Screening Eligibility Status — `ncd_screening_eligibility_status`

Source: `NCD_Screening_Events` CTE (`baseline_diagnosis`) + `LatestFollowUp` + `mamba_dim_client`

| Label | Rule |
|---|---|
| `Not Applicable` | `follow_up_status IN ('Dead', 'Died', 'Transferred out', 'TO', 'Stop all')` |
| `Not Screened` | No NCD screening record |
| `Confirmed DM & HTN` | `baseline_diagnosis` contains both `'Hypertension'` and `'Diabetes'` |
| `Confirmed HTN` | `baseline_diagnosis` contains `'Hypertension'` |
| `Confirmed DM` | `baseline_diagnosis` contains `'Diabetes Mellitus'` |
| `Need Rescreening Prediabetes` | `baseline_diagnosis` contains `'Prediabetes'` or `'Pre-diabetes'` |
| `Needs Rescreening Normal BG (≤50 Yrs)` | `baseline_diagnosis` contains `'Normal BG'` or `'Normal Blood Glucose'`; age ≤ 50 |
| `Needs Rescreening Normal BG (>50 Yrs)` | `baseline_diagnosis` contains `'Normal BG'` or `'Normal Blood Glucose'`; age > 50 |
| `Needs Rescreening Normal BP` | `baseline_diagnosis` contains `'Normal BP'` or `'Normal Blood Pressure'` |
| `Not Applicable` | All other cases |

> Full date-based rescreening interval logic requires the external **"NCD Screening & PHRH Line Lists_111825"** Excel document.

#### NCD Screening Eligibility Date — `ncd_screening_eligibility_date`

Source: `NCD_FollowUp_Events.next_screening_date`

Mapped from `return_visit_date` on the most recent NCD follow-up encounter. Full interval-based calculation requires the external NCD reference document.

#### NCD Screening Reason — `ncd_screening_reason`

Source: Computed from `ncd_screening_eligibility_status`

| Value | Condition |
|---|---|
| `NOT APPLICABLE` | Dead, transferred out, stopped, or not screened |
| `Confirmed Hypertension and Diabetes Mellitus` | DM & HTN confirmed |
| `Confirmed Hypertension` | HTN confirmed |
| `Confirmed Diabetes Mellitus` | DM confirmed |
| `Prediabetes - Needs Rescreening` | Prediabetes |
| `Normal Blood Glucose - Needs Rescreening` | Normal BG |
| `Normal Blood Pressure - Needs Rescreening` | Normal BP |
| `NOT APPLICABLE` | All other |

---

## Part 2: DQI Corrected Status Columns

Original status columns are **preserved unchanged**. The following `_dqi` columns implement the corrected label logic and should be used for DQI&U reporting.

### `vl_status_dqi`

Source: same CTEs as `vl_status` (`Medical_Aggregates`, `vl_eligibility`)

| Label | Rule (evaluated in order) |
|---|---|
| `ART Not Started` | `art_start_date IS NULL` |
| `'-'` | No follow-up visit on record |
| `Not Applicable` | `vl_status_final = 'N/A'` |
| `Eligible for Viral Load` | `eligibility_date <= TODAY` |
| `Viral Load Done (Currently not Eligible)` | `eligibility_date > TODAY` |
| `'-'` | All other |

**Change from `vl_status`:** ART Not Started is an explicit first-priority check on `art_start_date IS NULL`. Original had a dual-null condition (`art_start_date IS NULL AND follow_up_status IS NULL`) that missed many cases.

---

### `tpt_status_dqi`

Source: `Medical_Aggregates` CTE + `Latest_TPT_Contraindication` CTE (queries `mamba_fact_follow_up`)

| Label | Rule (evaluated in order) |
|---|---|
| `ART Not Started` | `art_start_date IS NULL` |
| `Gold (TPT Completed)` | `tpt_completed_date IS NOT NULL` |
| `On TPT` | Start date exists; no complete or discontinued date |
| `Contraindicated for TPT` | Latest follow-up has `reason_not_eligible_for_tuberculosi = 'Contraindication'` |
| `Bronze 5` | `tb_treatment_completed_date IS NOT NULL` and `TIMESTAMPDIFF(YEAR, tb_treatment_completed_date, TODAY) > 3` |
| `Not Started` | All other |

**Changes from `tpt_status`:**
- ART Not Started added as first check
- Contraindicated now uses **latest follow-up only** (not MAX flag across all visits)
- Bronze 5 implemented: TB treatment completed > 3 years ago

---

### `ict_screening_status_dqi`

Source: `Medical_Aggregates` CTE + `ICT_Events` CTE

| Label | Rule |
|---|---|
| `ART Not Started` | `art_start_date IS NULL` |
| `Accepted` / `Refused` / `Offered` / `Not Screened` | ICT offer status from `ICT_Events`; same logic as `ict_screening_status` |

**Change from `ict_screening_status`:** ART Not Started prepended as first check.

---

### `ncd_screening_status_dqi`

Source: `Medical_Aggregates` CTE + `NCD_Screening_Events` CTE

| Label | Rule |
|---|---|
| `ART Not Started` | `art_start_date IS NULL` |
| `<baseline_diagnosis>` | Most recent `baseline_diagnosis` from NCD screening encounter |
| `Screened` | NCD screening record exists but `baseline_diagnosis` is null |
| `Not Screened` | No NCD screening record |

**Change from `ncd_screening_status`:** ART Not Started prepended as first check.

---

### `cxca_screening_status_dqi`

Source: `CXCA_Status` CTE (same eligibility logic as `cxca_screening_status`)

| Label | Maps from `cxca_screening_status` |
|---|---|
| `Confirmed Cervical Cancer (Red)` | `'Confirmed CXCA (RED)'` |
| `Eligible for Cervical Cancer Screening/RX (Yellow)` | `'Eligible (Yellow)'` |
| `Previously Screened (Green)` | `'Previously Screened (Green)'` — unchanged |
| `ART Not Started (Black)` | `'ART Not Started (Black)'` — unchanged |
| `Not Applicable (Blue)` | `'Not Applicable (Blue)'` — unchanged |
| `Not Eligible (White)` | `'Not Eligible (White)'` — unchanged |

**Change from `cxca_screening_status`:** Two label renames only — `'Confirmed CXCA (RED)'` and `'Eligible (Yellow)'`.

---

## Supporting Infrastructure

### `mamba_fact_follow_up` additions

Two columns added to serve as sources for the `Latest_*` CTEs in `sp_fact_client_insert.sql`:

| Column | Source | Purpose |
|---|---|---|
| `stages_of_disclosure` | `mamba_flat_encounter_follow_up_5.stages_of_disclosure` | Powers `Latest_Disclosure` CTE → `disclosure_stage` |
| `reason_not_eligible_for_tuberculosi` | `mamba_flat_encounter_follow_up` (USING join) | Powers `Latest_TPT_Contraindication` CTE → `tpt_status_dqi` |

### CTEs added to `sp_fact_client_insert.sql`

| CTE | Source Table | Purpose |
|---|---|---|
| `Latest_CD4` | `mamba_fact_follow_up` | Most recent non-null `cd4_count` / `visitect_cd4_result` per client |
| `Latest_Disclosure` | `mamba_fact_follow_up` | Most recent non-null `stages_of_disclosure` per client |
| `Latest_TPT_Contraindication` | `mamba_fact_follow_up` | Most recent non-null `reason_not_eligible_for_tuberculosi` per client |

All three CTEs rank rows by `follow_up_date_followup_ DESC, encounter_id DESC` and return `rn = 1`.

### Ethiopian Calendar Conversion

All `_ec` columns use the existing scalar function:
```sql
fn_gregorian_to_ethiopian_calendar(<date_column>, 'Y-M-D')
```
Returns NULL when the source date is NULL.

---

## Part 3: Window Based DQI Query

A date-parameterised line list that reproduces the same CTE logic as `sp_fact_client_insert.sql` but evaluates everything **as of a caller-supplied end date** rather than `CURDATE()`. The primary data source is the flat encounter tables (same as the ETL build), with `mamba_fact_follow_up` used for `Latest_CD4`, `Latest_Disclosure`, and `Latest_TPT_Contraindication` lookups.

**File:** `omod/src/main/resources/_etl/derived/line_list/facts/line_list_queries/sp_fact_line_list_dqi_query.sql`

### Signature

```sql
CALL sp_fact_line_list_dqi_query(IN REPORT_START_DATE DATE, IN REPORT_END_DATE DATE);
```

| Parameter | Default when NULL | Role |
|---|---|---|
| `REPORT_END_DATE` | `CURDATE()` | Primary evaluation anchor — `v_end = COALESCE(REPORT_END_DATE, CURDATE())` used throughout |
| `REPORT_START_DATE` | no lower bound | Optional: lower bound on the patient's latest visit date within the window |

### How Date Parameters Are Applied

The key structural change from `sp_fact_client_insert.sql` is that the base `FollowUp` CTE is gated at `v_end`, which cascades through all derived CTEs automatically. Additional explicit filters are added where a date field may differ from the follow-up date.

| CTE | Date filter added |
|---|---|
| `FollowUp` (base) | `WHERE follow_up_date_followup_ <= v_end` — gates all encounter data |
| `Vl_Events` | `date_of_reported_hiv_viral_load <= v_end` and `date_viral_load_results_received <= v_end` — VL dates may differ from follow-up date |
| `vl_scenario` | `TIMESTAMPDIFF(DAY, art_start_date, v_end)` replaces `CURDATE()` in INELIGIBLE/PREG/FIRST_NULL checks |
| `vl_eligibility` | UNASSIGNED fallback: `DATE_ADD(v_end, INTERVAL 100 YEAR)` instead of `DATE_ADD(CURDATE(), INTERVAL 100 YEAR)` |
| `ICT_Events` | `WHERE offered_date <= v_end` |
| `NCD_Screening_Events` | `WHERE screening_date <= v_end` |
| `NCD_FollowUp_Events` | `WHERE f.follow_up_date <= v_end` |
| `CXCA_Eligibility_Base` | All `CURDATE()` → `v_end` (VIA age, HPV age, biopsy age, treat/referral dates) |
| `CXCA_Eligibility_Base` | Age: `TIMESTAMPDIFF(YEAR, date_of_birth, v_end)` |
| `PMTCT_CTE` | `WHERE date_of_enrollment_or_booking <= v_end` |
| `PMTCT_Discharge_CTE` | `WHERE discharge_date <= v_end` |
| `PHRH_Target_Population` | `WHERE followup_date <= v_end` |
| `Latest_CD4` (mamba_fact_follow_up) | `WHERE follow_up_date_followup_ <= v_end` |
| `Latest_Disclosure` (mamba_fact_follow_up) | `WHERE follow_up_date_followup_ <= v_end` |
| `Latest_TPT_Contraindication` (mamba_fact_follow_up) | `WHERE follow_up_date_followup_ <= v_end` |

### Computed Columns Using `v_end`

These columns in the final SELECT use `v_end` instead of `CURDATE()`:

| Column | Logic |
|---|---|
| `age` | `TIMESTAMPDIFF(YEAR, date_of_birth, v_end)` |
| `months_on_art` | `TIMESTAMPDIFF(MONTH, art_start_date, v_end)` |
| `current_status` | `treatment_end_date >= v_end` → `'Active'` |
| `days_overdue` | `DATEDIFF(v_end, COALESCE(next_visit_date, visit_date))` |
| `vl_status` / `vl_status_dqi` | `eligiblityDate <= v_end` → `'Eligible for Viral Load'` |
| `tpt_status_dqi` | Bronze 5: `TIMESTAMPDIFF(YEAR, tb_treatment_completed_date, v_end) > 3` |
| `tb_treatment_rx_status` | `TIMESTAMPDIFF(YEAR, tb_treatment_start_date, v_end) < 2` → On Treatment |
| `pmtct_enrollment_status` | `pmtct_discharge_date > v_end` → Currently on PMTCT |
| `advanced_hiv_disease` | `TIMESTAMPDIFF(YEAR, date_of_birth, v_end) < 5` |
| `ncd_screening_eligibility_status` | Normal BG age bucket: `TIMESTAMPDIFF(YEAR, date_of_birth, v_end) <= 50` |

### Population Filter

```sql
WHERE c.mrn IS NOT NULL
  AND lfu.visit_date IS NOT NULL
  AND lfu.visit_date <= v_end
  AND (REPORT_START_DATE IS NULL OR lfu.visit_date >= REPORT_START_DATE)
```

Returns all patients with a visit record within the reporting window (not restricted to TX_CURR so analysts can see all statuses). `lfu.visit_date` is already guaranteed to be `<= v_end` by the base `FollowUp` CTE gate; the explicit check here makes the intent clear.

---

## Out of Scope

- **Target Population PHRH label expansion** (FSW, PWID, High Risk AYGW, etc.) — requires the external "People at High Risk of HIV (PHRH) Reports_FINAL_111825" document.
- **Full NCD rescreening interval rules** — `ncd_screening_eligibility_date` interval logic requires the external "NCD Screening & PHRH Line Lists_111825" Excel document.
- **Historical CXCA re-evaluation** — requires raw VIA/HPV encounter dates; use `sp_fact_line_list_providers_view_query` for exact historical CXCA status.
