# EthiOHRI Mamba Report API — Frontend Developer Guide

## 1. API Architecture

**Base URL:** `{openmrs_base}/ws/rest/v1/ethiohri-mamba/reports/execute/{procedureName}`

**Authentication:** Standard OpenMRS basic auth or session cookie.

**Two HTTP methods:**

```
GET  .../execute/{procedureName}?param1=value&param2=value&offset=0&limit=0
POST .../execute/{procedureName}?offset=0&limit=0
     Content-Type: application/json
     Body: { "param1": "value", "param2": "value" }
```

**Pagination:** `offset` (0-based) and `limit` (0 = all rows).  
**Date format:** All dates are Gregorian `yyyy-MM-dd` (e.g., `2024-09-30`).

**Response shape (always):**
```json
{
  "procedureName": "sp_dim_tx_curr_datim_query",
  "rowCount": 42,
  "data": [
    { "column_name": "value", "..." : "..." }
  ]
}
```

---

## 2. Key Design Patterns

### Pattern A — Single call, single parameter set
One `GET` returns the full section. Most line-list reports work this way.

### Pattern B — Multiple calls, one per `aggregationType`
Most DATIM indicator sections are disaggregated. The frontend calls the same `procedureName` once per disaggregate, passing a different `aggregationType` value each time. Each call returns independent rows for that section.

### Pattern C — Virtual composite SP name
Several "virtual" procedure names bundle multiple stored procedures into one server-side call, returning all result rows concatenated in `data`.

| Virtual SP name | Internally runs |
|---|---|
| `sp_fact_line_list_art_cohort_analysis` | art_cohort_analysis_query + art_cohort_analysis_summary_query |
| `sp_fact_line_list_child_cohort_analysis` | child_cohort_analysis_query + child_cohort_analysis_summary_query |
| `sp_fact_line_list_mother_cohort_analysis` | mother_cohort_analysis_query + mother_cohort_analysis_summary_query |
| `sp_fact_line_list_tb_prev_linelist` | tx_tbt_numerator_query + tx_tbt_denominator_query |
| `sp_fact_line_list_vl_sent_received` | vl_sent_query + vl_received_query |
| `sp_fact_hmis_all` | All 20 HMIS stored procedures |

### Pattern D — TX_CURR (special case)
`sp_dim_tx_curr_datim_query` always fires all 5 internal sub-calls (CD4 ×3, age/sex, numerator) in one request regardless of any parameter. The `data` array contains rows from all sub-calls concatenated. No `aggregationType` param is used.

---

## 3. DATIM Reports

### 3.1 TX_CURR — "DATIM TREATMENT- TX_CURR"

**Params:** `endDate`  
**SP:** `sp_dim_tx_curr_datim_query`  
**Pattern D — always all 5 sub-calls, 1 API call total**

```
GET .../execute/sp_dim_tx_curr_datim_query?endDate=2024-09-30
```

Returns all sections (Numerator, Age/Sex, CD4) concatenated. Distinguish sections by inspecting column names in the row objects — they differ between sub-procedure result sets.

---

### 3.2 TX_NEW — "DATIM TREATMENT- TX_NEW"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_new_datim_query`  
**6 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Numerator (auto-calc) | `numerator` |
| <200 CD4 | `<200` |
| ≥200 CD4 | `>=200` |
| Unknown CD4 | `unknown` |
| Not Tested | `not_tested` |
| Breastfeeding at ART Initiation | `breast_feeding` |

```
GET .../execute/sp_dim_tx_new_datim_query?startDate=2024-07-01&endDate=2024-09-30&aggregationType=numerator
GET .../execute/sp_dim_tx_new_datim_query?startDate=2024-07-01&endDate=2024-09-30&aggregationType=%3C200
...one request per aggregationType value
```

---

### 3.3 TX_ML — "DATIM TREATMENT- TX_ML"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_ml_datim_query`  
**7 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (numerator auto-calc) | `TOTAL` |
| Died | `DIED` |
| IIT < 3 months | `LESS_THAN_THREE_MONTHS` |
| IIT 3–5 months | `THREE_TO_FIVE_MONTHS` |
| IIT 6+ months | `MORE_THAN_SIX_MONTHS` |
| Transferred Out | `TRANSFERRED_OUT` |
| Refused / Stopped Treatment | `REFUSED` |

---

### 3.4 TX_RTT — "DATIM TREATMENT- TX_RTT"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_rtt_datim_query`  
**6 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (numerator auto-calc) | `TOTAL` |
| <200 CD4 | `CD4_LESS_THAN_200` |
| ≥200 CD4 | `CD4_GREATER_THAN_200` |
| Unknown CD4 | `CD4_UNKNOWN` |
| Not Eligible for CD4 | `CD4_NOT_ELIGIBLE` |
| IIT | `IIT` |

---

### 3.5 TX_PVLS Numerator — "DATIM VIRAL SUPPRESSION- TX_PVLS (Numerator)"

**Params:** `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_pvls_datim_query`  
**3 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Numerator total | `NUMERATOR_TOTAL` |
| Age/Sex disaggregate | `NUMERATOR` |
| Pregnant / Breastfeeding | `NUMERATOR_BREAST_FEEDING_PREGNANT` |

---

### 3.6 TX_PVLS Denominator — "DATIM VIRAL SUPPRESSION- TX_PVLS (Denominator)"

**Params:** `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_pvls_datim_query`  
**4 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Denominator total | `DENOMINATOR_TOTAL` |
| Age/Sex disaggregate | `DENOMINATOR` |
| Pregnant / Breastfeeding | `DENOMINATOR_BREAST_FEEDING_PREGNANT` |
| Unknown | `UNKNOWN` |

---

### 3.7 TX_TB Denominator — "DATIM TREATMENT- TX_TB (Denominator)"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_tb_datim_query`  
**9 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `TOTAL` |
| New on ART / Screen Positive | `NEW_ART_POSITIVE` |
| New on ART / Screen Negative | `NEW_ART_NEGATIVE` |
| Already on ART / Screen Positive | `PREV_ART_POSITIVE` |
| Already on ART / Screen Negative | `PREV_ART_NEGATIVE` |
| Screen Type disaggregate | `SCREEN_TYPE` |
| Specimen Sent | `SPECIMEN_SENT` |
| Diagnostic Test | `DIAGNOSTIC_TEST` |
| Positive Result Returned | `POSITIVE_RESULT` |

---

### 3.8 TX_TB Numerator — "DATIM TREATMENT- TX_TB (Numerator)"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tx_tb_datim_query`  
**3 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `NUMERATOR_TOTAL` |
| New on ART | `NUMERATOR_NEW` |
| Already on ART | `NUMERATOR_PREV` |

---

### 3.9 TB_ART — "DATIM TREATMENT- TB_ART"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tb_art_datim_query`  
**3 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `TOTAL` |
| Already on ART | `ALREADY_ON_ART` |
| New on ART | `NEW_ON_ART` |

---

### 3.10 TB_PREV Numerator — "DATIM PREVENTION- TB_PREV(NUMERATOR)"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tb_prev_datim_numerator_query`  
**2 API calls** per full report render.

> **Important:** Passing `aggregationType=PREV_ART` triggers **two internal sub-calls** (NEW_ART + PREV_ART) whose rows are concatenated in `data`. Distinguish ART-start groups by the ART-start type column in the response. Pass `TOTAL` for a single-call total. Pass `DEBUG` for debug/checker mode (single call).

| Section | `aggregationType` | Internal sub-calls |
|---|---|---|
| Total | `TOTAL` | 1 |
| Disaggregated by ART Start (New + Prev) | `PREV_ART` | 2 (concatenated) |

---

### 3.11 TB_PREV Denominator — "DATIM PREVENTION- TB_PREV(DENOMINATOR)"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_tb_prev_datim_denominator_query`  
**2 API calls** per full report render.

Same behavior as TB_PREV Numerator.

| Section | `aggregationType` | Internal sub-calls |
|---|---|---|
| Total | `TOTAL` | 1 |
| Disaggregated by ART Start (New + Prev) | `PREV_ART` | 2 (concatenated) |

---

### 3.12 HTS_INDEX — "DATIM TESTING- HTS_INDEX"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_ict_datim_query`  
**8 API calls** per full report render.

> **Important:** Every call to this SP always fires **two internal sub-calls** (elicited flag=1 + non-elicited flag=0) regardless of `aggregationType`. Both sub-call results are concatenated in `data`. The `aggregationType` controls the disaggregation category within each sub-call.

| Section | `aggregationType` |
|---|---|
| Total | `ICT_TOTAL` |
| Testing Offered (index cases) | `TESTING_OFFERED` |
| Testing Accepted | `TESTING_ACCEPTED` |
| Contacts Elicited | `ELICITED` |
| Known Positives | `KNOWN_POSITIVE` |
| Newly Tested Positives | `NEW_POSITIVE` |
| New Negatives | `NEW_NEGATIVE` |
| Documented Negatives | `DOCUMENTED_NEGATIVE` |

---

### 3.13 CXCA_SCRN — "DATIM TESTING- CXCA_SCRN"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_cxca_scrn_datim_query`  
**4 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `TOTAL` |
| First-time Screened | `FIRST_TIME_SCREENING` |
| Rescreened | `RE_SCREENING` |
| Post-treatment Follow-up | `POST_TREATMENT` |

---

### 3.14 CXCA_TX — "DATIM TESTING- CXCA_TX"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_cxca_tx_datim_query`  
**4 API calls** per full report render.

Same `aggregationType` values as CXCA_SCRN: `TOTAL`, `FIRST_TIME_SCREENING`, `RE_SCREENING`, `POST_TREATMENT`.

---

### 3.15 PrEP_CT — "DATIM PREVENTION- PrEP_CT"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_prep_ct_datim_query`  
**6 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `TOTAL` |
| Age/Sex | `AGE_SEX` |
| Test Result | `TEST_RESULT` |
| PrEP Type | `PREP_TYPE` |
| Pregnant / Breastfeeding | `PREGNANT_BF` |
| PrEP Distribution | `FACILITY` |

---

### 3.16 PrEP_NEW — "DATIM PREVENTION- PrEP_NEW"

**Params:** `startDate`, `endDate`, `aggregationType`  
**SP:** `sp_dim_prep_new_datim_query`  
**5 API calls** per full report render.

| Section | `aggregationType` |
|---|---|
| Total (auto-calc) | `TOTAL` |
| Age/Sex | `AGE_SEX` |
| Pregnant / Breastfeeding | `PREGNANT_BF` |
| PrEP Distribution | `FACILITY` |
| PrEP Type | `PREP_TYPE` |

---

### 3.17 PMTCT_FO — "DATIM TREATMENT- PMTCT_FO"

**Params:** `startDate`, `endDate`, `reportType`  
**SP:** `sp_dim_pmtct_fo_datim_query`  
**2 API calls** per full report render.

| Section | `reportType` |
|---|---|
| Denominator (auto-calc) | `DENOMINATOR` |
| Numerator disaggregated by outcome | `DISAGGREGATED` |

---

### 3.18 PMTCT_HEI — "DATIM TREATMENT- PMTCT_HEI"

**Params:** `startDate`, `endDate`, `reportType`  
**SP:** `sp_dim_pmtct_hei_datim_query`  
**2 API calls** per full report render.

| Section | `reportType` |
|---|---|
| Numerator (auto-calc) | `NUMERATOR` |
| Disaggregated by age at sample collection | `DISAGGREGATED` |

---

### 3.19 PMTCT_EID — "DATIM TREATMENT- PMTCT_EID"

**Params:** `startDate`, `endDate`, `reportType`  
**SP:** `sp_dim_pmtct_eid_datim_query`  
**2 API calls** per full report render.

Same `reportType` values as PMTCT_HEI: `NUMERATOR`, `DISAGGREGATED`.

---

## 4. HMIS Report — "HMIS- HMIS DHIS 2"

### Option A — Combined (recommended)

Single API call. All 20 HMIS stored procedures run server-side and their rows are concatenated in `data`. Distinguish sections by inspecting column names.

```
GET .../execute/sp_fact_hmis_all?startDate=2024-07-01&endDate=2024-09-30
```

### Option B — Individual calls (for rendering sections separately)

| Section | SP name | Params |
|---|---|---|
| HTS Index | `sp_fact_hmis_hiv_hts_tst_index_query` | startDate, endDate |
| TX_CURR | `sp_fact_hmis_tx_curr_query` | endDate |
| TX_NEW | `sp_fact_hmis_tx_new_query` | startDate, endDate |
| ART Retention | `sp_fact_hmis_hiv_art_ret_query` | startDate, endDate |
| ART Retention Net | `sp_fact_hmis_hiv_art_ret_net_query` | startDate, endDate |
| HIV Linkage | `sp_fact_hmis_hiv_linkage_query` | startDate, endDate |
| TX_PVLS | `sp_fact_hmis_hiv_tx_pvls_query` | startDate, endDate |
| DSD | `sp_fact_hmis_hiv_dsd_query` | endDate |
| ART Interruption | `sp_fact_hmis_art_intr_query` | startDate, endDate |
| ART Restart | `sp_fact_hmis_art_restart_query` | startDate, endDate |
| PrEP | `sp_fact_hmis_hiv_prep_query` | startDate, endDate |
| PEP | `sp_fact_hmis_hiv_pep_query` | startDate, endDate |
| PHLIV TSP | `sp_fact_hmis_phliv_tsp_query` | startDate, endDate |
| FP | `sp_fact_hmis_hiv_fp_query` | endDate |
| TB Screening | `sp_fact_hmis_hiv_tb_scrn_query` | startDate, endDate |
| TPT | `sp_fact_hmis_hiv_tpt_query` | startDate, endDate |
| CXCA Screening | `sp_fact_hmis_cxca_scrn_query` | startDate, endDate |
| CXCA Treatment | `sp_fact_hmis_cxca_rx_query` | startDate, endDate |
| TB Lab LF-LAM | `sp_fact_hmis_tb_lb_lf_lam_query` | startDate, endDate |
| MTCT | `sp_fact_hmis_mtct_query` | startDate, endDate |

---

## 5. Line List Reports

All line lists are **1 API call per report** unless noted otherwise.

### 5.1 endDate Only

| Report name | SP name |
|---|---|
| AHD | `sp_fact_line_list_ahd_query` |
| VL Eligibility | `sp_fact_line_list_vl_eligibility_query` |
| CXCA Eligibility | `sp_fact_line_list_cxca_eligibility_query` |
| Missed Appointment | `sp_fact_line_list_missed_appointment_query` |
| PrEP (Pre-Exposure) | `sp_fact_line_list_pre_exposure_query` |
| TX_CURR | `sp_fact_line_list_tx_curr_query` |

```
GET .../execute/sp_fact_line_list_ahd_query?endDate=2024-09-30
```

### 5.2 startDate + endDate

| Report name | SP name |
|---|---|
| ART Retention | `sp_fact_line_list_art_retention_query` |
| CXCA Screening | `sp_fact_line_list_cxca_scrn_query` |
| DSD | `sp_fact_line_list_dsd_query` |
| EID DNA PCR | `sp_fact_line_list_eid_dna_pcr_query` |
| EID Rapid Antibody | `sp_fact_line_list_eid_rapid_antibody_query` |
| HEI | `sp_fact_line_list_hei_query` |
| HVL (High Viral Load) | `sp_fact_line_list_hvl_query` |
| ICT Contacts | `sp_fact_line_list_ict_contacts_query` |
| ICT Screening | `sp_fact_line_list_ict_screening_query` |
| Maternal PMTCT | `sp_fact_line_list_maternal_pmtct_query` |
| Monthly Visit Care | `sp_fact_line_list_monthly_visit_care_query` |
| NCD Screening | `sp_fact_line_list_ncd_screening_query` |
| NCD Treatment | `sp_fact_line_list_ncd_treatment_query` |
| OTZ | `sp_fact_line_list_otz_query` |
| Pediatric Age Out | `sp_fact_line_list_pediatric_age_out_query` |
| HIV Positive Tracking | `sp_fact_line_list_positive_tracking_query` |
| PEP (Post-Exposure) | `sp_fact_line_list_post_exposure_query` |
| Re-Test | `sp_fact_line_list_re_test_query` |
| Schedule Visit | `sp_fact_line_list_schedule_visit_query` |
| TX_ML | `sp_fact_line_list_tx_ml_query` |
| TX_NEW | `sp_fact_line_list_tx_new_query` |
| TX_RTT | `sp_fact_line_list_tx_rtt_query` |
| TX_TB ART | `sp_fact_line_list_tx_tb_art_query` |
| TX_TB Denominator | `sp_fact_line_list_tx_tb_denominator_query` |
| TX_TB Numerator | `sp_fact_line_list_tx_tb_numerator_query` |

```
GET .../execute/sp_fact_line_list_hvl_query?startDate=2024-07-01&endDate=2024-09-30
```

### 5.3 startDate + endDate + Extra Required Param

| Report name | SP name | Extra param | Valid values |
|---|---|---|---|
| Chronic Care | `sp_fact_line_list_chronic_care_query` | `targetGroup` | Follow-up status string |
| PHRH Service | `sp_fact_line_list_phrh_service_query` | `targetGroup` | Target group string |
| PHRH SNS | `sp_fact_line_list_phrh_sns_query` | `phrhCode` | PHRH code string |
| TPT/CPT/FPT | `sp_fact_line_list_tpt_linelist_query` | `tptType` | `ALL`, `CPT`, `TPT`, `FPT` |
| TX_CURR Analysis | `sp_fact_line_list_tx_curr_analysis_query` | `txCurrAnalysisCategories` | See table below |

**TX_CURR Analysis — `txCurrAnalysisCategories` values:**

| Display label | Value to pass |
|---|---|
| TX_Curr Summary | `Tx_Curr Summary` |
| Previous Month TX_Curr | `Previous Month Tx_Curr` |
| Excluded From TX_Curr | `Excluded From Tx_Curr` |
| Not Updated | `Not Updated` |
| Newly Included TX_Curr | `Newly Included Tx_Curr` |
| This Month TX_Curr | `This Month TX_Curr` |
| On DSD | `Tx_Curr On DSD` |
| Other Outcome | `Other Outcome` |

### 5.4 Special-Parameter Line Lists

**TI/TO — Transferred In / Transferred Out**  
Call the appropriate SP directly based on user selection (`status=TI` or `status=TO`):

```
GET .../execute/sp_fact_line_list_ti_query?startDate=2024-07-01&endDate=2024-09-30
GET .../execute/sp_fact_line_list_to_query?startDate=2024-07-01&endDate=2024-09-30
```

**Patient Summary**

```
GET .../execute/sp_fact_line_list_patient_summary_query?patientUUID={patient-uuid}
```

**Providers View / Data Quality and Data Use**

```
GET .../execute/sp_fact_line_list_providers_view_query?endDate=2024-09-30&clientType=TX_CURR&patientGUID={optional-uuid}
```

- `clientType`: `ALL` or `TX_CURR`
- `patientGUID`: optional; omit or leave empty to return all patients

### 5.5 Virtual Composite Line Lists (1 API call, multiple SPs server-side)

| Report name | Virtual SP name | Params | Internally runs |
|---|---|---|---|
| VL Sent / Received | `sp_fact_line_list_vl_sent_received` | startDate, endDate | vl_sent_query + vl_received_query |
| TB Prev Line List | `sp_fact_line_list_tb_prev_linelist` | startDate, endDate | tx_tbt_numerator_query + tx_tbt_denominator_query |
| ART Cohort Analysis | `sp_fact_line_list_art_cohort_analysis` | startDate, endDate | art_cohort_analysis_query + art_cohort_analysis_summary_query |
| Child Cohort Analysis | `sp_fact_line_list_child_cohort_analysis` | startDate, endDate | child_cohort_analysis_query + child_cohort_analysis_summary_query |
| Mother Cohort Analysis | `sp_fact_line_list_mother_cohort_analysis` | startDate, endDate | mother_cohort_analysis_query + mother_cohort_analysis_summary_query |

```
GET .../execute/sp_fact_line_list_vl_sent_received?startDate=2024-07-01&endDate=2024-09-30
```

---

## 6. API Call Count Summary

| Report | API calls per render |
|---|---|
| TX_CURR DATIM | 1 (all-in-one) |
| TX_NEW DATIM | 6 |
| TX_ML DATIM | 7 |
| TX_RTT DATIM | 6 |
| TX_PVLS Numerator | 3 |
| TX_PVLS Denominator | 4 |
| TX_TB Denominator | 9 |
| TX_TB Numerator | 3 |
| TB_ART | 3 |
| TB_PREV Numerator | 2 |
| TB_PREV Denominator | 2 |
| HTS_INDEX | 8 |
| CXCA_SCRN | 4 |
| CXCA_TX | 4 |
| PrEP_CT | 6 |
| PrEP_NEW | 5 |
| PMTCT_FO | 2 |
| PMTCT_HEI | 2 |
| PMTCT_EID | 2 |
| HMIS DHIS2 (combined) | 1 (virtual SP) |
| All line lists | 1 each |

---

## 7. Implementation Notes

### Parallel requests
For reports with multiple sections (e.g., TX_ML's 7 calls), fire all section requests in parallel (`Promise.all`). Render each section independently as its data arrives to avoid blocking the full page on the slowest query.

### TX_CURR special case
The single call returns rows from 5 internal sub-procedures concatenated. Distinguish sections by inspecting column names in the row objects — they differ between the numerator, age/sex, and CD4 result sets.

### HTS_INDEX
Each API call returns rows from 2 sub-procedures (elicited + non-elicited) concatenated. The `aggregationType` controls the disaggregation category within each sub-call, not which sub-calls run.

### TB_PREV `PREV_ART` aggregation
Returns NEW_ART and PREV_ART rows concatenated in one response. Distinguish them by the ART-start type column in `data`.

### Error responses
| Status | Meaning |
|---|---|
| `400 Bad Request` | Unknown SP name or procedure name contains invalid characters (only `[a-zA-Z0-9_]` allowed) |
| `500 Internal Server Error` | Database error |

### Pagination for large line lists
```
GET .../execute/sp_fact_line_list_tx_curr_query?endDate=2024-09-30&offset=0&limit=100
GET .../execute/sp_fact_line_list_tx_curr_query?endDate=2024-09-30&offset=100&limit=100
```
Use `limit=0` to return all rows (default — suitable for reports rendered as tables or exported).
