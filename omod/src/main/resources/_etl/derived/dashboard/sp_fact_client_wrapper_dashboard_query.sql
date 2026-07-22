-- Thin COUNT()/GROUP BY wrapper over the already-ETL'd mamba_fact_client table
-- (see sp_fact_client_create.sql / sp_fact_client_insert.sql for the column derivations
-- referenced below) -- no new per-client business logic, per the KPI dashboard's "cheap
-- wrapper" gap analysis. One SP, several branches, instead of one SP per KPI.
--
-- Branch dispatch on AGGREGATION_TYPE mirrors the REPORT_TYPE convention already used in
-- sp_dim_tx_pvls_datim_query.sql. Covers: KPI-01 (Registered Clients), KPI-02 (TX_Curr),
-- KPI-07 (VL Performance), KPI-08 (Service Coverage), KPI-09 (Co-Infections),
-- KPI-11 (Possible AHD), KPI-13 (Risk Assessment / Target Population).
DELIMITER //

DROP PROCEDURE IF EXISTS sp_fact_client_wrapper_dashboard_query;

CREATE PROCEDURE sp_fact_client_wrapper_dashboard_query(
    IN AGGREGATION_TYPE VARCHAR(50),
    IN REPORT_START_DATE DATE,
    IN REPORT_END_DATE DATE
)
BEGIN

    IF AGGREGATION_TYPE = 'REGISTERED_CLIENTS' THEN
        -- KPI-01: cumulative registrations, ART-enrolled vs not, by age-band x sex.
        SELECT CASE
                   WHEN TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) < 15 THEN '<15'
                   ELSE '>=15' END AS age_band,
               sex,
               SUM(art_start_date IS NOT NULL)                                                     AS art_enrolled,
               SUM(art_start_date IS NULL)                                                         AS not_enrolled,
               COUNT(*)                                                                             AS total
        FROM mamba_fact_client
        GROUP BY age_band, sex WITH ROLLUP;

    ELSEIF AGGREGATION_TYPE = 'CO_INFECTIONS' THEN
        -- KPI-09: TB (active) / CXCA (confirmed) / HTN-only / DM-only / HTN&DM, excl. Dead/TO/Stopped.
        -- "TB (active)" was originally `tb_treatment_rx_status NOT NULL AND <> 'Completed TB
        -- Treatment'` -- on review this inherited a real gap in tb_treatment_rx_status itself
        -- (sp_fact_client_insert.sql:691-701): that CASE only checks tb_treatment_completed_date,
        -- never tb_treatment_discontinued_date, so a client who discontinued (but didn't
        -- "complete") TB treatment would still read 'On TB Treatment' and get counted as active.
        -- Replaced below with the same date-window logic the canonical sp_dim_tb_art_datim_query.sql
        -- (lines 100-108) uses for its TB_ART cohort -- diagnosed/started by REPORT_END_DATE, and
        -- either (no discontinue/complete yet, within the default 1-year treatment window) or
        -- (discontinued/completed AFTER REPORT_END_DATE, i.e. was still active as of that date).
        -- Uses active_tb_diagnosis_date / tb_treatment_start_date / tb_treatment_completed_date /
        -- tb_treatment_discontinued_date, all already exposed on mamba_fact_client
        -- (sp_fact_client_insert.sql INSERT column list, lines 9-10).
        SELECT SUM(
                       (active_tb_diagnosis_date <= COALESCE(REPORT_END_DATE, CURDATE())
                           OR tb_treatment_start_date <= COALESCE(REPORT_END_DATE, CURDATE()))
                       AND (
                           (tb_treatment_discontinued_date IS NULL AND tb_treatment_completed_date IS NULL
                               AND DATE_ADD(tb_treatment_start_date, INTERVAL 1 YEAR) >= COALESCE(REPORT_END_DATE, CURDATE()))
                               OR (tb_treatment_completed_date > COALESCE(REPORT_END_DATE, CURDATE())
                               OR tb_treatment_discontinued_date > COALESCE(REPORT_END_DATE, CURDATE()))
                           )
                   )                                                                                           AS tb_active,
               SUM(cxca_screening_status = 'Confirmed CXCA (RED)')                                             AS cxca_confirmed,
               SUM(ncd_screening_status = 'Confirmed HTN')                                                     AS htn_only,
               SUM(ncd_screening_status = 'Confirmed DM')                                                      AS dm_only,
               SUM(ncd_screening_status = 'Confirmed DM & HTN')                                                AS htn_and_dm
        FROM mamba_fact_client
        WHERE current_status NOT IN ('Dead', 'Transferred Out', 'Stop all');

    ELSEIF AGGREGATION_TYPE = 'AHD' THEN
        -- KPI-11: Possible AHD total + sub-criteria (children<5 / WHO III-IV / CD4<200), not
        -- mutually exclusive -- recomputed independently from age/who_stage/cd4_result columns,
        -- since mamba_fact_client.advanced_hiv_disease itself is a single collapsed
        -- Yes/No flag (OR of all three criteria) and doesn't expose which criterion matched.
        SELECT SUM(advanced_hiv_disease = 'Yes')                                                                              AS total_ahd,
               SUM(advanced_hiv_disease = 'Yes' AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) < 5) AS ahd_under5,
               SUM(advanced_hiv_disease = 'Yes' AND who_stage IN ('WHO stage 3 adult', 'WHO stage 3 peds',
                                                                    'WHO stage 4 peds', 'WHO stage 4 adult'))                  AS ahd_who_stage_3_4,
               SUM(advanced_hiv_disease = 'Yes' AND cd4_result IS NOT NULL AND cd4_result < 200)                              AS ahd_cd4_under200
        FROM mamba_fact_client;

    ELSEIF AGGREGATION_TYPE = 'TARGET_POPULATION' THEN
        -- KPI-13: TX_Curr assessed vs not, by target_population category.
        -- (b) STILL OPEN: target_population is `COALESCE(phrh.target_population, c.key_population,
        --   '-')` (sp_fact_client_insert.sql:664), sourced from the PHRH followup form concept
        --   ca2c04ba-d9bd-4bad-ab03-e57ea9e49016 (_etl/config/phrh_followup.json:25) with a
        --   `client.key_population` fallback whose own source wasn't traced further in this pass.
        --   Neither concept's actual answer set (the real raw strings this column can hold) is
        --   enumerable from this repo
        SELECT COALESCE(NULLIF(target_population, '-'), 'Not Assessed') AS category,
               COUNT(*)                                                  AS client_count
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
        GROUP BY category WITH ROLLUP;

    ELSEIF AGGREGATION_TYPE = 'TX_CURR' THEN
        -- KPI-02: Currently on ART (TX_Curr), disaggregated by client-type and by regimen line.
        -- "client_type" is not a stored column -- approximated from transfer_in_date /
        -- art_start_date falling inside the reporting window [REPORT_START_DATE,
        -- REPORT_END_DATE], mirroring how TX_NEW/TI are period-scoped in the canonical DATIM
        -- procedures (sp_dim_tx_new_datim_query.sql / sp_dim_tx_curr_datim_query.sql). A client
        -- whose transfer-in AND ART start both fall in the window is counted as Transfer-In
        -- (transfer takes precedence, matching TX_NEW's own exclusion of TI clients).
        -- "regimen_line" only distinguishes First/Second/Other -- the underlying column
        -- (sp_fact_client_insert.sql:580-585, derived from `regimen LIKE '1%'/'2%'`) has no
        -- separate Third Line bucket, unlike the KPI mockup's 3-tier breakdown.
        -- Two disaggregations unioned into one result set (dimension/category/client_count)
        -- rather than two separate SPs, per this file's existing "one SP, several branches"
        -- convention; the frontend filters by `dimension` and sums either group for the total.
        SELECT 'client_type' AS dimension,
               CASE
                   WHEN transfer_in_date IS NOT NULL
                       AND transfer_in_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE THEN 'Transfer-In'
                   WHEN art_start_date IS NOT NULL
                       AND art_start_date BETWEEN REPORT_START_DATE AND REPORT_END_DATE THEN 'Newly Started'
                   ELSE 'Previously Started'
                   END          AS category,
               COUNT(*)         AS client_count
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
        GROUP BY dimension, category

        UNION ALL

        SELECT 'regimen_line' AS dimension,
               regimen_line   AS category,
               COUNT(*)       AS client_count
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
        GROUP BY dimension, category;

    ELSEIF AGGREGATION_TYPE = 'VL_PERFORMANCE' THEN
        -- KPI-07: VL Suppression Rate among active TX_Curr clients, computed for real.
        -- VL Testing Coverage is a HARDCODED PLACEHOLDER (784/1000 = 78.4%, matching the mockup's
        -- sample figure) -- TODO(owner): replace with the real eligibility/coverage definition
        -- before this ships; tracked on the frontend TODO list alongside the other pending KPI-08
        -- metrics. Left as a placeholder rather than the previous vl_eligibility_date-based guess,
        -- per explicit request, since that definition hadn't been confirmed against real business
        -- rules the way VL Suppression Rate's is_suppressed/last_vl_date columns have been.
        -- VL Re-Suppression Rate (in the mockup) is NOT computable and intentionally omitted:
        -- mamba_fact_client stores only each client's MOST RECENT VL result
        -- (last_vl_date/last_vl_result/is_suppressed) -- there is no prior-result history to
        -- detect "was unsuppressed, now suppressed" from.
        -- Suppression is scoped to results from the trailing 12 months of REPORT_END_DATE,
        -- matching the mockup's own "TX_PVLS: VL <1000 copies/ml, last 12 months" footnote.
        SELECT
            1000                                                                                     AS vl_coverage_denominator,
            784                                                                                       AS vl_coverage_numerator,
            SUM(is_suppressed IS NOT NULL
                AND last_vl_date >= DATE_SUB(COALESCE(REPORT_END_DATE, CURDATE()), INTERVAL 1 YEAR)) AS vl_suppression_denominator,
            SUM(is_suppressed = 1
                AND last_vl_date >= DATE_SUB(COALESCE(REPORT_END_DATE, CURDATE()), INTERVAL 1 YEAR)) AS vl_suppression_numerator
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE());

    ELSEIF AGGREGATION_TYPE = 'SERVICE_COVERAGE' THEN
        -- KPI-08: coverage percentages, plus a DSD-category breakdown. Definitions below were
        -- corrected against this module's existing HMIS v2 dashboard queries (the canonical
        -- reference for these indicators) rather than guessed from mamba_fact_client
        -- alone:
        --   * Modern FP: sp_fact_hmis_hiv_fp_query_v2.sql resolves the "modern method" question
        --     this branch previously couldn't -- its `tmp_fp` CTE defines modern FP as
        --     method_of_family_planning NOT IN ('None', 'Sexual abstinence'), scoped to TX_Curr,
        --     non-pregnant, female, 15-49. Reproduced here directly against family_planning_method
        --     since that column is the same COALESCE(fp.method_of_family_planning, '-') source
        --     (sp_fact_client_insert.sql:606).
        --   * DSD categories: sp_fact_hmis_hiv_dsd_query_v2.sql enumerates the real 10 raw
        --     dsd_category values (3MMD / Appointment spacing model (6MMD) / Fast track
        --     antiretroviral refill / Health extension professional led community / Community
        --     based group model by peer / DSD for adolescent / DSD for key populations / DSD for
        --     maternal child health / Other / Advanced HIV disease model) -- these ARE the
        --     curated category set, not raw noise; the previous "uncurated" caveat here was wrong.
        --     Its own per-category age>=15 gates (for the adult-only categories) aren't
        --     reproduced below for simplicity -- this branch counts every non-null dsd_category
        --     among TX_Curr clients regardless of age.
        --   * CXCA / TPT / Address: denominators broadened from "TX_Curr only" to the metric's
        --     own natural population (all registered clients matching the metric's demographic
        --     filter, not just those currently on ART) -- CXCA and FP eligibility, TPT
        --     start/completion, and address collection all happen independently of current
        --     TX_Curr status.
        -- Six metric rows unioned with the DSD breakdown into one result set
        -- (dimension/category/numerator/denominator), per this file's "one SP, several
        -- branches" convention.
        SELECT 'metric' AS dimension, 'CXCA_SCREENING' AS category,
               SUM(sex = 'Female'
                   AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) BETWEEN 25 AND 65
                   AND cxca_screening_status IN ('Confirmed CXCA (RED)', 'Previously Screened (Green)')) AS numerator,
               SUM(sex = 'Female'
                   AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) BETWEEN 25 AND 65) AS denominator
        FROM mamba_fact_client

        UNION ALL

        -- Matches sp_fact_line_list_tpt_linelist_query.sql's own tpt_start_date/tpt_completed_date
        -- columns -- "started" is cumulative up to REPORT_END_DATE (not window-scoped), per the
        -- KPI-08 definition given for this metric.
        SELECT 'metric', 'TPT_COMPLETION',
               SUM(tpt_completed_date IS NOT NULL AND tpt_completed_date <= COALESCE(REPORT_END_DATE, CURDATE())),
               SUM(tpt_start_date IS NOT NULL AND tpt_start_date <= COALESCE(REPORT_END_DATE, CURDATE()))
        FROM mamba_fact_client

        UNION ALL

        SELECT 'metric', 'ADDRESS_COMPLETENESS',
               SUM(address_completeness = 'GREEN'),
               COUNT(*)
        FROM mamba_fact_client

        UNION ALL

        -- Modern FP: TX_Curr, female, non-pregnant, 15-49 -- see header comment for the
        -- sp_fact_hmis_hiv_fp_query_v2.sql definition this reproduces. That reference query
        -- checks `pregnancy_status IS NULL` against mamba_fact_follow_up, where the column can
        -- genuinely be NULL; mamba_fact_client instead COALESCEs it to '-'
        -- (sp_fact_client_insert.sql:599), so an IS NULL check here would be dead code and
        -- silently drop every client with unknown pregnancy status from BOTH sides of the ratio
        -- (inflating the %, not just shrinking the denominator). `<> 'Yes'` correctly treats
        -- "not known to be pregnant" (No, or unrecorded) as eligible, matching the intent.
        SELECT 'metric', 'MODERN_FP',
               SUM(sex = 'Female'
                   AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) BETWEEN 15 AND 49
                   AND pregnancy_status <> 'Yes'
                   AND tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
                   AND family_planning_method NOT IN ('-', 'None', 'Sexual abstinence')),
               SUM(sex = 'Female'
                   AND TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) BETWEEN 15 AND 49
                   AND pregnancy_status <> 'Yes'
                   AND tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE()))
        FROM mamba_fact_client

        UNION ALL

        -- DSD and ICT stay TX_Curr-scoped: both are ART-service-delivery indicators that only
        -- apply to clients currently in care (sp_fact_hmis_hiv_dsd_query_v2.sql's own denominator
        -- is likewise its tx_curr CTE, not all registered clients).
        SELECT 'metric', 'DSD_ENROLLMENT',
               SUM(dsd_category IS NOT NULL AND dsd_category <> '-'),
               COUNT(*)
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())

        UNION ALL

        SELECT 'metric', 'ICT_OFFERING',
               SUM(TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) >= 15
                   AND ict_screening_status <> 'Not Screened'),
               SUM(TIMESTAMPDIFF(YEAR, birthdate, COALESCE(REPORT_END_DATE, CURDATE())) >= 15)
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())

        UNION ALL

        SELECT 'dsd_category' AS dimension,
               dsd_category   AS category,
               COUNT(*)       AS numerator,
               NULL           AS denominator
        FROM mamba_fact_client
        WHERE tx_curr_end_date >= COALESCE(REPORT_END_DATE, CURDATE())
          AND dsd_category IS NOT NULL
          AND dsd_category <> '-'
        GROUP BY category;

    END IF;
END //

DELIMITER ;
