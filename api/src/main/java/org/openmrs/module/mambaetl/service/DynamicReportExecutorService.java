package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBPrevAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAnalysisCategories;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DynamicReportExecutorService {
	
	private static final Log log = LogFactory.getLog(DynamicReportExecutorService.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	public List<Map<String, Object>> executeReport(String procedureName, Map<String, String> params, int offset,
	        int limit) throws SQLException {

		validateProcedureName(procedureName);

		SimpleDataSet data = new SimpleDataSet(null, null);
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = getProcedureCalls(procedureName, params);

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = DataSetEvaluatorHelper
			        .prepareStatements(connection, procedureCalls)) {

				DataSetEvaluatorHelper.executeStatements(statementContainer, procedureCalls);

				ResultSet[] allResultSets = statementContainer.getResultSets();
				DataSetEvaluatorHelper.mapResultSet(data, resultSetMapper, allResultSets, false);
				connection.commit();

				return mapAndPaginateData(data, offset, limit);

			}
			catch (SQLException e) {
				DataSetEvaluatorHelper.rollbackAndThrowException(connection,
				    ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		}
		catch (SQLException | EvaluationException e) {
			throw new SQLException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		return new ArrayList<>();
	}
	
	private void validateProcedureName(String procedureName) {
		if (!procedureName.matches("^[a-zA-Z0-9_]+$")) {
			throw new IllegalArgumentException(
			        "Invalid procedure name formatting. Only alphanumeric characters and underscores are allowed.");
		}
	}
	
	// -------------------------------------------------------------------------
	// Procedure mapping registry
	// -------------------------------------------------------------------------
	
	private List<DataSetEvaluatorHelper.ProcedureCall> getProcedureCalls(String name,
	        Map<String, String> params) {

		// --- Line lists: endDate only ---

		if ("sp_fact_line_list_ahd_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_ahd_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_cxca_eligibility_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_cxca_eligibility_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_missed_appointment_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_missed_appointment_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_pre_exposure_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_pre_exposure_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_tx_curr_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_tx_curr_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_vl_eligibility_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_vl_eligibility_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}

		// --- Line lists: startDate, endDate ---

		if ("sp_fact_line_list_art_retention_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_art_retention_query(?,?)}", params);
		}
		if ("sp_fact_line_list_cxca_scrn_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_cxca_scrn_query(?,?)}", params);
		}
		if ("sp_fact_line_list_dsd_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_dsd_query(?,?)}", params);
		}
		if ("sp_fact_line_list_eid_dna_pcr_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_eid_dna_pcr_query(?,?)}", params);
		}
		if ("sp_fact_line_list_eid_rapid_antibody_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_eid_rapid_antibody_query(?,?)}", params);
		}
		if ("sp_fact_line_list_hei_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_hei_query(?,?)}", params);
		}
		if ("sp_fact_line_list_hvl_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_hvl_query(?,?)}", params);
		}
		if ("sp_fact_line_list_ict_contacts_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_ict_contacts_query(?,?)}", params);
		}
		if ("sp_fact_line_list_ict_screening_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_ict_screening_query(?,?)}", params);
		}
		if ("sp_fact_line_list_maternal_pmtct_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_maternal_pmtct_query(?,?)}", params);
		}
		if ("sp_fact_line_list_ncd_screening_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_ncd_screening_query(?,?)}", params);
		}
		if ("sp_fact_line_list_ncd_treatment_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_ncd_treatment_query(?,?)}", params);
		}
		if ("sp_fact_line_list_otz_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_otz_query(?,?)}", params);
		}
		if ("sp_fact_line_list_pediatric_age_out_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_pediatric_age_out_query(?,?)}", params);
		}
		if ("sp_fact_line_list_positive_tracking_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_positive_tracking_query(?,?)}", params);
		}
		if ("sp_fact_line_list_post_exposure_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_post_exposure_query(?,?)}", params);
		}
		if ("sp_fact_line_list_re_test_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_re_test_query(?,?)}", params);
		}
		if ("sp_fact_line_list_schedule_visit_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_schedule_visit_query(?,?)}", params);
		}
		if ("sp_fact_line_list_ti_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_ti_query(?,?)}", params);
		}
		if ("sp_fact_line_list_to_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_to_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_ml_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_ml_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_new_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_new_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_rtt_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_rtt_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_tb_art_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_tb_art_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_tb_denominator_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_tb_denominator_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_tb_numerator_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_tb_numerator_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_tbt_numerator_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_tbt_numerator_query(?,?)}", params);
		}
		if ("sp_fact_line_list_tx_tbt_denominator_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_line_list_tx_tbt_denominator_query(?,?)}", params);
		}

		// --- Line lists: startDate, endDate, extra string param ---

		if ("sp_fact_line_list_chronic_care_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_chronic_care_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("targetGroup"));
			});
		}
		if ("sp_fact_line_list_monthly_visit_care_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_monthly_visit_care_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, "Alive,Restart medication");
			});
		}
		if ("sp_fact_line_list_phrh_service_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_phrh_service_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("targetGroup"));
			});
		}
		if ("sp_fact_line_list_phrh_sns_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_phrh_sns_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("phrhCode"));
			});
		}
		if ("sp_fact_line_list_tpt_linelist_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_tpt_linelist_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("tptType"));
			});
		}
		if ("sp_fact_line_list_tx_curr_analysis_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_tx_curr_analysis_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, TxCurrAnalysisCategories.fromString(params.get("txCurrAnalysisCategories")).name());
			});
		}

		// --- Line lists: special params ---

		if ("sp_fact_line_list_patient_summary_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_patient_summary_query(?)}", s -> {
				s.setString(1, params.get("patientUUID"));
			});
		}
		if ("sp_fact_line_list_providers_view_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_line_list_providers_view_query(?,?,?,?,?)}", s -> {
				s.setString(1, params.get("clientType"));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setDate(3, null);
				s.setDate(4, null);
				s.setString(5, params.get("patientGUID"));
			});
		}

		// --- Line lists: multi-call (combined evaluator pairs) ---

		if ("sp_fact_line_list_art_cohort_analysis".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_art_cohort_analysis_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_art_cohort_analysis_summary_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }));
		}
		if ("sp_fact_line_list_child_cohort_analysis".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_child_cohort_analysis_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }),
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_child_cohort_analysis_summary_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }));
		}
		if ("sp_fact_line_list_mother_cohort_analysis".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_mother_cohort_analysis_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }),
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_mother_cohort_analysis_summary_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }));
		}
		if ("sp_fact_line_list_tb_prev_linelist".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_tx_tbt_numerator_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_tx_tbt_denominator_query(?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
			            }));
		}
		if ("sp_fact_line_list_vl_sent_received".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_vl_sent_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_vl_received_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }));
		}

		// --- HMIS / DHIS2 procedures ---

		// Combined HMIS: mirrors HMISDHIS2DataSetEvaluator (all 20 SPs in one call)
		if ("sp_fact_hmis_all".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_hts_tst_index_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_tx_curr_query(?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_tx_new_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_art_ret_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_art_ret_net_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_linkage_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_tx_pvls_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_dsd_query(?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_art_intr_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_art_restart_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_prep_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_pep_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_phliv_tsp_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_fp_query(?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_tb_scrn_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_hiv_tpt_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_cxca_scrn_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_cxca_rx_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_tb_lb_lf_lam_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_hmis_mtct_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }));
		}

		if ("sp_fact_hmis_hiv_hts_tst_index_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_hts_tst_index_query(?,?)}", params);
		}
		if ("sp_fact_hmis_tx_curr_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_hmis_tx_curr_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_hmis_tx_new_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_tx_new_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_art_ret_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_art_ret_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_art_ret_net_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_art_ret_net_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_linkage_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_linkage_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_tx_pvls_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_tx_pvls_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_dsd_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_hmis_hiv_dsd_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_hmis_art_intr_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_art_intr_query(?,?)}", params);
		}
		if ("sp_fact_hmis_art_restart_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_art_restart_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_prep_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_prep_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_pep_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_pep_query(?,?)}", params);
		}
		if ("sp_fact_hmis_phliv_tsp_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_phliv_tsp_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_fp_query".equalsIgnoreCase(name)) {
			return single("{call sp_fact_hmis_hiv_fp_query(?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_hmis_hiv_tb_scrn_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_tb_scrn_query(?,?)}", params);
		}
		if ("sp_fact_hmis_hiv_tpt_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_hiv_tpt_query(?,?)}", params);
		}
		if ("sp_fact_hmis_cxca_scrn_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_cxca_scrn_query(?,?)}", params);
		}
		if ("sp_fact_hmis_cxca_rx_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_cxca_rx_query(?,?)}", params);
		}
		if ("sp_fact_hmis_tb_lb_lf_lam_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_tb_lb_lf_lam_query(?,?)}", params);
		}
		if ("sp_fact_hmis_mtct_query".equalsIgnoreCase(name)) {
			return dateRange("{call sp_fact_hmis_mtct_query(?,?)}", params);
		}

		// --- DATIM indicator procedures ---

		if ("sp_dim_tb_art_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tb_art_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_tx_tb_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tx_tb_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_tx_rtt_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tx_rtt_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_tx_new_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tx_new_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_tx_ml_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tx_ml_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_cxca_scrn_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_cxca_scrn_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_cxca_tx_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_cxca_tx_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_prep_ct_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_prep_ct_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_prep_new_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_prep_new_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, 0);
				s.setString(4, params.get("aggregationType"));
			});
		}
		if ("sp_dim_tx_pvls_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_tx_pvls_datim_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("endDate")));
				s.setInt(2, 0);
				s.setString(3, params.get("aggregationType"));
			});
		}
		if ("sp_dim_pmtct_fo_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_pmtct_fo_datim_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("reportType"));
			});
		}
		if ("sp_dim_pmtct_hei_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_pmtct_hei_datim_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("reportType"));
			});
		}
		if ("sp_dim_pmtct_eid_datim_query".equalsIgnoreCase(name)) {
			return single("{call sp_dim_pmtct_eid_datim_query(?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setString(3, params.get("reportType"));
			});
		}

		// HTS Index: two calls (ELICITED flag=1 + DEFAULT flag=0)
		if ("sp_dim_ict_datim_query".equalsIgnoreCase(name)) {
			String aggType = params.get("aggregationType");
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_ict_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
				    s.setInt(3, 1);
				    s.setString(4, aggType);
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_ict_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
				    s.setInt(3, 0);
				    s.setString(4, aggType);
			    }));
		}

		// TB Prev Numerator: TOTAL/DEBUG → 1 call; otherwise → NEW_ART + PREV_ART (2 calls)
		if ("sp_dim_tb_prev_datim_numerator_query".equalsIgnoreCase(name)) {
			if ("TOTAL".equalsIgnoreCase(params.get("aggregationType"))) {
				return single("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
					s.setInt(3, 1);
					s.setString(4, TBPrevAggregationTypes.TOTAL.getSqlValue());
				});
			}
			if ("DEBUG".equalsIgnoreCase(params.get("aggregationType"))) {
				return single("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
					s.setInt(3, 1);
					s.setString(4, TBPrevAggregationTypes.DEBUG.getSqlValue());
				});
			}
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
				    s.setInt(3, 1);
				    s.setString(4, TBPrevAggregationTypes.NEW_ART.getSqlValue());
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
				    s.setInt(3, 1);
				    s.setString(4, TBPrevAggregationTypes.PREV_ART.getSqlValue());
			    }));
		}

		// TB Prev Denominator: TOTAL/DEBUG → 1 call; otherwise → NEW_ART + PREV_ART (2 calls)
		if ("sp_dim_tb_prev_datim_denominator_query".equalsIgnoreCase(name)) {
			if ("TOTAL".equalsIgnoreCase(params.get("aggregationType"))) {
				return single("{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
					s.setInt(3, 1);
					s.setString(4, TBPrevAggregationTypes.TOTAL.getSqlValue());
				});
			}
			if ("DEBUG".equalsIgnoreCase(params.get("aggregationType"))) {
				return single("{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
					s.setInt(3, 1);
					s.setString(4, TBPrevAggregationTypes.DEBUG.getSqlValue());
				});
			}
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
				            s.setInt(3, 1);
				            s.setString(4, TBPrevAggregationTypes.NEW_ART.getSqlValue());
			            }),
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", s -> {
				            s.setDate(1, parseSqlDate(params.get("startDate")));
				            s.setDate(2, parseSqlDate(params.get("endDate")));
				            s.setInt(3, 1);
				            s.setString(4, TBPrevAggregationTypes.PREV_ART.getSqlValue());
			            }));
		}

		// TX_CURR DATIM: 5 fixed calls (CD4 breakdown × 3 + age/sex × 2)
		if ("sp_dim_tx_curr_datim_query".equalsIgnoreCase(name)) {
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
				    s.setInt(2, 1);
				    s.setInt(3, 0);
				    s.setInt(4, 0);
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
				    s.setInt(2, 1);
				    s.setInt(3, 1);
				    s.setInt(4, 0);
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
				    s.setInt(2, 1);
				    s.setInt(3, 2);
				    s.setInt(4, 0);
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
				    s.setInt(2, 0);
				    s.setInt(3, 3);
				    s.setInt(4, 0);
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("endDate")));
				    s.setInt(2, 0);
				    s.setInt(3, 3);
				    s.setInt(4, 1);
			    }));
		}

		throw new IllegalArgumentException("No parameter mapping defined for procedure: " + name);
	}
	
	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------
	
	private List<DataSetEvaluatorHelper.ProcedureCall> single(String call, DataSetEvaluatorHelper.ParameterSetter setter) {
		return Collections.singletonList(new DataSetEvaluatorHelper.ProcedureCall(call, setter));
	}
	
	private List<DataSetEvaluatorHelper.ProcedureCall> dateRange(String call, Map<String, String> params) {
		return single(call, s -> {
			s.setDate(1, parseSqlDate(params.get("startDate")));
			s.setDate(2, parseSqlDate(params.get("endDate")));
		});
	}
	
	private List<Map<String, Object>> mapAndPaginateData(SimpleDataSet data, int offset, int limit) {
		List<Map<String, Object>> result = new ArrayList<>();
		if (data == null || data.getRows() == null) {
			return result;
		}

		List<DataSetRow> rows = data.getRows();
		int start = Math.max(0, offset);
		int end = (limit > 0) ? Math.min(start + limit, rows.size()) : rows.size();

		if (start >= rows.size()) {
			return result;
		}

		for (int i = start; i < end; i++) {
			DataSetRow row = rows.get(i);
			Map<String, Object> map = new HashMap<>();
			for (DataSetColumn column : row.getColumnValues().keySet()) {
				map.put(column.getName(), row.getColumnValue(column));
			}
			result.add(map);
		}
		return result;
	}
	
	private java.sql.Date parseSqlDate(String dateStr) {
		if (dateStr == null || dateStr.trim().isEmpty()) {
			return null;
		}
		try {
			java.util.Date parsed = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
			return new java.sql.Date(parsed.getTime());
		}
		catch (Exception e) {
			log.warn("Could not parse date parameter: " + dateStr, e);
			return null;
		}
	}
}
