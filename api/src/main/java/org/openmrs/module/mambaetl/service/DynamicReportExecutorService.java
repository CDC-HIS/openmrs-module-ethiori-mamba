package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.FollowUpConstant;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class DynamicReportExecutorService {
	
	private static final Log log = LogFactory.getLog(DynamicReportExecutorService.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	public ReportExecutionResult executeReport(String procedureName, Map<String, String> params, int offset, int limit)
	        throws SQLException {

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

				String[] sideBySideLabels = resolveSideBySideLabels(procedureName, params);

				List<String> columns;
				if (sideBySideLabels != null) {
					mergeResultSetsSideBySide(data, allResultSets, sideBySideLabels);
					columns = data.getRows().isEmpty() ? Collections.emptyList()
					        : data.getRows().get(0).getColumnValues().keySet().stream()
					                .map(DataSetColumn::getName).collect(Collectors.toList());
				} else {
					columns = extractColumnsInOrder(allResultSets, resultSetMapper);
					DataSetEvaluatorHelper.mapResultSet(data, resultSetMapper, allResultSets, false);
				}
				connection.commit();

				return new ReportExecutionResult(columns, mapAndPaginateData(data, columns, offset, limit));

			}
			catch (SQLException e) {
				DataSetEvaluatorHelper.rollbackAndThrowException(connection,
				    ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		}
		catch (SQLException | EvaluationException e) {
			throw new SQLException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		return new ReportExecutionResult(Collections.emptyList(), new ArrayList<>());
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
				// Old UI used "followupStatus" and mapped it to DB representation.
				// Keep backward compatibility: accept either followupStatus or targetGroup.
				String followupStatus = params.get("followupStatus");
				if (followupStatus == null || followupStatus.trim().isEmpty()) {
					followupStatus = params.get("targetGroup");
				}
				s.setString(3, FollowUpConstant.getDbRepresentation(followupStatus));
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

		// Old UI TXTB report selected which SP to run using "type".
		// This alias replicates that behavior for the API executor.
		if ("sp_fact_line_list_tx_tb".equalsIgnoreCase(name)) {
			String type = params.getOrDefault("type", "");
			if ("TB-ART".equalsIgnoreCase(type) || "tb_art".equalsIgnoreCase(type)) {
				return dateRange("{call sp_fact_line_list_tx_tb_art_query(?,?)}", params);
			}
			if ("TB Screening".equalsIgnoreCase(type) || "denominator".equalsIgnoreCase(type)) {
				return dateRange("{call sp_fact_line_list_tx_tb_denominator_query(?,?)}", params);
			}
			if ("TB Treatment".equalsIgnoreCase(type) || "numerator".equalsIgnoreCase(type)) {
				return dateRange("{call sp_fact_line_list_tx_tb_numerator_query(?,?)}", params);
			}
			throw new IllegalArgumentException(
			        "Invalid type for sp_fact_line_list_tx_tb. Expected: TB-ART/tb_art, TB Screening/denominator, TB Treatment/numerator");
		}

		// Old UI TI/TO report selected which SP to run using "status".
		// This alias replicates that behavior for the API executor.
		if ("sp_fact_line_list_ti_to".equalsIgnoreCase(name)) {
			String status = params.getOrDefault("status", "TO");
			if ("TI".equalsIgnoreCase(status)) {
				return dateRange("{call sp_fact_line_list_ti_query(?,?)}", params);
			}
			if ("TO".equalsIgnoreCase(status)) {
				return dateRange("{call sp_fact_line_list_to_query(?,?)}", params);
			}
			throw new IllegalArgumentException("Invalid status for sp_fact_line_list_ti_to. Expected TI or TO");
		}

		if ("sp_fact_line_list_art_cohort_analysis".equalsIgnoreCase(name)) {
			String type = params.getOrDefault("type", "LineList");
			if ("SUMMARY".equalsIgnoreCase(type)) {
				return single("{call sp_fact_line_list_art_cohort_analysis_summary_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			return single("{call sp_fact_line_list_art_cohort_analysis_query(?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_child_cohort_analysis".equalsIgnoreCase(name)) {
			String type = params.getOrDefault("type", "LineList");
			if ("SUMMARY".equalsIgnoreCase(type)) {
				return single("{call sp_fact_line_list_child_cohort_analysis_summary_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			return single("{call sp_fact_line_list_child_cohort_analysis_query(?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_mother_cohort_analysis".equalsIgnoreCase(name)) {
			String type = params.getOrDefault("type", "LineList");
			if ("SUMMARY".equalsIgnoreCase(type)) {
				return single("{call sp_fact_line_list_mother_cohort_analysis_summary_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			return single("{call sp_fact_line_list_mother_cohort_analysis_query(?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
			});
		}
		if ("sp_fact_line_list_tb_prev_linelist".equalsIgnoreCase(name)) {
			String tptStatus = params.getOrDefault("tptStatus", "Numerator");
			if ("DENOMINATOR".equalsIgnoreCase(tptStatus)) {
				return single("{call sp_fact_line_list_tx_tbt_denominator_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			if ("NUMERATOR".equalsIgnoreCase(tptStatus)) {
				return single("{call sp_fact_line_list_tx_tbt_numerator_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			// Fallback: run both if unknown/ALL
			return Arrays.asList(
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_tx_tbt_numerator_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }),
			    new DataSetEvaluatorHelper.ProcedureCall("{call sp_fact_line_list_tx_tbt_denominator_query(?,?)}", s -> {
				    s.setDate(1, parseSqlDate(params.get("startDate")));
				    s.setDate(2, parseSqlDate(params.get("endDate")));
			    }));
		}
		if ("sp_fact_line_list_vl_sent_received".equalsIgnoreCase(name)) {
			String type = params.getOrDefault("type", "all");
			if ("SENT".equalsIgnoreCase(type)) {
				return single("{call sp_fact_line_list_vl_sent_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			if ("RECEIVED".equalsIgnoreCase(type)) {
				return single("{call sp_fact_line_list_vl_received_query(?,?)}", s -> {
					s.setDate(1, parseSqlDate(params.get("startDate")));
					s.setDate(2, parseSqlDate(params.get("endDate")));
				});
			}
			// Fallback: run both for "all"/unknown
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
			java.sql.Date vlStartDate = resolveVlStartDate(params);
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
				    s.setDate(1, vlStartDate);
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

		if ("sp_dim_ict_datim_query".equalsIgnoreCase(name)) {
			String aggType = params.get("aggregationType");
			int elicitedFlag = "ELICITED".equalsIgnoreCase(aggType) ? 1 : 0;
			return single("{call sp_dim_ict_datim_query(?,?,?,?)}", s -> {
				s.setDate(1, parseSqlDate(params.get("startDate")));
				s.setDate(2, parseSqlDate(params.get("endDate")));
				s.setInt(3, elicitedFlag);
				s.setString(4, aggType);
			});
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

		// TX_CURR DATIM: dispatch by aggregationType
		if ("sp_dim_tx_curr_datim_query".equalsIgnoreCase(name)) {
			String aggType = params.getOrDefault("aggregationType", "").toUpperCase();
			switch (aggType) {
				case "CD4":
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
					    }));
				case "AGE_SEX":
					return single("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
						s.setDate(1, parseSqlDate(params.get("endDate")));
						s.setInt(2, 0);
						s.setInt(3, 3);
						s.setInt(4, 0);
					});
				case "NUMERATOR":
					return single("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", s -> {
						s.setDate(1, parseSqlDate(params.get("endDate")));
						s.setInt(2, 0);
						s.setInt(3, 3);
						s.setInt(4, 1);
					});
				default:
					// All 5 calls combined (no aggregationType specified)
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
	
	private List<Map<String, Object>> mapAndPaginateData(SimpleDataSet data, List<String> columnsInOrder, int offset,
	        int limit) {
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
			Map<String, Object> nameToValue = new HashMap<>();
			for (Map.Entry<DataSetColumn, Object> entry : row.getColumnValues().entrySet()) {
				nameToValue.put(entry.getKey().getName(), entry.getValue());
			}

			Map<String, Object> map = new LinkedHashMap<>();
			if (columnsInOrder != null && !columnsInOrder.isEmpty()) {
				for (String columnName : columnsInOrder) {
					map.put(columnName, nameToValue.get(columnName));
				}
				// Defensive: include any extra columns at end (stable-ish).
				for (Map.Entry<String, Object> entry : nameToValue.entrySet()) {
					if (!map.containsKey(entry.getKey())) {
						map.put(entry.getKey(), entry.getValue());
					}
				}
			} else {
				// Fallback to whatever order DataSetRow provides
				for (Map.Entry<DataSetColumn, Object> entry : row.getColumnValues().entrySet()) {
					map.put(entry.getKey().getName(), entry.getValue());
				}
			}
			result.add(map);
		}
		return result;
	}
	
	private List<String> extractColumnsInOrder(ResultSet[] resultSets, ResultSetMapper mapper) throws SQLException {
		if (resultSets == null) {
			return Collections.emptyList();
		}
		// Composite procedures may return multiple result sets with different schemas.
		// Build a stable union of columns while preserving the DB order per result set.
		// (First time a column name is seen defines its position.)
		Map<String, Boolean> seen = new LinkedHashMap<>();
		for (ResultSet rs : resultSets) {
			if (rs == null) {
				continue;
			}
			ResultSetMetaData metaData = rs.getMetaData();
			if (metaData == null) {
				continue;
			}
			int columnCount = metaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				String label = metaData.getColumnLabel(i);
				String mapped = mapper.mapColumnName(label);
				if (!seen.containsKey(mapped)) {
					seen.put(mapped, Boolean.TRUE);
				}
			}
		}
		if (seen.isEmpty()) {
			return Collections.emptyList();
		}
		return new ArrayList<>(seen.keySet());
	}
	
	private java.sql.Date resolveVlStartDate(Map<String, String> params) {
		try {
			String setting = Context.getAdministrationService().getGlobalProperty("_viralLoad12MSetting");
			if ("YES".equalsIgnoreCase(setting)) {
				java.sql.Date endDate = parseSqlDate(params.get("endDate"));
				if (endDate != null) {
					Calendar cal = Calendar.getInstance();
					cal.setTime(endDate);
					cal.add(Calendar.MONTH, -12);
					return new java.sql.Date(cal.getTimeInMillis());
				}
			}
		}
		catch (Exception e) {
			log.warn("Could not read _viralLoad12MSetting global property, using startDate", e);
		}
		return parseSqlDate(params.get("startDate"));
	}
	
	private String[] resolveSideBySideLabels(String procedureName, Map<String, String> params) {
		if ("sp_dim_tx_curr_datim_query".equalsIgnoreCase(procedureName)
		        && "CD4".equalsIgnoreCase(params.get("aggregationType"))) {
			return new String[] { "<3 months of ARVs (not MMD)", "3-5 months of ARVs", "6 or more months of ARVs" };
		}
		if (("sp_dim_tb_prev_datim_numerator_query".equalsIgnoreCase(procedureName) || "sp_dim_tb_prev_datim_denominator_query"
		        .equalsIgnoreCase(procedureName))
		        && !"TOTAL".equalsIgnoreCase(params.get("aggregationType"))
		        && !"DEBUG".equalsIgnoreCase(params.get("aggregationType"))) {
			return new String[] { "Newly enrolled on ART", "Previously Enrolled on ART" };
		}
		return null;
	}
	
	private void mergeResultSetsSideBySide(SimpleDataSet data, ResultSet[] resultSets, String[] labels)
	        throws SQLException {
		if (resultSets == null || resultSets.length == 0) {
			return;
		}
		Map<String, DataSetRow> rowsMap = new LinkedHashMap<>();
		int count = 0;
		for (ResultSet rs : resultSets) {
			if (rs == null) {
				log.warn("Encountered a null ResultSet in mergeResultSetsSideBySide. Skipping.");
				count++;
				continue;
			}
			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();
			while (rs.next()) {
				String key = rs.getObject(1) != null ? rs.getObject(1).toString() : "null_key_" + UUID.randomUUID();
				DataSetRow row = rowsMap.computeIfAbsent(key, k -> new DataSetRow());
				for (int i = 1; i <= cols; i++) {
					String orig = meta.getColumnLabel(i);
					Object val = rs.getObject(i);
					String colName;
					if (orig.equalsIgnoreCase("sex")) {
						colName = orig;
						if (!row.getColumnValues().containsKey(
						    new DataSetColumn(colName, colName, val != null ? val.getClass() : Object.class))) {
							row.addColumnValue(
							    new DataSetColumn(colName, colName, val != null ? val.getClass() : Object.class), val);
						}
					} else {
						String prefix = (labels != null && count < labels.length) ? labels[count] : "Group" + count;
						colName = prefix + " " + orig;
						row.addColumnValue(
						    new DataSetColumn(colName, colName, val != null ? val.getClass() : Object.class), val);
					}
				}
			}
			count++;
		}
		rowsMap.values().forEach(data::addRow);
	}
	
	public static class ReportExecutionResult {
		
		private final List<String> columns;
		
		private final List<Map<String, Object>> data;
		
		public ReportExecutionResult(List<String> columns, List<Map<String, Object>> data) {
			this.columns = columns;
			this.data = data;
		}
		
		public List<String> getColumns() {
			return columns;
		}
		
		public List<Map<String, Object>> getData() {
			return data;
		}
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
