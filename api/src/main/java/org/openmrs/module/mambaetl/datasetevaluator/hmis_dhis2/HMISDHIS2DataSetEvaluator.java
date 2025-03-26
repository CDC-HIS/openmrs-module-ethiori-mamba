package org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2;

import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*; //Static import DataSetEvaluatorHelper methods and inner classes

@Handler(supports = { HMISDHIS2DatasetDefinition.class })
public class HMISDHIS2DataSetEvaluator implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(HMISDHIS2DataSetEvaluator.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition = (HMISDHIS2DatasetDefinition) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ResultSetMapper resultSetMapper = new ResultSetMapper();

		ValidateDates(data, hmisdhis2DatasetDefinition.getStartDate(), hmisdhis2DatasetDefinition.getEndDate());

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) { // Use static method from helper
			connection.setAutoCommit(false); // Ensure consistency across multiple queries

			List<ProcedureCall> procedureCalls = createProcedureCalls(hmisdhis2DatasetDefinition);

			try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) { // Use static method from helper

				executeStatements(statementContainer, procedureCalls); // Use static method from helper

				ResultSet[] allResultSets = statementContainer.getResultSets();

				// Merge results
				mapResultSet(data, resultSetMapper, allResultSets); // Use static method from helper
				connection.commit();
				return data;

			} catch (SQLException e) {
				rollbackAndThrowException(connection, ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log); // Use static method from helper and pass logger
			}
		} catch (SQLException e) {
			throw new EvaluationException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		return null;
	}
	
	private List<ProcedureCall> createProcedureCalls(HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition) {
		java.sql.Date startDate = new java.sql.Date(hmisdhis2DatasetDefinition.getStartDate().getTime());
		java.sql.Date endDate = new java.sql.Date(hmisdhis2DatasetDefinition.getEndDate().getTime());

		java.sql.Date startDateVL12Month;

		String viralLoadType = Context.getService(AdministrationService.class).getGlobalProperty("_viralLoad12MSetting");
		if( Objects.nonNull(viralLoadType) && viralLoadType.equalsIgnoreCase("YES")){
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(endDate);
			calendar.add(Calendar.MONTH,-12);
			startDateVL12Month = new java.sql.Date(calendar.getTime().getTime());
		} else {
            startDateVL12Month = startDate;
        }

        return Arrays.asList(
				new ProcedureCall("{call sp_fact_hmis_hiv_hts_tst_index_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_tx_curr_query(?)}", statement -> {
					statement.setDate(1, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_tx_new_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_art_ret_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_art_ret_net_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_linkage_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_tx_pvls_query(?,?)}", statement -> {
					statement.setDate(1, startDateVL12Month);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_dsd_query(?)}", statement -> {
					statement.setDate(1, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_art_intr_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_prep_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_pep_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_phliv_tsp_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_fp_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_tb_scrn_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_hiv_tpt_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_cxca_scrn_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_cxca_rx_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_tb_lb_lf_lam_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				}),
				new ProcedureCall("{call sp_fact_hmis_mtct_query(?,?)}", statement -> {
					statement.setDate(1, startDate);
					statement.setDate(2, endDate);
				})
		);
	}
}
