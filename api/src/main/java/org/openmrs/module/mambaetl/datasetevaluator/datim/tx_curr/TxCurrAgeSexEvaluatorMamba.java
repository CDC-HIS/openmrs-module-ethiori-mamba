package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_curr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;
import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.rollbackAndThrowException;

@Handler(supports = { TxCurrAgeSexDataSetDefinitionMamba.class })
public class TxCurrAgeSexEvaluatorMamba implements DataSetEvaluator {

	private static final Log log = LogFactory.getLog(TxCurrAgeSexEvaluatorMamba.class);

	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";

	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";

	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		TxCurrAgeSexDataSetDefinitionMamba dataSetDefinitionMamba = (TxCurrAgeSexDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ResultSetMapper resultSetMapper = new ResultSetMapper();

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) { // Use static method from helper
			connection.setAutoCommit(false); // Ensure consistency across multiple queries

			List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) { // Use static method from helper

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

	private List<ProcedureCall> createProcedureCalls(TxCurrAgeSexDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());

		return Collections.singletonList(
				new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?)}", statement -> {
					statement.setDate(2, endDate);
					statement.setInt(3, 0);
				})
		);
	}
	
}
