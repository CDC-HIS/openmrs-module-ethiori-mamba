package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_tb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_tb.TxTBDenominatorDataSetDefinitionMamba;
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
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;

@Handler(supports = { TxTBDenominatorDataSetDefinitionMamba.class })
public class TxTBDenominatorEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(TxTBDenominatorEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		TxTBDenominatorDataSetDefinitionMamba dataSetDefinitionMamba = (TxTBDenominatorDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ResultSetMapper resultSetMapper = new ResultSetMapper();

		ValidateDates(data, dataSetDefinitionMamba.getStartDate(), dataSetDefinitionMamba.getEndDate());

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) { // Use static method from helper
			connection.setAutoCommit(false); // Ensure consistency across multiple queries

			List<ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);

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
	
	private List<ProcedureCall> createProcedureCalls(TxTBDenominatorDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date startDate = new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime());
		java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());

		return Collections.singletonList(
                new ProcedureCall("{call sp_dim_tx_tb_datim_query(?,?,?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                    statement.setInt(3, 0);
	                    statement.setString(4, dataSetDefinitionMamba.getTxTBAggregationTypes().getSqlValue());
                })
        );
	}
}
