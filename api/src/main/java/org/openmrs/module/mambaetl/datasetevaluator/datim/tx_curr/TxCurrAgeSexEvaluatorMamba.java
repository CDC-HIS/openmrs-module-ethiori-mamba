package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_curr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;

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

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);
			List<ResultSet> resultSets = new ArrayList<>();

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

				executeStatements(statementContainer, procedureCalls);

				resultSets.addAll(Arrays.asList(statementContainer.getResultSets()));

				// Merge results side by side
				mergeResultSetsSideBySide(data, resultSets);

				connection.commit();
				return data;

			} catch (SQLException e) {
				rollbackAndThrowException(connection, ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		} catch (SQLException e) {
			throw new EvaluationException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		return null;
	}
	
	private List<ProcedureCall> createProcedureCalls(TxCurrAgeSexDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());

		return Arrays.asList(
				new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?)}", statement -> {
					statement.setDate(1, endDate);
					statement.setInt(2, 1);
					statement.setInt(3, 0);
				}),
				new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?)}", statement -> {
					statement.setDate(1, endDate);
					statement.setInt(2, 1);
					statement.setInt(3, 1);
				}),
				new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?)}", statement -> {
					statement.setDate(1, endDate);
					statement.setInt(2, 1);
					statement.setInt(3, 2);
				})
		);
	}
	
	private void mergeResultSetsSideBySide(SimpleDataSet data, List<ResultSet> resultSets) throws SQLException {
		if (resultSets == null || resultSets.isEmpty()) {
			return;
		}

		List<String> columnHeaders = new ArrayList<>();
		ResultSetMetaData metaData = resultSets.get(0).getMetaData();
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			columnHeaders.add(metaData.getColumnLabel(i));
		}
		DataSetRow row = new DataSetRow();

		for (ResultSet rs : resultSets) {
			while (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					String columnName = metaData.getColumnLabel(i);
					Object columnValue = rs.getObject(i);
					DataSetColumn column = new DataSetColumn(columnName, columnName, columnValue != null ? columnValue.getClass() : Object.class);
					row.addColumnValue(column, columnValue);
				}

			}
		}
		data.addRow(row);
	}
}
