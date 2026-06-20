package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_curr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection,
			    procedureCalls)) {

				executeStatements(statementContainer, procedureCalls);

				List<ResultSet> resultSets = new ArrayList<>(Arrays.asList(statementContainer.getResultSets()));

				DataSetEvaluatorHelper.mergeResultSetsSideBySide(data, resultSets,
				    new String[] { "<3 months of ARVs (not MMD)", "3-5 months of ARVs", "6 or more months of ARVs" });

				connection.commit();
				return data;

			}
			catch (SQLException e) {
				rollbackAndThrowException(connection, ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		}
		catch (SQLException e) {
			throw new EvaluationException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		throw new EvaluationException("unreachable");
	}

	private List<ProcedureCall> createProcedureCalls(TxCurrAgeSexDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date endDate = dataSetDefinitionMamba.getEndDate() != null
		        ? new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime())
		        : null;
		TxCurrAggregationTypes aggregation = dataSetDefinitionMamba.getTxCurrAggregationType();

		if (aggregation == TxCurrAggregationTypes.CD4) {
			return Arrays.asList(
			    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, endDate);
				    statement.setInt(2, 1);
				    statement.setInt(3, 0);
				    statement.setInt(4, 0);
			    }),
			    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, endDate);
				    statement.setInt(2, 1);
				    statement.setInt(3, 1);
				    statement.setInt(4, 0);
			    }),
			    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, endDate);
				    statement.setInt(2, 1);
				    statement.setInt(3, 2);
				    statement.setInt(4, 0);
			    }));
		} else if (aggregation == TxCurrAggregationTypes.AGE_SEX) {
			return Collections.singletonList(
			    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, endDate);
				    statement.setInt(2, 0);
				    statement.setInt(3, 3);
				    statement.setInt(4, 0);
			    }));
		} else if (aggregation == TxCurrAggregationTypes.NUMERATOR) {
			return Collections.singletonList(
			    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, endDate);
				    statement.setInt(2, 0);
				    statement.setInt(3, 3);
				    statement.setInt(4, 1);
			    }));
		} else {
			throw new IllegalArgumentException("Unknown aggregation type: " + aggregation);
		}
	}
}
