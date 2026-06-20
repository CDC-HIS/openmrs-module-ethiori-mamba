package org.openmrs.module.mambaetl.datasetevaluator.datim.tb_prev;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tb_prev.TBPrevDenominatorDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBPrevAggregationTypes;
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
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;

@Handler(supports = { TBPrevDenominatorDataSetDefinitionMamba.class })
public class TBPrevDenominatorEvaluatorMamba implements DataSetEvaluator {

	private static final Log log = LogFactory.getLog(TBPrevDenominatorEvaluatorMamba.class);

	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";

	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";

	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
	        throws EvaluationException {

		TBPrevDenominatorDataSetDefinitionMamba dataSetDefinitionMamba = (TBPrevDenominatorDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ValidateDates(data, dataSetDefinitionMamba.getStartDate(), dataSetDefinitionMamba.getEndDate());

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);

			try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

				executeStatements(statementContainer, procedureCalls);

				List<ResultSet> resultSets = new ArrayList<>(Arrays.asList(statementContainer.getResultSets()));

				DataSetEvaluatorHelper.mergeResultSetsSideBySide(data, resultSets,
				    new String[] { "Newly enrolled on ART", "Previously Enrolled on ART" });

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

	private List<ProcedureCall> createProcedureCalls(TBPrevDenominatorDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date startDate = dataSetDefinitionMamba.getStartDate() != null
		        ? new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime())
		        : null;
		java.sql.Date endDate = dataSetDefinitionMamba.getEndDate() != null
		        ? new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime())
		        : null;

		if (!dataSetDefinitionMamba.getTbPrevAggregationTypes().getSqlValue().equalsIgnoreCase("TOTAL")) {
			return Arrays.asList(
			    new ProcedureCall("{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, startDate);
				    statement.setDate(2, endDate);
				    statement.setInt(3, 1);
				    statement.setString(4, TBPrevAggregationTypes.NEW_ART.getSqlValue());
			    }),
			    new ProcedureCall("{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, startDate);
				    statement.setDate(2, endDate);
				    statement.setInt(3, 1);
				    statement.setString(4, TBPrevAggregationTypes.PREV_ART.getSqlValue());
			    }));
		} else {
			return Collections.singletonList(
			    new ProcedureCall("{call sp_dim_tb_prev_datim_denominator_query(?,?,?,?)}", statement -> {
				    statement.setDate(1, startDate);
				    statement.setDate(2, endDate);
				    statement.setInt(3, 1);
				    statement.setString(4, TBPrevAggregationTypes.TOTAL.getSqlValue());
			    }));
		}
	}
}
