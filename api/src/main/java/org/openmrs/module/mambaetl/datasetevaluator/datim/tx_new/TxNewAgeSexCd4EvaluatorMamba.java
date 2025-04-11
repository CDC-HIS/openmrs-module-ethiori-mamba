package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_new;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.TxNewAgeSexCd4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
import org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2.HMISDHIS2DataSetEvaluator;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
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
import java.util.*;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;
import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.rollbackAndThrowException;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;

@Handler(supports = { TxNewAgeSexCd4DataSetDefinitionMamba.class })
public class TxNewAgeSexCd4EvaluatorMamba implements DataSetEvaluator {

	private static final Log log = LogFactory.getLog(TxNewAgeSexCd4EvaluatorMamba.class);

	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";

	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";

	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		TxNewAgeSexCd4DataSetDefinitionMamba dataSetDefinitionMamba = (TxNewAgeSexCd4DataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ResultSetMapper resultSetMapper = new ResultSetMapper();

		ValidateDates(data, dataSetDefinitionMamba.getStartDate(), dataSetDefinitionMamba.getEndDate());

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

	private List<ProcedureCall> createProcedureCalls(TxNewAgeSexCd4DataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date startDate = new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime());
		java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());

		return Collections.singletonList(
                new ProcedureCall("{call sp_dim_tx_new_datim_query(?,?,?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                    statement.setInt(3, 0);
                    statement.setString(4, dataSetDefinitionMamba.getCd4Status().getSqlValue());
                })
        );
	}
}






//
//
//
//@Override
//public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
//	TxNewAgeSexCd4DataSetDefinitionMamba dataSetDefinitionMamba = (TxNewAgeSexCd4DataSetDefinitionMamba) dataSetDefinition;
//	SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
//	ResultSetMapper resultSetMapper = new ResultSetMapper();
//
//	try (Connection connection = getDataSource().getConnection();
//		 CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_query(?,?,?,?)}")) {
//
//		statement.setDate(1, new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime()));
//		statement.setDate(2, new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime()));
//		statement.setInt(3, 0);
//		statement.setString(4, dataSetDefinitionMamba.getCd4Status().getSqlValue());
//
//		try (ResultSet resultSet = statement.executeQuery()) {
//			if (resultSet != null) {
//				return resultSetMapper.mapResultSetToDataSet(resultSet, data);
//			} else {
//				throw new EvaluationException("No data returned from the query.");
//			}
//		}
//	} catch (SQLException e) {
//		throw new EvaluationException("Error processing ResultSet: " + e.getMessage(), e);
//	}
//}
//
//private DataSource getDataSource() {
//	return ConnectionPoolManager.getInstance().getDataSource();
//}
//
//