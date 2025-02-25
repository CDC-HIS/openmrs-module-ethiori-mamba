package org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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

        ValidationHelper validationHelper = new ValidationHelper();
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        // Validate start and end dates
        validationHelper.validateDates(hmisdhis2DatasetDefinition, data);

        try (Connection connection = getDataSource().getConnection()) {
            connection.setAutoCommit(false); // Ensure consistency across multiple queries

            List<ProcedureCall> procedureCalls = createProcedureCalls(hmisdhis2DatasetDefinition);

            try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

                executeStatements(statementContainer, hmisdhis2DatasetDefinition);

                ResultSet[] allResultSets = statementContainer.getResultSets();

                // Merge results
                mapResultSet(data, resultSetMapper, allResultSets);
                connection.commit();
                return data;

            } catch (SQLException e) {
                rollbackAndThrowException(connection, ERROR_PROCESSING_RESULT_SET + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new EvaluationException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
        }
        return null;
    }
	
	private List<ProcedureCall> createProcedureCalls(HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition) {
        java.sql.Date startDate = new java.sql.Date(hmisdhis2DatasetDefinition.getStartDate().getTime());
        java.sql.Date endDate = new java.sql.Date(hmisdhis2DatasetDefinition.getEndDate().getTime());

        return Arrays.asList(
                new ProcedureCall("{call sp_fact_dhis_tx_curr_query(?)}", statement -> {
                    statement.setDate(1, endDate);
                }),
                new ProcedureCall("{call sp_fact_dhis_tx_new_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                }),
                new ProcedureCall("{call sp_fact_hmis_hiv_art_ret_net_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                }),
                new ProcedureCall("{call sp_fact_hmis_hiv_art_ret_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                }),
                new ProcedureCall("{call sp_fact_hmis_hiv_dsd_query(?)}", statement -> {
                    statement.setDate(1, endDate); //Correct Parameter Index
                }),
                new ProcedureCall("{call sp_fact_hmis_hiv_pep_query(?)}", statement -> {
                    statement.setDate(1, endDate); //Incorrect Parameter Index, Procedure expects only one
                }),
                new ProcedureCall("{call sp_fact_hmis_hiv_prep_query(?,?)}", statement -> {
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
                new ProcedureCall("{call sp_fact_hmis_hiv_tx_pvls_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                })
        );
    }
	
	private CallableStatementContainer prepareStatements(Connection connection, List<ProcedureCall> procedureCalls)
	        throws SQLException {
		CallableStatement[] statements = new CallableStatement[procedureCalls.size()];
		for (int i = 0; i < procedureCalls.size(); i++) {
			statements[i] = connection.prepareCall(procedureCalls.get(i).getProcedureName());
		}
		return new CallableStatementContainer(statements);
	}
	
	private void executeStatements(CallableStatementContainer statementContainer,
	        HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition) throws SQLException {
		CallableStatement[] statements = statementContainer.getStatements();
		ResultSet[] resultSets = statementContainer.getResultSets();
		
		List<ProcedureCall> procedureCalls = createProcedureCalls(hmisdhis2DatasetDefinition); //Recreate to get the date setting Lambda
		
		for (int i = 0; i < procedureCalls.size(); i++) {
			ProcedureCall call = procedureCalls.get(i);
			CallableStatement statement = statements[i];
			if (statement != null) {
				call.getParameterSetter().setParameters(statement); // Use the lambda to set parameters
				resultSets[i] = statement.executeQuery();
			}
		}
	}
	
	private void mapResultSet(SimpleDataSet data, ResultSetMapper resultSetMapper, ResultSet[] resultSets)
	        throws SQLException {
		for (ResultSet resultSet : resultSets) {
			if (resultSet != null) {
				resultSetMapper.mapResultSetToDataSet(resultSet, data);
			}
		}
	}
	
	private DataSource getDataSource() {
		return ConnectionPoolManager.getInstance().getDataSource();
	}
	
	private void rollbackAndThrowException(Connection connection, String message, SQLException e) throws EvaluationException {
		try {
			connection.rollback();
		}
		catch (SQLException rollbackException) {
			log.error("Error during rollback: " + rollbackException.getMessage(), rollbackException);
		}
		throw new EvaluationException(message, e);
	}
	
	/**
	 * Inner class to hold ProcedureCall details.
	 */
	private static class ProcedureCall {
		
		private final String procedureName;
		
		private final ParameterSetter parameterSetter;
		
		public ProcedureCall(String procedureName, ParameterSetter parameterSetter) {
			this.procedureName = procedureName;
			this.parameterSetter = parameterSetter;
		}
		
		public String getProcedureName() {
			return procedureName;
		}
		
		public ParameterSetter getParameterSetter() {
			return parameterSetter;
		}
	}
	
	/**
	 * Functional interface for setting parameters on a CallableStatement.
	 */
	private interface ParameterSetter {
		
		void setParameters(CallableStatement statement) throws SQLException;
	}
	
	/**
	 * Inner class to manage CallableStatements and ResultSets within try-with-resources.
	 */
	private static class CallableStatementContainer implements AutoCloseable {
		
		private final CallableStatement[] statements;
		
		private final ResultSet[] resultSets; // To store ResultSets
		
		public CallableStatementContainer(CallableStatement[] statements) {
			this.statements = statements;
			this.resultSets = new ResultSet[statements.length]; // Initialize ResultSet array
		}
		
		public CallableStatement[] getStatements() {
			return statements;
		}
		
		public ResultSet[] getResultSets() {
			return resultSets;
		}
		
		@Override
		public void close() throws SQLException {
			SQLException suppressedException = null;
			for (CallableStatement statement : statements) {
				if (statement != null) {
					try {
						statement.close();
					}
					catch (SQLException e) {
						if (suppressedException == null) {
							suppressedException = e;
						} else {
							suppressedException.addSuppressed(e);
						}
					}
				}
			}
			//Close ResultSets
			for (ResultSet resultSet : resultSets) {
				if (resultSet != null) {
					try {
						resultSet.close();
					}
					catch (SQLException e) {
						if (suppressedException == null) {
							suppressedException = e;
						} else {
							suppressedException.addSuppressed(e);
						}
					}
				}
			}
			
			if (suppressedException != null) {
				throw suppressedException;
			}
		}
	}
}
