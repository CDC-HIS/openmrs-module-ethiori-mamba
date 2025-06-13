package org.openmrs.module.mambaetl.helpers;

import org.apache.commons.logging.Log;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DataSetEvaluatorHelper {
	
	public static DataSource getDataSource() {
		return ConnectionPoolManager.getInstance().getDataSource();
	}
	
	public static void rollbackAndThrowException(Connection connection, String message, SQLException e, Log log)
	        throws EvaluationException {
		try {
			connection.rollback();
		}
		catch (SQLException rollbackException) {
			log.error("Error during rollback: " + rollbackException.getMessage(), rollbackException);
		}
		throw new EvaluationException(message, e);
	}
	
	public static CallableStatementContainer prepareStatements(Connection connection, List<ProcedureCall> procedureCalls)
	        throws SQLException {
		CallableStatement[] statements = new CallableStatement[procedureCalls.size()];
		for (int i = 0; i < procedureCalls.size(); i++) {
			statements[i] = connection.prepareCall(procedureCalls.get(i).getProcedureName());
		}
		return new CallableStatementContainer(statements);
	}
	
	public static void executeStatements(CallableStatementContainer statementContainer, List<ProcedureCall> procedureCalls)
	        throws SQLException {
		CallableStatement[] statements = statementContainer.getStatements();
		ResultSet[] resultSets = statementContainer.getResultSets();
		
		for (int i = 0; i < procedureCalls.size(); i++) {
			ProcedureCall call = procedureCalls.get(i);
			CallableStatement statement = statements[i];
			if (statement != null) {
				call.getParameterSetter().setParameters(statement); // Use the lambda to set parameters
				resultSets[i] = statement.executeQuery();
			}
		}
	}
	
	public static void mapResultSet(SimpleDataSet data, ResultSetMapper resultSetMapper, ResultSet[] resultSets,
	        Boolean addTotal) throws SQLException {
		
		// Adding total row at the start of dataset to be updated later
		DataSetRow totalRow = new DataSetRow();
		// Using total text and total count columns from restultset first and second indexes
		DataSetColumn totalCountColumn = new DataSetColumn();
		DataSetColumn totalCountNameColumn;
		if (addTotal && resultSets.length > 0 && data.getMetaData().getColumn("Error") == null) {
			totalCountNameColumn = new DataSetColumn(resultSets[0].getMetaData().getColumnLabel(1), resultSets[0]
			        .getMetaData().getColumnLabel(1), resultSets[0].getMetaData().getColumnTypeName(1).getClass());
			totalCountColumn = new DataSetColumn(resultSets[0].getMetaData().getColumnLabel(2), resultSets[0].getMetaData()
			        .getColumnLabel(2), resultSets[0].getMetaData().getColumnTypeName(2).getClass());
			totalRow.addColumnValue(totalCountNameColumn, "TOTAL");
			totalRow.addColumnValue(totalCountColumn, 0);
			data.addRow(totalRow);
		}
		for (ResultSet resultSet : resultSets) {
			if (resultSet != null) {
				resultSetMapper.mapResultSetToDataSet(resultSet, data);
				
			}
		}
		
		if (addTotal && resultSets.length > 0) {
			int dataRowCount = data.getRows().size() - 1;
			totalRow.addColumnValue(totalCountColumn, dataRowCount);
		}
		
	}
	
	/**
	 * Inner class to hold ProcedureCall details.
	 */
	public static class ProcedureCall {
		
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
	public interface ParameterSetter {
		
		void setParameters(CallableStatement statement) throws SQLException;
	}
	
	/**
	 * Inner class to manage CallableStatements and ResultSets within try-with-resources.
	 */
	public static class CallableStatementContainer implements AutoCloseable {
		
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
