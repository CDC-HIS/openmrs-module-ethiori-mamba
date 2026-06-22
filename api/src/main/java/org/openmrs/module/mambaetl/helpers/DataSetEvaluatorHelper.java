package org.openmrs.module.mambaetl.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DataSetEvaluatorHelper {
	
	private static final Log log = LogFactory.getLog(DataSetEvaluatorHelper.class);
	
	public static DataSource getDataSource() {
		return CustomConnectionPoolManager.getInstance().getDataSource();
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
		executeStatements(statementContainer, procedureCalls, 0, null, 0, null);
	}
	
	public static void executeStatements(CallableStatementContainer statementContainer, List<ProcedureCall> procedureCalls,
	        int queryTimeoutSeconds, ProgressReporter progressReporter, int maxRows) throws SQLException {
		executeStatements(statementContainer, procedureCalls, queryTimeoutSeconds, progressReporter, maxRows, null);
	}
	
	public static void executeStatements(CallableStatementContainer statementContainer, List<ProcedureCall> procedureCalls,
	        int queryTimeoutSeconds, ProgressReporter progressReporter, int maxRows, StatementRegistrar statementRegistrar)
	        throws SQLException {
		CallableStatement[] statements = statementContainer.getStatements();
		ResultSet[] resultSets = statementContainer.getResultSets();
		int total = procedureCalls.size();
		
		for (int i = 0; i < total; i++) {
			ProcedureCall call = procedureCalls.get(i);
			CallableStatement statement = statements[i];
			if (statement != null) {
				if (queryTimeoutSeconds > 0) {
					statement.setQueryTimeout(queryTimeoutSeconds);
				}
				if (maxRows > 0) {
					statement.setMaxRows(maxRows);
				}
				call.getParameterSetter().setParameters(statement);
				log.debug("Executing procedure [" + (i + 1) + "/" + total + "]: " + call.getProcedureName());
				if (statementRegistrar != null) {
					statementRegistrar.register(statement);
				}
				try {
					resultSets[i] = statement.executeQuery();
				}
				catch (SQLException e) {
					log.error("SQL error executing procedure [" + (i + 1) + "/" + total + "]: " + call.getProcedureName()
					        + " — " + e.getMessage());
					throw e;
				}
				finally {
					if (statementRegistrar != null) {
						statementRegistrar.register(null);
					}
				}
				if (progressReporter != null) {
					progressReporter.report(i + 1, total);
				}
			}
		}
	}
	
	@FunctionalInterface
	public interface ProgressReporter {
		
		void report(int completedSteps, int totalSteps);
	}
	
	@FunctionalInterface
	public interface StatementRegistrar {
		
		/**
		 * Called with the active statement before executeQuery(); called with null once it
		 * completes.
		 */
		void register(CallableStatement statement);
	}
	
	public static void mapResultSet(SimpleDataSet data, ResultSetMapper resultSetMapper, ResultSet[] resultSets,
	        Boolean addTotal) throws SQLException {
		
		DataSetRow totalRow = new DataSetRow();
		DataSetColumn totalCountColumn = new DataSetColumn();
		boolean totalRowAdded = false;
		
		if (addTotal && resultSets.length > 0) {
			DataSetColumn totalCountNameColumn = new DataSetColumn(resultSets[0].getMetaData().getColumnLabel(1),
			        resultSets[0].getMetaData().getColumnLabel(1), String.class);
			totalCountColumn = new DataSetColumn(resultSets[0].getMetaData().getColumnLabel(2), resultSets[0].getMetaData()
			        .getColumnLabel(2), String.class);
			totalRow.addColumnValue(totalCountNameColumn, "TOTAL");
			totalRow.addColumnValue(totalCountColumn, 0);
			data.addRow(totalRow);
			totalRowAdded = true;
		}
		for (ResultSet resultSet : resultSets) {
			if (resultSet != null) {
				resultSetMapper.mapResultSetToDataSet(resultSet, data);
			}
		}
		
		if (totalRowAdded) {
			int dataRowCount = data.getRows().size() - 1;
			totalRow.addColumnValue(totalCountColumn, dataRowCount);
		}
		
	}
	
	public static void mergeResultSetsSideBySide(SimpleDataSet data, List<ResultSet> resultSets, String[] columnPrefixes)
	        throws SQLException {
		if (resultSets == null || resultSets.isEmpty()) {
			return;
		}
		if (resultSets.size() == 1) {
			mapResultSet(data, new ResultSetMapper(), resultSets.toArray(new ResultSet[0]), Boolean.FALSE);
			return;
		}

		int count = 0;
		Map<String, DataSetRow> rowsMap = new LinkedHashMap<>();

		for (ResultSet resultSet : resultSets) {
			if (resultSet == null) {
				log.warn("Encountered a null ResultSet in mergeResultSetsSideBySide. Skipping.");
				count++;
				continue;
			}

			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();

			while (resultSet.next()) {
				String rowIndexKey = resultSet.getObject(1) != null ? resultSet.getObject(1).toString()
				        : "null_key_" + UUID.randomUUID();
				DataSetRow row = rowsMap.computeIfAbsent(rowIndexKey, k -> new DataSetRow());

				for (int i = 1; i <= columnCount; i++) {
					String originalColumnName = metaData.getColumnLabel(i);
					Object columnValue = resultSet.getObject(i);
					String columnName;

					if (originalColumnName.equalsIgnoreCase("sex")) {
						columnName = originalColumnName;
						DataSetColumn col = new DataSetColumn(columnName, columnName,
						    columnValue != null ? columnValue.getClass() : Object.class);
						if (!row.getColumnValues().containsKey(col)) {
							row.addColumnValue(col, columnValue);
						}
					} else {
						String prefix = (columnPrefixes != null && count < columnPrefixes.length)
						        ? columnPrefixes[count] + " "
						        : "Group" + count + " ";
						columnName = prefix + originalColumnName;
						row.addColumnValue(
						    new DataSetColumn(columnName, columnName,
						        columnValue != null ? columnValue.getClass() : Object.class),
						    columnValue);
					}
				}
			}
			count++;
		}

		for (DataSetRow row : rowsMap.values()) {
			data.addRow(row);
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
