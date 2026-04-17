package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DynamicReportExecutorService {

	private static final Log log = LogFactory.getLog(DynamicReportExecutorService.class);

	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";

	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";

	public List<Map<String, Object>> executeReport(String procedureName, Map<String, String> params, int offset,
	        int limit) throws SQLException {

		validateProcedureName(procedureName);

		SimpleDataSet data = new SimpleDataSet(null, null);
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = getProcedureCalls(procedureName, params);

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = DataSetEvaluatorHelper
			        .prepareStatements(connection, procedureCalls)) {

				DataSetEvaluatorHelper.executeStatements(statementContainer, procedureCalls);

				ResultSet[] allResultSets = statementContainer.getResultSets();
				DataSetEvaluatorHelper.mapResultSet(data, resultSetMapper, allResultSets, false);
				connection.commit();

				return mapAndPaginateData(data, offset, limit);

			}
			catch (SQLException e) {
				DataSetEvaluatorHelper.rollbackAndThrowException(connection,
				    ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		}
		catch (SQLException | EvaluationException e) {
			throw new SQLException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		return new ArrayList<>();
	}

	private void validateProcedureName(String procedureName) {
		if (!procedureName.matches("^[a-zA-Z0-9_]+$")) {
			throw new IllegalArgumentException(
			        "Invalid procedure name formatting. Only alphanumeric characters and underscores are allowed.");
		}
	}

	private List<DataSetEvaluatorHelper.ProcedureCall> getProcedureCalls(String procedureName,
	        Map<String, String> params) {

		if ("sp_fact_line_list_tx_curr_analysis_query".equalsIgnoreCase(procedureName)) {
			return Collections.singletonList(
			    new DataSetEvaluatorHelper.ProcedureCall(
			            "{call sp_fact_line_list_tx_curr_analysis_query(?, ?, ?)}", statement -> {
				            statement.setDate(1, parseSqlDate(params.get("startDate")));
				            statement.setDate(2, parseSqlDate(params.get("endDate")));
				            statement.setString(3, params.get("txCurrAnalysisCategories"));
			            }));
		}

		throw new IllegalArgumentException("No parameter mapping defined for procedure: " + procedureName);
	}

	private List<Map<String, Object>> mapAndPaginateData(SimpleDataSet data, int offset, int limit) {
		List<Map<String, Object>> result = new ArrayList<>();
		if (data == null || data.getRows() == null) {
			return result;
		}

		List<DataSetRow> rows = data.getRows();
		int start = Math.max(0, offset);
		int end = (limit > 0) ? Math.min(start + limit, rows.size()) : rows.size();

		if (start >= rows.size()) {
			return result;
		}

		for (int i = start; i < end; i++) {
			DataSetRow row = rows.get(i);
			Map<String, Object> map = new HashMap<>();
			for (DataSetColumn column : row.getColumnValues().keySet()) {
				map.put(column.getName(), row.getColumnValue(column));
			}
			result.add(map);
		}
		return result;
	}

	private java.sql.Date parseSqlDate(String dateStr) {
		if (dateStr == null || dateStr.trim().isEmpty()) {
			return null;
		}
		try {
			java.util.Date parsed = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
			return new java.sql.Date(parsed.getTime());
		}
		catch (Exception e) {
			log.warn("Could not parse date parameter: " + dateStr, e);
			return null;
		}
	}
}
