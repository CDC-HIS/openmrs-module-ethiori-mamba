package org.openmrs.module.mambaetl.datasetevaluator.datim.tb_prev;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tb_prev.TBPrevNumeratorDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBPrevAggregationTypes;
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

@Handler(supports = { TBPrevNumeratorDataSetDefinitionMamba.class })
public class TBPrevNumeratorEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(TBPrevNumeratorEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {

        TBPrevNumeratorDataSetDefinitionMamba dataSetDefinitionMamba = (TBPrevNumeratorDataSetDefinitionMamba) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

        try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            List<ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);

            try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

                executeStatements(statementContainer, procedureCalls);

                List<ResultSet> resultSets = new ArrayList<>(Arrays.asList(statementContainer.getResultSets()));

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
	
	private List<ProcedureCall> createProcedureCalls(TBPrevNumeratorDataSetDefinitionMamba dataSetDefinitionMamba) {
        java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());
        java.sql.Date startDate = new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime());

        if (!dataSetDefinitionMamba.getTbPrevAggregationTypes().getSqlValue().equalsIgnoreCase("TOTAL"))
        {
            return Arrays.asList(
                    new ProcedureCall("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", statement -> {
                        statement.setDate(1, startDate);
                        statement.setDate(2, endDate);
                        statement.setInt(3, 1);
                        statement.setString(4, TBPrevAggregationTypes.NEW_ART.getSqlValue());
                    }),
                    new ProcedureCall("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", statement -> {
                        statement.setDate(1, startDate);
                        statement.setDate(2, endDate);
                        statement.setInt(3, 1);
                        statement.setString(4, TBPrevAggregationTypes.PREV_ART.getSqlValue());
                    })

            );
        }
        else {
            return Collections.singletonList(new ProcedureCall("{call sp_dim_tb_prev_datim_numerator_query(?,?,?,?)}", statement -> {
                statement.setDate(1, startDate);
                statement.setDate(2, endDate);
                statement.setInt(3, 1);
                statement.setString(4, TBPrevAggregationTypes.TOTAL.getSqlValue());
            }));
        }


    }
	
	private void mergeResultSetsSideBySide(SimpleDataSet data, List<ResultSet> resultSets) throws SQLException {
        if (resultSets == null || resultSets.isEmpty() ) {
            // No result sets to process, the calling method will handle adding "No results" if data is empty
            return;
        }
        if(resultSets.size() == 1){
            ResultSet[] resultSets1 = resultSets.toArray(new ResultSet[0]);
            ResultSetMapper resultSetMapper = new ResultSetMapper();
            mapResultSet(data, resultSetMapper, resultSets1,Boolean.FALSE);
            // mapResultSet adds rows directly to 'data', so no need for rowsMap in this case
            return;
        }

        int count = 0;
        String[] duration = {"Newly enrolled on ART", "Previously Enrolled on ART"};

        Map<String, DataSetRow> rowsMap = new LinkedHashMap<>();


        for (ResultSet resultSet : resultSets) {
            if (resultSet == null) {
                log.warn("Encountered a null ResultSet in mergeResultSetsSideBySide. Skipping.");
                count++; // Increment count even for null to align with duration array
                continue; // Skip null result sets
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                // Use a key that uniquely identifies a logical row across result sets.
                // Assuming the first column (index 1) can serve as a key like 'sex' or a similar identifier.
                // You might need to adjust the key based on the actual data structure returned by your procedures.
                String rowIndexKey = resultSet.getObject(1) != null ? resultSet.getObject(1).toString() : "null_key_" + UUID.randomUUID();


                DataSetRow row = rowsMap.computeIfAbsent(rowIndexKey, k -> new DataSetRow());


                for (int i = 1; i <= columnCount; i++) {
                    String columnName;
                    String originalColumnName = metaData.getColumnLabel(i);
                    Object columnValue = resultSet.getObject(i);


                    if (originalColumnName.equalsIgnoreCase("sex")) {
                        // If 'sex' column exists, add it without duration prefix
                        columnName = originalColumnName;
                        // Ensure sex column is only added once if it appears in multiple result sets
                        if (!row.getColumnValues().containsKey(new DataSetColumn(columnName, columnName, columnValue != null ? columnValue.getClass() : Object.class))) {
                            row.addColumnValue(new DataSetColumn(columnName, columnName, columnValue != null ? columnValue.getClass() : Object.class), columnValue);
                        }
                    } else {
                        // For other columns, prepend the duration.
                        // Ensure count is within bounds of duration array
                        String durationPrefix = (count < duration.length) ? duration[count] + " " : "UnknownDuration" + count + " ";
                        columnName = durationPrefix + originalColumnName;
                        row.addColumnValue(new DataSetColumn(columnName, columnName, columnValue != null ? columnValue.getClass() : Object.class), columnValue);
                    }
                }
            }
            count++;
        }

        // Add all processed rows from the map to the dataset
        for (DataSetRow row : rowsMap.values()) {
            data.addRow(row);
        }
    }
}
