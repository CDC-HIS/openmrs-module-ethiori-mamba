package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_curr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
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

            try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

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
	
	private List<ProcedureCall> createProcedureCalls(TxCurrAgeSexDataSetDefinitionMamba dataSetDefinitionMamba) {
        java.sql.Date endDate = new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime());
        TxCurrAggregationTypes aggregation = dataSetDefinitionMamba.getTxCurrAggregationType();

        if(aggregation == TxCurrAggregationTypes.CD4){
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
                    })
            );
        }
        else if(aggregation == TxCurrAggregationTypes.AGE_SEX) {
            return Collections.singletonList(
                    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
                        statement.setDate(1, endDate);
                        statement.setInt(2, 0);
                        statement.setInt(3, 3);
                        statement.setInt(4, 0);
                    })
            );
        }
        else if(aggregation == TxCurrAggregationTypes.NUMERATOR){
            return Collections.singletonList(
                    new ProcedureCall("{call sp_dim_tx_curr_datim_query(?,?,?,?)}", statement -> {
                        statement.setDate(1, endDate);
                        statement.setInt(2, 0);
                        statement.setInt(3, 3);
                        statement.setInt(4, 1);
                    })
            );
        }
        else return null;
    }
	
	private void mergeResultSetsSideBySide(SimpleDataSet data, List<ResultSet> resultSets) throws SQLException {
        if (resultSets == null || resultSets.isEmpty() ) {
            return;
        }
        if(resultSets.size() == 1){
            ResultSet[] resultSets1 = resultSets.toArray(new ResultSet[0]);
            ResultSetMapper resultSetMapper = new ResultSetMapper();
            mapResultSet(data, resultSetMapper, resultSets1);
        }
        int count = 0;
        String[] duration = {"<3 months of ARVs (not MMD)", "3-5 months of ARVs", "6 or more months of ARVs"};

        Map<String, DataSetRow> rowsMap = new HashMap<>(); // Implement dynamic list


        for (ResultSet resultSet : resultSets) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                DataSetRow row = new DataSetRow();

                for (int i = 1; i <= columnCount; i++) {
                    if(count == 0 || i>1){
                        String columnName;
                        if (metaData.getColumnName(i).equalsIgnoreCase("sex")) {
                            columnName = metaData.getColumnName(i);
                        } else {
                            columnName = duration[count] +" "+ metaData.getColumnName(i); // Create a unique column name duration[count]
                        }
                        Object columnValue = resultSet.getObject(i);
                        row.addColumnValue(new DataSetColumn(columnName, columnName, columnValue != null ? columnValue.getClass() : Object.class), columnValue);
                    }
                }

                int rowIndex = resultSet.getRow();
                String rowIndexKey = String.valueOf(rowIndex);

                if (rowsMap.containsKey(rowIndexKey)) {
                    DataSetRow existingRow = rowsMap.get(rowIndexKey);
                    for (Map.Entry<DataSetColumn, Object> entry : row.getColumnValues().entrySet()) {
                        existingRow.addColumnValue(new DataSetColumn(String.valueOf(entry.getKey()), String.valueOf(entry.getKey()), entry.getValue() != null ? entry.getValue().getClass() : Object.class), entry.getValue());
                    }
                } else {
                    rowsMap.put(rowIndexKey, row);
                }
            }
            count++;
        }

        for (Map.Entry<String, DataSetRow> entry : rowsMap.entrySet()) {
            data.addRow(entry.getValue());
        }
    }
}
