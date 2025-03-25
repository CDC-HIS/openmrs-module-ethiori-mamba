package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.TXNewLineListDataSetDefinitionMamba;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*; //Static import DataSetEvaluatorHelper methods and inner classes

@Handler(supports = { TXNewLineListDataSetDefinitionMamba.class })
public class TxNewLineListDatasetEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(TxNewLineListDatasetEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {

        TXNewLineListDataSetDefinitionMamba txNewLineListDataSetDefinitionMamba = (TXNewLineListDataSetDefinitionMamba) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

        ResultSetMapper resultSetMapper = new ResultSetMapper();

        ValidateDates(data, txNewLineListDataSetDefinitionMamba.getStartDate(), txNewLineListDataSetDefinitionMamba.getEndDate());

        try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = createProcedureCalls(txNewLineListDataSetDefinitionMamba);

            try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

                executeStatements(statementContainer, procedureCalls);

                ResultSet[] allResultSets = statementContainer.getResultSets();

                mapResultSet(data, resultSetMapper, allResultSets);
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
	
	private List<ProcedureCall> createProcedureCalls(TXNewLineListDataSetDefinitionMamba txNewLineListDataSetDefinitionMamba) {
        java.sql.Date startDate = new java.sql.Date(txNewLineListDataSetDefinitionMamba.getStartDate().getTime());
        java.sql.Date endDate = new java.sql.Date(txNewLineListDataSetDefinitionMamba.getEndDate().getTime());

        return Collections.singletonList(
                new ProcedureCall("{call sp_fact_dhis_tx_new_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                })
        );
    }
}
