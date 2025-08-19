package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.PHRHSnsLineListDataSetDefinitionMamba;
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
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;

@Handler(supports = { PHRHSnsLineListDataSetDefinitionMamba.class })
public class PHRHSnsLineListDataSetEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(PHRHSnsLineListDataSetEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {

        PHRHSnsLineListDataSetDefinitionMamba dataSetDefinitionMamba = (PHRHSnsLineListDataSetDefinitionMamba) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        ValidateDates(data, dataSetDefinitionMamba.getStartDate(), dataSetDefinitionMamba.getEndDate());
        if(!data.getRows().isEmpty()){
            return data;
        }
        try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            List<ProcedureCall> procedureCalls = createProcedureCalls(dataSetDefinitionMamba);

            try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

                executeStatements(statementContainer, procedureCalls);

                ResultSet[] allResultSets = statementContainer.getResultSets();

                mapResultSet(data, resultSetMapper, allResultSets,Boolean.TRUE);
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
	
	private List<ProcedureCall> createProcedureCalls(PHRHSnsLineListDataSetDefinitionMamba dataSetDefinitionMamba) {
        java.sql.Date startDate = dataSetDefinitionMamba.getStartDate() != null ? new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime()) : null;
        java.sql.Date endDate = dataSetDefinitionMamba.getEndDate() != null ? new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime()) : null;
        String phrhCode = dataSetDefinitionMamba.getPhrhCode() != null ? dataSetDefinitionMamba.getPhrhCode() : null;


        return Collections.singletonList(
                new ProcedureCall("{call sp_fact_line_list_phrh_sns_query(?,?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                    statement.setString(3, phrhCode);
                })
        );
    }
}
