package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.TPTLineListDataSetDefinitionMamba;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;
import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.rollbackAndThrowException;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Handler(supports = { TPTLineListDataSetDefinitionMamba.class })
public class TPTLineListDataSetEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(TPTLineListDataSetEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {

        TPTLineListDataSetDefinitionMamba tptLineListDataSetDefinitionMamba = (TPTLineListDataSetDefinitionMamba) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
        DataSetRow totalRow = new DataSetRow();
        totalRow.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        ValidateDates(data, tptLineListDataSetDefinitionMamba.getStartDate(), tptLineListDataSetDefinitionMamba.getEndDate());

        try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
            connection.setAutoCommit(false);

            List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = createProcedureCalls(tptLineListDataSetDefinitionMamba);

            try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

                executeStatements(statementContainer, procedureCalls);

                ResultSet[] allResultSets = statementContainer.getResultSets();
                totalRow.addColumnValue(new DataSetColumn("Patient Name", "Patient Name", Integer.class), allResultSets.length);

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
	
	private List<ProcedureCall> createProcedureCalls(TPTLineListDataSetDefinitionMamba tptLineListDataSetDefinitionMamba) {
        java.sql.Date startDate = new java.sql.Date(tptLineListDataSetDefinitionMamba.getStartDate().getTime());
        java.sql.Date endDate = new java.sql.Date(tptLineListDataSetDefinitionMamba.getEndDate().getTime());

        return Arrays.asList(

                new ProcedureCall("{call sp_fact_line_list_tpt_linelist_query(?,?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                    statement.setString(3,tptLineListDataSetDefinitionMamba.getTptType());
                })
        );
    }
}
