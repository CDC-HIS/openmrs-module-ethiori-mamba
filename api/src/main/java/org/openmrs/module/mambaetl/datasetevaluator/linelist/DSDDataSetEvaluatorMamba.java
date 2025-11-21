package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.DSDDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.DefaultDateParameter;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.FollowUpConstant;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;

@Handler(supports = { DSDDataSetDefinitionMamba.class })
public class DSDDataSetEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(DSDDataSetDefinitionMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		DSDDataSetDefinitionMamba ahdLineListDataSetDefinitionMamba = (DSDDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<ProcedureCall> procedureCalls = createProcedureCalls(ahdLineListDataSetDefinitionMamba);

			try (CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

				executeStatements(statementContainer, procedureCalls);

				ResultSet[] allResultSets = statementContainer.getResultSets();
				mapResultSet(data, resultSetMapper, allResultSets,Boolean.TRUE);
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
	
	private List<ProcedureCall> createProcedureCalls(DSDDataSetDefinitionMamba dataSetDefinitionMamba) {
		java.sql.Date startDate = dataSetDefinitionMamba.getStartDate() != null ? new java.sql.Date(dataSetDefinitionMamba.getStartDate().getTime()):null ;
		java.sql.Date endDate = dataSetDefinitionMamba.getEndDate() != null ? new java.sql.Date( dataSetDefinitionMamba.getEndDate().getTime()):null ;

		return Collections.singletonList(
                new ProcedureCall("{call sp_fact_line_list_dsd_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
					statement.setDate(2, endDate);

                })
        );
	}
}
