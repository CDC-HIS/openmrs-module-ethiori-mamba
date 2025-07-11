package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.PediatricAgeOutLineListDatasetDefinition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
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
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;

@Handler(supports = { PediatricAgeOutLineListDatasetDefinition.class })
public class PediatricAgeOutLineListDataSetEvaluator implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(PediatricAgeOutLineListDataSetEvaluator.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		PediatricAgeOutLineListDatasetDefinition pediatricAgeOutLineListDatasetDefinition = (PediatricAgeOutLineListDatasetDefinition) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		ValidateDates(data, pediatricAgeOutLineListDatasetDefinition.getStartDate(), pediatricAgeOutLineListDatasetDefinition.getEndDate());
		if(!data.getRows().isEmpty()){
			return data;
		}
		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<DataSetEvaluatorHelper.ProcedureCall> procedureCalls = createProcedureCalls(pediatricAgeOutLineListDatasetDefinition);

			try (DataSetEvaluatorHelper.CallableStatementContainer statementContainer = prepareStatements(connection, procedureCalls)) {

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
	
	private List<ProcedureCall> createProcedureCalls(PediatricAgeOutLineListDatasetDefinition pediatricAgeOutLineListDatasetDefinition) {
		java.sql.Date startDate = pediatricAgeOutLineListDatasetDefinition.getStartDate()!=null? new java.sql.Date(pediatricAgeOutLineListDatasetDefinition.getStartDate().getTime()):null;
		java.sql.Date endDate = pediatricAgeOutLineListDatasetDefinition.getEndDate()!= null? new java.sql.Date(pediatricAgeOutLineListDatasetDefinition.getEndDate().getTime()):null;

		return Collections.singletonList(

                new ProcedureCall("{call sp_fact_line_list_pediatric_age_out_query(?,?)}", statement -> {
                    statement.setDate(1, startDate);
                    statement.setDate(2, endDate);
                })
        );
	}
}
