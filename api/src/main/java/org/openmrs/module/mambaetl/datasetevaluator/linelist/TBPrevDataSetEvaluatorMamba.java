package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.ChronicCareEnrollmentDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TBPrevDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
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
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.*;

@Handler(supports = { TBPrevDataSetDefinitionMamba.class })
public class TBPrevDataSetEvaluatorMamba implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(TBPrevDataSetEvaluatorMamba.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		TBPrevDataSetDefinitionMamba definition = (TBPrevDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<ProcedureCall> procedureCalls = createProcedureCalls(definition);

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
	
	private List<ProcedureCall> createProcedureCalls(TBPrevDataSetDefinitionMamba dataSetDefinitionMamba) {
		Date startDate = dataSetDefinitionMamba.getStartDate() != null ?
				new Date(getPrevSixMonth(dataSetDefinitionMamba.getStartDate())) :
				new Date(LocalDate.of(1900, 1, 1).toEpochDay() * 24 * 60 * 60 * 1000);
		Date endDate = dataSetDefinitionMamba.getEndDate() != null ?
				new Date( dataSetDefinitionMamba.getEndDate().getTime()): new Date(System.currentTimeMillis());
				String storeProc = dataSetDefinitionMamba.getTptStatus().equalsIgnoreCase("Numerator")?
						"{call sp_fact_line_list_tx_tbt_numerator_query(?,?)}":
						"{call sp_fact_line_list_tx_tbt_denominator_query(?,?)}";
		return Collections.singletonList(
                new ProcedureCall(storeProc, statement -> {
                    statement.setDate(1, startDate);
					statement.setDate(2, endDate);

                })
        );
	}
	
	private long getPrevSixMonth(java.util.Date date) {
		Calendar subSixMonth = Calendar.getInstance();
		subSixMonth.setTime(date);
		subSixMonth.add(Calendar.MONTH, -6);
		return subSixMonth.getTime().getTime();
	}
}
