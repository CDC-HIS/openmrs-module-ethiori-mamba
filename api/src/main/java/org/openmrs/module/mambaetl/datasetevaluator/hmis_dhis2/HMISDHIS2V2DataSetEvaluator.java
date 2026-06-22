package org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2V2DatasetDefinition;
import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.mambaetl.service.DynamicReportExecutorService;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.ProcedureCall;

import static org.openmrs.module.mambaetl.helpers.DataSetEvaluatorHelper.rollbackAndThrowException;
import static org.openmrs.module.mambaetl.helpers.ValidationHelper.ValidateDates;

@Handler(supports = { HMISDHIS2V2DatasetDefinition.class })
public class HMISDHIS2V2DataSetEvaluator implements DataSetEvaluator {
	
	private static final Log log = LogFactory.getLog(HMISDHIS2V2DataSetEvaluator.class);
	
	private static final String ERROR_PROCESSING_RESULT_SET = "Error processing ResultSet: ";
	
	private static final String DATABASE_CONNECTION_ERROR = "Database connection error: ";
	
	@Autowired
	private DynamicReportExecutorService reportExecutorService;
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
	        throws EvaluationException {

		HMISDHIS2V2DatasetDefinition datasetDefinition = (HMISDHIS2V2DatasetDefinition) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ResultSetMapper resultSetMapper = new ResultSetMapper();

		ValidateDates(data, datasetDefinition.getStartDate(), datasetDefinition.getEndDate());

		try (Connection connection = DataSetEvaluatorHelper.getDataSource().getConnection()) {
			connection.setAutoCommit(false);

			List<ProcedureCall> procedureCalls = reportExecutorService.buildCalls("sp_fact_hmis_all_v2",
			    toParamMap(datasetDefinition));

			try {
				for (ProcedureCall call : procedureCalls) {
					try (CallableStatement statement = connection.prepareCall(call.getProcedureName())) {
						call.getParameterSetter().setParameters(statement);
						boolean hasResultSet = statement.execute();
						if (hasResultSet) {
							try (ResultSet rs = statement.getResultSet()) {
								resultSetMapper.mapResultSetToDataSet(rs, data);
							}
						}
					}
				}
				connection.commit();
				return data;
			}
			catch (SQLException e) {
				rollbackAndThrowException(connection, ERROR_PROCESSING_RESULT_SET + e.getMessage(), e, log);
			}
		}
		catch (SQLException e) {
			throw new EvaluationException(DATABASE_CONNECTION_ERROR + e.getMessage(), e);
		}
		throw new EvaluationException("unreachable");
	}
	
	private Map<String, String> toParamMap(HMISDHIS2V2DatasetDefinition def) {
		Map<String, String> params = new HashMap<>();
		if (def.getStartDate() != null) {
			params.put("startDate", new java.sql.Date(def.getStartDate().getTime()).toString());
		}
		if (def.getEndDate() != null) {
			params.put("endDate", new java.sql.Date(def.getEndDate().getTime()).toString());
		}
		return params;
	}
}
