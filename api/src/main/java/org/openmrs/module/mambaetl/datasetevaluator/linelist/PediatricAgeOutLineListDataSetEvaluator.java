package org.openmrs.module.mambaetl.datasetevaluator.linelist;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.PediatricAgeOutLineListDatasetDefinition;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Handler(supports = { PediatricAgeOutLineListDatasetDefinition.class })
public class PediatricAgeOutLineListDataSetEvaluator implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {

		PediatricAgeOutLineListDatasetDefinition pediatricAgeOutLineListDatasetDefinition = (PediatricAgeOutLineListDatasetDefinition) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ValidationHelper validationHelper = new ValidationHelper();
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		// Validate start and end dates
		validationHelper.validateDates(pediatricAgeOutLineListDatasetDefinition, data);

		// Get ResultSet from the database
		try (Connection connection = getDataSource().getConnection();
			 CallableStatement statement = connection.prepareCall("{call sp_fact_pediatric_age_out_query(?,?)}")) {
			statement.setDate(1, new java.sql.Date(pediatricAgeOutLineListDatasetDefinition.getStartDate().getTime()));
			statement.setDate(2, new java.sql.Date(pediatricAgeOutLineListDatasetDefinition.getEndDate().getTime()));

			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet != null) {
					return resultSetMapper.mapResultSetToDataSet(resultSet, data);
				} else {
					throw new EvaluationException("No data returned from the query.");
				}
			}
		} catch (SQLException e) {
			throw new EvaluationException("Error processing ResultSet: " + e.getMessage(), e);
		}
	}
	
	private DataSource getDataSource() {
		return ConnectionPoolManager.getInstance().getDataSource();
	}
}
