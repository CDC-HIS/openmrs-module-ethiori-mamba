package org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
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

@Handler(supports = { HMISDHIS2DatasetDefinition.class })
public class HMISDHIS2DataSetEvaluator implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
			throws EvaluationException {

		HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition = (HMISDHIS2DatasetDefinition) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ValidationHelper validationHelper = new ValidationHelper();
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		// Validate start and end dates
		validationHelper.validateDates(hmisdhis2DatasetDefinition, data);

		// Get ResultSet from the database
		try (Connection connection = getDataSource().getConnection()) {
			connection.setAutoCommit(false); // Ensure consistency across multiple queries

			try (CallableStatement statement1 = connection.prepareCall("{call sp_fact_cxca_query(?,?)}");
				 CallableStatement statement2 = connection.prepareCall("{call sp_fact_other_query(?,?)}")

			) {

				java.sql.Date startDate = new java.sql.Date(hmisdhis2DatasetDefinition.getStartDate().getTime());
				java.sql.Date endDate = new java.sql.Date(hmisdhis2DatasetDefinition.getEndDate().getTime());

				// Execute first stored procedure
				statement1.setDate(1, startDate);
				statement1.setDate(2, endDate);
				ResultSet resultSet1 = statement1.executeQuery();

				// Execute second stored procedure
				statement2.setDate(1, startDate);
				statement2.setDate(2, endDate);
				ResultSet resultSet2 = statement2.executeQuery();

				// Merge results
				if (resultSet1 != null) {
					resultSetMapper.mapResultSetToDataSet(resultSet1, data);
				}
				if (resultSet2 != null) {
					resultSetMapper.mapResultSetToDataSet(resultSet2, data);
				}

				connection.commit();
				return data;

			} catch (SQLException e) {
				connection.rollback();
				throw new EvaluationException("Error processing ResultSet: " + e.getMessage(), e);
			}
		} catch (SQLException e) {
			throw new EvaluationException("Database connection error: " + e.getMessage(), e);
		}
	}
	
	private DataSource getDataSource() {
		return ConnectionPoolManager.getInstance().getDataSource();
	}
}
