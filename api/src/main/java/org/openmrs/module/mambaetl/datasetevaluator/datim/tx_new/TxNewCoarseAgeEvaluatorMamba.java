package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_new;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewCoarseAgeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
import org.openmrs.module.mambaetl.helpers.dto.CoarseAgeData;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hibernate.search.util.AnalyzerUtils.log;

@Handler(supports = { TxNewCoarseAgeDataSetDefinitionMamba.class })
public class TxNewCoarseAgeEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		TxNewCoarseAgeDataSetDefinitionMamba txNewCoarseAgeDataSetDefinitionMamba = (TxNewCoarseAgeDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

		ValidationHelper validationHelper = new ValidationHelper();
		ResultSetMapper resultSetMapper = new ResultSetMapper();

		// Validate start and end dates
		validationHelper.validateDates(txNewCoarseAgeDataSetDefinitionMamba, data);

		// Get ResultSet from the database
		try (Connection connection = getDataSource().getConnection();
			 CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_query(?,?,?,?)}")) {

			statement.setDate(1, new java.sql.Date(txNewCoarseAgeDataSetDefinitionMamba.getStartDate().getTime()));
			statement.setDate(2, new java.sql.Date(txNewCoarseAgeDataSetDefinitionMamba.getEndDate().getTime()));
			statement.setInt(3, 0);
			statement.setString(4, "All");

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
