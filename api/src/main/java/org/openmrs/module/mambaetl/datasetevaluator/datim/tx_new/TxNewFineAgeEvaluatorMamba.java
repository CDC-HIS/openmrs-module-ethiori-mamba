package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_new;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewCoarseAgeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewFineAgeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
import org.openmrs.module.mambaetl.helpers.dto.FineAgeData;
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
import java.util.List;

import static org.hibernate.search.util.AnalyzerUtils.log;

@Handler(supports = { TxNewFineAgeDataSetDefinitionMamba.class })
public class TxNewFineAgeEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		TxNewFineAgeDataSetDefinitionMamba txNewFineAgeDataSetDefinitionMamba = (TxNewFineAgeDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ValidationHelper validationHelper = new ValidationHelper();
		
		// Validate start and end dates
		validationHelper.validateDates(txNewFineAgeDataSetDefinitionMamba, data);
		
		List<FineAgeData> resultSet = getEtlNew(txNewFineAgeDataSetDefinitionMamba);
		
		if (!resultSet.isEmpty()) {
			DataSetRow headerRow = new DataSetRow();
			headerRow.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
			headerRow.addColumnValue(new DataSetColumn("Patient Count", "Patient Count", Integer.class), resultSet.size());
			data.addRow(headerRow);
			
			for (FineAgeData fineAgeData : resultSet) {
				try {
					DataSetRow row = new DataSetRow();
					mapDataToDataSetRow(row, fineAgeData);
					data.addRow(row);
				}
				catch (Exception e) {
					log.error("Exception mapping dataset row: " + e.getMessage(), e);
				}
			}
		}
		
		return data;
	}
	
	private List<FineAgeData> getEtlNew(TxNewFineAgeDataSetDefinitionMamba definition) {
		List<FineAgeData> fineAgeData = new ArrayList<>();
		DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();
		try (Connection connection = dataSource.getConnection();
			 CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_query(?,?,?,?)}")) {

			statement.setDate(1, new java.sql.Date(definition.getStartDate().getTime()));
			statement.setDate(2, new java.sql.Date(definition.getEndDate().getTime()));
			statement.setInt(3, 1);
			statement.setString(4, "All");

			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					FineAgeData data = mapRowToTxNewFineAgeData(resultSet);
					fineAgeData.add(data);
				}
			}
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		return fineAgeData;
	}
	
	private FineAgeData mapRowToTxNewFineAgeData(ResultSet resultSet) throws SQLException {
		// Assuming TXNewData has a constructor or setters for these fields
		return new FineAgeData(resultSet.getString("sex"), resultSet.getString("1-4"), resultSet.getString("5-9"),
		        resultSet.getString("10-14"), resultSet.getString("15-19"), resultSet.getString("20-24"),
		        resultSet.getString("25-29"), resultSet.getString("30-34"), resultSet.getString("35-39"),
		        resultSet.getString("40-44"), resultSet.getString("45-49"), resultSet.getString("50-54"),
		        resultSet.getString("55-59"), resultSet.getString("60-64"), resultSet.getString("65+"));
	}
	
	private void mapDataToDataSetRow(DataSetRow row, FineAgeData data) throws IllegalAccessException {
		Field[] fields = FineAgeData.class.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			String fieldName = field.getName();
			Object value = field.get(data);
			Class<?> fieldType = field.getType();
			
			// Handle special cases or transformations if necessary
			if (value instanceof java.util.Date) {
				value = EthiOhriUtil.getEthiopianDate((java.util.Date) value);
			}
			
			DataSetColumn column = new DataSetColumn(fieldName, fieldName, fieldType);
			row.addColumnValue(column, value);
		}
	}
}
