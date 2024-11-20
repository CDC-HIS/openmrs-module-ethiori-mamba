package org.openmrs.module.mambaetl.helpers.mapper;

import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ResultSetMapper {
	
	private Map<String, String> columnNameMapping;
	
	public ResultSetMapper() {
		// Initialize the column name mapping
		columnNameMapping = new HashMap<>();
		columnNameMapping.put("patient_name", "Patient Name");
		columnNameMapping.put("mrn", "MRN");
		columnNameMapping.put("uan", "UAN");
		columnNameMapping.put("current_age", "Age");
		columnNameMapping.put("sex", "Sex");
		columnNameMapping.put("mobile_no", "Mobile No.");
		columnNameMapping.put("weight_in_kg", "Weight in Kg");
		columnNameMapping.put("cd4_count", "Cd4 Count");
		columnNameMapping.put("current_who_hiv_stage", "Current WHO Stage");
		columnNameMapping.put("nutritional_status", "Nutrition Status");
		columnNameMapping.put("tb_screening_result", "Tb Screening Result");
		columnNameMapping.put("enrollment_date", "Enrollment Date");
		columnNameMapping.put("hiv_confirmed_date", "HIV Confirmed Date");
		columnNameMapping.put("art_start_date", "ART Start Date");
		columnNameMapping.put("days_difference", "Days Difference");
		columnNameMapping.put("followup_date", "Followup Date");
		columnNameMapping.put("regimen", "Regimen");
		columnNameMapping.put("arv_dose_days", "ARV Dose Days");
		columnNameMapping.put("pregnancy_status", "Pregnancy(Y/N)");
		columnNameMapping.put("breast_feeding_status", "Breast Feeding Status");
		columnNameMapping.put("follow_up_status", "Follow Up Status");
		columnNameMapping.put("ti", "TI");
		columnNameMapping.put("treatment_end_date", "Treatment End Date");
		columnNameMapping.put("next_visit_date", "Next Visit Date");
		columnNameMapping.put("latest_followup_date", "Last Follow-up Date");
		columnNameMapping.put("latest_followup_status", "Last Follow-up Status");
		columnNameMapping.put("latest_regimen", "Latest Regimen");
		columnNameMapping.put("latest_arv_dose_days", "Latest ARV Dose Days");
		columnNameMapping.put("age_at_enrollment", "Age at Enrollment");
		columnNameMapping.put("anitiretroviral_adherence_level", "Adherence");
		columnNameMapping.put("dsd_category", "DSD Category");
		columnNameMapping.put("tpt_start_date", "TPT Start Date");
		columnNameMapping.put("tpt_completed_date", "TPT Completed Date");
		columnNameMapping.put("tpt_discontinued_date", "TPT Discontinued Date");
		columnNameMapping.put("tuberculosis_treatment_end_date", "TB Treatment End Date");
		columnNameMapping.put("date_viral_load_results_received", "Date Viral Load Results Received");
		columnNameMapping.put("viral_load_test_status", "Viral Load Test Status");
	}
	
	public DataSet mapResultSetToDataSet(ResultSet resultSet, SimpleDataSet dataSet) throws SQLException {
		
		DynamicDataSet dynamicDataSet = new DynamicDataSet();
		
		// Get metadata
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		// Add columns to DataSet
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			String mappedColumnName = getMappedColumnName(columnName);
			String columnLabel = metaData.getColumnLabel(i);
			String columnType = metaData.getColumnTypeName(i);
			
			// Determine column class based on SQL type
			Class<?> columnClass = getClassForSqlType(columnType);
			
			// Create and add DataSetColumn using the mapped column name
			DataSetColumn column = new DataSetColumn(mappedColumnName, mappedColumnName, columnClass);
			dynamicDataSet.addColumn(column);
		}
		
		// Iterate through rows and add them to the DataSet
		while (resultSet.next()) {
			DataSetRow row = new DataSetRow();
			for (int i = 1; i <= columnCount; i++) {
				String columnName = metaData.getColumnName(i);
				Object value = resultSet.getObject(i);
				
				// Add column value to the row
				DataSetColumn column = dynamicDataSet.getColumn(getMappedColumnName(columnName));
				if (column != null) {
					if (value instanceof Date) {
						value = transformDate((Date) value);
					}
					
					row.addColumnValue(column, value);
				}
			}
			dataSet.addRow(row);
		}
		
		return dataSet;
	}
	
	private String getMappedColumnName(String originalColumnName) {
		// Get the mapped column name if it exists; otherwise, return the original name
		return columnNameMapping.getOrDefault(originalColumnName, originalColumnName);
	}
	
	private Class<?> getClassForSqlType(String sqlType) {
		switch (sqlType) {
			case "VARCHAR":
			case "CHAR":
				return String.class;
			case "INTEGER":
			case "INT":
				return Integer.class;
			case "DOUBLE":
			case "FLOAT":
				return Double.class;
			case "DATE":
				return java.util.Date.class;
			case "TIMESTAMP":
				return java.sql.Timestamp.class;
			default:
				return Object.class;
		}
	}
	
	private Object transformDate(Date date) {
		if (date != null) {
			// Example transformation, e.g., to a different date format or a specific timezone
			return EthiOhriUtil.getEthiopianDate(new java.util.Date(date.getTime()));
		}
		return null;
	}
}
