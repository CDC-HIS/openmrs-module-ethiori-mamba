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
	
	private final Map<String, String> columnNameMapping;
	
	public ResultSetMapper() {
		// Initialize the column name mapping
		columnNameMapping = new HashMap<>();
//		columnNameMapping.put("patient_name", "Patient Name");
//		columnNameMapping.put("mrn", "MRN");

	}
	
	public DataSet mapResultSetToDataSet(ResultSet resultSet, SimpleDataSet dataSet) throws SQLException {
		
		DynamicDataSet dynamicDataSet = new DynamicDataSet();
		
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnLabel(i);
			String mappedColumnName = getMappedColumnName(columnName);
			String columnType = metaData.getColumnTypeName(i);
			
			Class<?> columnClass = getClassForSqlType(columnType);
			
			DataSetColumn column = new DataSetColumn(mappedColumnName, mappedColumnName, columnClass);
			dynamicDataSet.addColumn(column);
		}
		
		while (resultSet.next()) {
			DataSetRow row = new DataSetRow();
			for (int i = 1; i <= columnCount; i++) {
				String columnName = metaData.getColumnLabel(i);
				Object value = resultSet.getObject(i);
				
				DataSetColumn column = dynamicDataSet.getColumn(getMappedColumnName(columnName));
				if (column != null) {
					if (value instanceof Date && column.getName().endsWith("EC.")) {
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
			return EthiOhriUtil.getEthiopianDate(new java.util.Date(date.getTime()));
		}
		return null;
	}
}
