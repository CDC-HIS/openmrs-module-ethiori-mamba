package org.openmrs.module.mambaetl.helpers.mapper;

import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.SimpleDataSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetMapper {
	
	public DataSet mapResultSetToDataSet(ResultSet resultSet, SimpleDataSet dataSet) throws SQLException {
		
		DynamicDataSet dynamicDataSet = new DynamicDataSet();
		
		// Get metadata
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		
		// Add columns to DataSet
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			String columnLabel = metaData.getColumnLabel(i);
			String columnType = metaData.getColumnTypeName(i);
			
			// Determine column class based on SQL type
			Class<?> columnClass = getClassForSqlType(columnType);
			
			// Create and add DataSetColumn
			DataSetColumn column = new DataSetColumn(columnName, columnLabel, columnClass);
			dynamicDataSet.addColumn(column);
		}
		
		// Iterate through rows and add them to the DataSet
		while (resultSet.next()) {
			DataSetRow row = new DataSetRow();
			for (int i = 1; i <= columnCount; i++) {
				String columnName = metaData.getColumnName(i);
				Object value = resultSet.getObject(i);
				
				// Add column value to the row
				DataSetColumn column = dynamicDataSet.getColumn(columnName);
				if (column != null) {
					row.addColumnValue(column, value);
				}
			}
			dataSet.addRow(row);
		}
		
		return dataSet;
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
}
