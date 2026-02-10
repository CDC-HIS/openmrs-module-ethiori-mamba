package org.openmrs.module.mambaetl.api.model;

public class CustomMambaReportItemColumn {
	
	private String columnName;
	
	private Object columnValue;
	
	public CustomMambaReportItemColumn(String columnName, Object columnValue) {
		this.columnName = columnName;
		this.columnValue = columnValue;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public Object getColumnValue() {
		return columnValue;
	}
	
	public void setColumnValue(Object columnValue) {
		this.columnValue = columnValue;
	}
}
