package org.openmrs.module.mambaetl.web.resource;

import java.util.List;
import java.util.Map;

public class ReportDataResponse {
	
	private String procedureName;
	
	private List<String> columns;
	
	private List<Map<String, Object>> data;
	
	public ReportDataResponse() {
	}
	
	public ReportDataResponse(String procedureName, List<String> columns, List<Map<String, Object>> data) {
		this.procedureName = procedureName;
		this.columns = columns;
		this.data = data;
	}
	
	public String getProcedureName() {
		return procedureName;
	}
	
	public void setProcedureName(String procedureName) {
		this.procedureName = procedureName;
	}
	
	public List<String> getColumns() {
		return columns;
	}
	
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	
	public int getRowCount() {
		return data != null ? data.size() : 0;
	}
	
	public List<Map<String, Object>> getData() {
		return data;
	}
	
	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}
}
