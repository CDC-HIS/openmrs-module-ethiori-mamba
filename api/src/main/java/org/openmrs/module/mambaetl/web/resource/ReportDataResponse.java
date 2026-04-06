package org.openmrs.module.mambaetl.web.resource;

import java.util.List;
import java.util.Map;

public class ReportDataResponse {
	
	private String procedureName;
	
	private int rowCount;
	
	private List<Map<String, Object>> data;
	
	public ReportDataResponse() {
	}
	
	public ReportDataResponse(String procedureName, List<Map<String, Object>> data) {
		this.procedureName = procedureName;
		this.data = data;
		this.rowCount = data != null ? data.size() : 0;
	}
	
	public String getProcedureName() {
		return procedureName;
	}
	
	public void setProcedureName(String procedureName) {
		this.procedureName = procedureName;
	}
	
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
	
	public List<Map<String, Object>> getData() {
		return data;
	}
	
	public void setData(List<Map<String, Object>> data) {
		this.data = data;
		this.rowCount = data != null ? data.size() : 0;
	}
}
