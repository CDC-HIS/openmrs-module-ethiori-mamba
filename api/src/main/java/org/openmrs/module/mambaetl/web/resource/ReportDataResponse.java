package org.openmrs.module.mambaetl.web.resource;

import java.util.List;
import java.util.Map;

public class ReportDataResponse {
	
	private String procedureName;
	
	private List<Map<String, Object>> data;
	
	public ReportDataResponse() {
	}
	
	public ReportDataResponse(String procedureName, List<Map<String, Object>> data) {
		this.procedureName = procedureName;
		this.data = data;
	}
	
	public String getProcedureName() {
		return procedureName;
	}
	
	public void setProcedureName(String procedureName) {
		this.procedureName = procedureName;
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
