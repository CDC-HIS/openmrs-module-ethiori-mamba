package org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct;

import org.openmrs.module.mambaetl.helpers.reportOptions.HEIAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class PmtctHeiDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private HEIAggregationTypes heiAggregationTypes;
	
	public Date getStartDate() {
		return startDate;
	}
	
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public HEIAggregationTypes getHeiAggregationTypes() {
		return heiAggregationTypes;
	}
	
	public void setHeiAggregationTypes(HEIAggregationTypes heiAggregationTypes) {
		this.heiAggregationTypes = heiAggregationTypes;
	}
	
	@ConfigurationProperty
	private String reportType = "DISAGGREGATED"; // Default to DISAGGREGATED
	
	public String getReportType() {
		return reportType;
	}
	
	public void setReportType(String reportType) {
		this.reportType = reportType;
	}
	
}
