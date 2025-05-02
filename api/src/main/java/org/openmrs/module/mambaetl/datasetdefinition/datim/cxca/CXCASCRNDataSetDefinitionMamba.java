package org.openmrs.module.mambaetl.datasetdefinition.datim.cxca;

import org.openmrs.module.mambaetl.helpers.reportOptions.CXCATXAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class CXCASCRNDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private CXCATXAggregationTypes cxcatxAggregationTypes;
	
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
	
	public CXCATXAggregationTypes getCxcatxAggregationTypes() {
		return cxcatxAggregationTypes;
	}
	
	public void setCxcatxAggregationTypes(CXCATXAggregationTypes cxcatxAggregationTypes) {
		this.cxcatxAggregationTypes = cxcatxAggregationTypes;
	}
}
