package org.openmrs.module.mambaetl.datasetdefinition.datim.prep_ct;

import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPCTggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBArtAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class PrEPCTDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private PrEPCTggregationTypes prEPCTggregationTypes;
	
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
	
	public PrEPCTggregationTypes getPrEPCTggregationTypes() {
		return prEPCTggregationTypes;
	}
	
	public void setPrEPCTggregationTypes(PrEPCTggregationTypes prEPCTggregationTypes) {
		this.prEPCTggregationTypes = prEPCTggregationTypes;
	}
}
