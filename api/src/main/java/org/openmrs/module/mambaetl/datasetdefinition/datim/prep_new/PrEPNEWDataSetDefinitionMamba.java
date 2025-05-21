package org.openmrs.module.mambaetl.datasetdefinition.datim.prep_new;

import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPNEWAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class PrEPNEWDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private PrEPNEWAggregationTypes prEPNEWAggregationTypes;
	
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
	
	public PrEPNEWAggregationTypes getPrEPNEWAggregationTypes() {
		return prEPNEWAggregationTypes;
	}
	
	public void setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes prEPNEWAggregationTypes) {
		this.prEPNEWAggregationTypes = prEPNEWAggregationTypes;
	}
}
