package org.openmrs.module.mambaetl.datasetdefinition.datim.prep_new;

import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPCTggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPNEWggregationTypes;
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
	private PrEPNEWggregationTypes prEPNEWggregationTypes;
	
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
	
	public PrEPNEWggregationTypes getPrEPNEWggregationTypes() {
		return prEPNEWggregationTypes;
	}
	
	public void setPrEPNEWggregationTypes(PrEPNEWggregationTypes prEPNEWggregationTypes) {
		this.prEPNEWggregationTypes = prEPNEWggregationTypes;
	}
}
