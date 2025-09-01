package org.openmrs.module.mambaetl.datasetdefinition.datim.hts_index;

import org.openmrs.module.mambaetl.helpers.reportOptions.HTSIndexAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxNewAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class HTSIndexDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private HTSIndexAggregationTypes htsIndexAggregationTypes;
	
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
	
	public HTSIndexAggregationTypes getHtsIndexAggregationTypes() {
		return htsIndexAggregationTypes;
	}
	
	public void setHtsIndexAggregationTypes(HTSIndexAggregationTypes htsIndexAggregationTypes) {
		this.htsIndexAggregationTypes = htsIndexAggregationTypes;
	}
}
