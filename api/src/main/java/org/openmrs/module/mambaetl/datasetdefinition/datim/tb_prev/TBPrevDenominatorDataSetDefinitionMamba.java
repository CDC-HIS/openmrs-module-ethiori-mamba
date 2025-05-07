package org.openmrs.module.mambaetl.datasetdefinition.datim.tb_prev;

import org.openmrs.module.mambaetl.helpers.reportOptions.TBPrevAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TBPrevDenominatorDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private TBPrevAggregationTypes tbPrevAggregationTypes;
	
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
	
	public TBPrevAggregationTypes getTBPrevAggregationTypes() {
		return tbPrevAggregationTypes;
	}
	
	public void setTBPrevAggregationTypes(TBPrevAggregationTypes tbPrevAggregationTypes) {
		this.tbPrevAggregationTypes = tbPrevAggregationTypes;
	}
	
}
