package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr;

import org.openmrs.module.mambaetl.helpers.mapper.AggregationType;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxCurrKeyPopulationDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date endDate;
	
	private AggregationType aggregationType;
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public AggregationType getAggregationType() {
		return aggregationType;
	}
	
	public void setAggregationType(AggregationType aggregationType) {
		this.aggregationType = aggregationType;
	}
	
}
