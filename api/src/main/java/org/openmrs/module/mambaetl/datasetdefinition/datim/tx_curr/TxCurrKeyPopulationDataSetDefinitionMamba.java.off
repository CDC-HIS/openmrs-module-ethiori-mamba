package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxCurrKeyPopulationDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date endDate;
	
	private TxCurrAggregationTypes txCurrAggregationTypes;
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public TxCurrAggregationTypes getAggregationType() {
		return txCurrAggregationTypes;
	}
	
	public void setAggregationType(TxCurrAggregationTypes txCurrAggregationTypes) {
		this.txCurrAggregationTypes = txCurrAggregationTypes;
	}
	
}
