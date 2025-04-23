package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxCurrAgeSexDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxCurrAggregationTypes txCurrAggregationType;
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public TxCurrAggregationTypes getTxCurrAggregationType() {
		return txCurrAggregationType;
	}
	
	public void setTxCurrAggregationType(TxCurrAggregationTypes txCurrAggregationTypes) {
		this.txCurrAggregationType = txCurrAggregationTypes;
	}
	
}
