package org.openmrs.module.mambaetl.datasetdefinition.datim;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrPvlsAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxCurrPvlsDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxCurrPvlsAggregationTypes txCurrPvlsAggregationTypes;
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public TxCurrPvlsAggregationTypes getTxCurrPvlsAggregationTypes() {
		return txCurrPvlsAggregationTypes;
	}
	
	public void setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes txCurrPvlsAggregationTypes) {
		this.txCurrPvlsAggregationTypes = txCurrPvlsAggregationTypes;
	}
}
