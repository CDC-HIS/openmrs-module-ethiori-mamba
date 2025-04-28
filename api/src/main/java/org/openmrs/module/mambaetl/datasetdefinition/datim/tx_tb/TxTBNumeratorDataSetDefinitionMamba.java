package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_tb;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxTBAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxTBNumeratorDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxTBAggregationTypes txTBAggregationTypes;
	
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
	
	public TxTBAggregationTypes getTxTBAggregationTypes() {
		return txTBAggregationTypes;
	}
	
	public void setTxTBAggregationTypes(TxTBAggregationTypes txTBAggregationTypes) {
		this.txTBAggregationTypes = txTBAggregationTypes;
	}
}
