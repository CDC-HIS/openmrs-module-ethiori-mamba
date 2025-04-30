package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_rtt;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxRTTAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxTBAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxRTTDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxRTTAggregationTypes txRTTAggregationTypes;
	
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
	
	public TxRTTAggregationTypes getTxRTTAggregationTypes() {
		return txRTTAggregationTypes;
	}
	
	public void setTxRTTAggregationTypes(TxRTTAggregationTypes txRTTAggregationTypes) {
		this.txRTTAggregationTypes = txRTTAggregationTypes;
	}
}
