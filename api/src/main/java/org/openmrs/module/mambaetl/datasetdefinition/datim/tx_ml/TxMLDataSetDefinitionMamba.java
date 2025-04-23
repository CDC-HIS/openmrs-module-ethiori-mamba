package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxMLAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxNewAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxMLDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxMLAggregationTypes txMLAggregationTypes;
	
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
	
	public TxMLAggregationTypes getTxMLAggregationTypes() {
		return txMLAggregationTypes;
	}
	
	public void setTxMLAggregationTypes(TxMLAggregationTypes txMLAggregationTypes) {
		this.txMLAggregationTypes = txMLAggregationTypes;
	}
}
