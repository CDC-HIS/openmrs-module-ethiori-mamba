package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxNewAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TxNewAgeSexCd4DataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	public Date getStartDate() {
		return startDate;
	}
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxNewAggregationTypes txNewAggregationTypes = TxNewAggregationTypes.UNKNOWN;
	
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public TxNewAggregationTypes getCd4Status() {
		return txNewAggregationTypes;
	}
	
	public void setCd4Status(TxNewAggregationTypes txNewAggregationTypes) {
		this.txNewAggregationTypes = txNewAggregationTypes;
	}
}
