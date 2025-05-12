package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import java.util.Date;

import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAnalysisCategories;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
public class TxCurrAnalysisLineListDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TxCurrAnalysisCategories txCurrAnalysisCategories;
	
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
	
	public TxCurrAnalysisCategories getTxCurrAnalysisCategories() {
		return txCurrAnalysisCategories;
	}
	
	public void setTxCurrAnalysisCategories(TxCurrAnalysisCategories txCurrAnalysisCategories) {
		this.txCurrAnalysisCategories = txCurrAnalysisCategories;
	}
}
